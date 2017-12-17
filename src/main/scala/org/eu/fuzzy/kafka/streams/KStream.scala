package org.eu.fuzzy.kafka.streams

import scala.util.Try
import java.util.Collections

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.apache.kafka.streams.kstream.{KStream => KafkaStream}

import org.eu.fuzzy.kafka.streams.internals.KStreamWrapper
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler
import org.eu.fuzzy.kafka.streams.error.CheckedOperation.DeserializeOperation
import org.eu.fuzzy.kafka.streams.support.LogErrorHandler
import com.typesafe.scalalogging.Logger

/**
 * Represents an abstraction of a record stream.
 *
 * Each record is an independent entity/event in the real world, e.g. a user X might buy two items I1 and I2,
 * and thus there might be two records `<K:I1>, <K:I2>` in the stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @see [[org.apache.kafka.streams.kstream.KStream]]
 */
trait KStream[K, V] {

  /** Returns an underlying instance of Kafka Stream. */
  private[streams] def internalStream: KafkaStream[K, V]

  /**
   * Returns a Kafka topic for this stream.
   *
   * @note The name of topic is absent for the streams which are created by any intermediate operations,
   *       e.g. [[filter()]], [[map()]], etc.
   */
  def topic: KTopic[K, V]

  /**
   * Returns a new stream that consists of all records of this stream which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @param predicate  a function to test an each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#filter]]
   */
  def filter(predicate: (K, V) => Boolean): KStream[K, V]

  /**
   * Returns a new stream that consists of all records of this stream which don't satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @param predicate  a function to test an each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#filterNot]]
   */
  @inline def filterNot(predicate: (K, V) => Boolean): KStream[K, V] = filter((key, value) => !predicate(key, value))

  /**
   * Returns a new stream that consists of all records of this stream which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @param predicate  a function to test an each record key
   */
  @inline def filterKeys(predicate: K => Boolean): KStream[K, V] = filter((key, _) => predicate(key))

  /**
   * Returns a new stream that consists of all records of this stream which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @param predicate  a function to test an each record value
   */
  @inline def filterValues(predicate: V => Boolean): KStream[K, V] = filter((_, value) => predicate(value))

  /**
   * Returns a new stream with a new key and value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new key and value for each record
   * @param keySerde  a serialization format for the record key
   * @param valueSerde  a serialization format for the record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#map]]
   */
  @inline def map[KR, VR](mapper: (K, V) => (KR, VR))
                 (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR] =
    flatMap((key, value) => Seq(mapper(key, value)))

  /**
   * Returns a new stream with a new key for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   *
   * @param mapper  a function to compute a new value for each record
   * @param serde  a serialization format for the record value
   */
  @inline def mapKeys[KR](mapper: K => KR)(implicit serde: KeySerde[KR]): KStream[KR, V] =
    map((key, value) => (mapper(key), value))(serde, topic.valueSerde)

  /**
   * Returns a new stream with a new value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new value for each record
   * @param serde  a serialization format for the record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#mapValues]]
   */
  @inline def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR] =
    flatMapValues(mapper.andThen(Seq(_)))

  /**
   * Transforms each record of the input stream into zero or more records in the output stream.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute the new output records
   * @param keySerde  a serialization format for the record key
   * @param valueSerde  a serialization format for the record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#flatMap]]
   */
  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                     (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR]

  /**
   * Transforms a value of each input record into zero or more records with the same key in the output stream.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute the new output values
   * @param serde  a serialization format for the record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#flatMapValues]]
   */
  def flatMapValues[VR](mapper: V => Iterable[VR])(implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a list of streams from this stream by branching the records in the original stream based on
   * the supplied predicates.
   *
   * Each stream in the result sequence corresponds position-wise (index) to the predicate in the supplied predicates.
   * The branching happens on first-match: A record in the original stream is assigned to the corresponding result
   * stream for the first predicate that evaluates to true, and is assigned to this stream only.
   * A record will be dropped if none of the predicates evaluate to true.
   *
   * This is a stateless record-by-record operation.
   *
   * @param predicates  an ordered list of functions to test an each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#branch]]
   */
  def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]]

  /**
   * Splits this stream in two streams according to a predicate.
   *
   * @param predicate  a function to test an each record
   *
   * @return a pair of streams: the iterator that satisfies the predicate and the iterator that does not.
   */
  @inline def split(predicate: (K, V) => Boolean): (KStream[K, V], KStream[K, V]) = {
    val streams = branch((key, value) => predicate(key, value), (key, value) => !predicate(key, value))
    (streams(0), streams(1))
  }

  /**
   * Merges this stream and the given stream into one larger stream.
   *
   * @param stream  a stream to merge
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#merge]]
   */
  def merge(stream: KStream[K, V]): KStream[K, V]

  /**
   * Performs an action for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @param action  an action to perform on each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#foreach]]
   */
  def foreach(action: (K, V) => Unit): Unit

  /**
   * Performs an action for each input record.
   *
   * This is a stateless record-by-record operation that triggers a side effect (such as logging or statistics collection)
   * and returns an unchanged stream.
   *
   * @param action  an action to perform on each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#peek]]
   */
  def peek(action: (K, V) => Unit): KStream[K, V]

  /**
   * Materializes this stream to a topic.
   *
   * @param topic  a name of topic to write
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#to]]
   */
  def to(topic: String): Unit

  /**
   * Materializes this stream to a topic and creates a new stream from the topic.
   *
   * @param topic  a name of topic to write
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#through]]
   */
  def through(topic: String): KStream[K, V]
}

object KStream {
  /**
   * Creates a stream for the given topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param handler  an optional handler of stream errors
   */
  def apply[K, V](builder: StreamsBuilder, topic: KTopic[K, V])(implicit handler: ErrorHandler = null): KStream[K, V] = {
    lazy val streamLogger = Logger("kafka.streams." + topic.name)
    val errorHandler = if (handler == null) new LogErrorHandler[KStream[K, V]](streamLogger) else handler
    val deserializer = topic.valueSerde.deserializer
    val stream: KafkaStream[K, V] = builder.stream(topic.name, Consumed.`with`(topic.keySerde, Serdes.ByteArray))
      .filter { (key, value) =>
        Try(deserializer.deserialize(topic.name, value)).map(_ => true)
          .recover(errorHandler.handle(topic, DeserializeOperation)(key, value)).get
      }
      .flatMapValues(value => Collections.singletonList(deserializer.deserialize(topic.name, value)))
    new KStreamWrapper(topic, stream, errorHandler)
  }
}
