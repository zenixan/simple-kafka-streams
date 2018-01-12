package org.eu.fuzzy.kafka.streams

import scala.util.Try

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.apache.kafka.streams.kstream.{KStream => KafkaStream}

import org.eu.fuzzy.kafka.streams.functions.{kstream, FilterFunctions, JoinFunctions}
import org.eu.fuzzy.kafka.streams.internals.StreamWrapper
import org.eu.fuzzy.kafka.streams.support.LogErrorHandler

/**
 * Represents an abstraction of a record stream with a set of stream operations.
 *
 * Each record is an independent entity/event in the real world, e.g. a user X might buy two items I1 and I2,
 * and thus there might be two records `<K:I1>, <K:I2>` in the stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @see [[org.apache.kafka.streams.kstream.KStream]]
 */
trait KStream[K, V]
    extends KStream.Wrapper[K, V]
    with FilterFunctions[K, V, KStream]
    with kstream.MaterializeFunctions[K, V]
    with kstream.TransformFunctions[K, V]
    with kstream.FlowFunctions[K, V]
    with kstream.IterativeFunctions[K, V]
    with JoinFunctions[K, V, kstream.JoinFunctions, kstream.TableJoinFunctions]
    with kstream.GroupFunctions[K, V]

/**
 * Represents an abstraction of a record stream.
 *
 * Each record is an independent entity/event in the real world, e.g. a user X might buy two items I1 and I2,
 * and thus there might be two records `<K:I1>, <K:I2>` in the stream.
 *
 * @see [[org.apache.kafka.streams.kstream.KStream]]
 */
object KStream {

  /**
   * Creates a stream for the given topic.
   *
   * [[org.eu.fuzzy.kafka.streams.support.LogErrorHandler LogErrorHandler]] will be used as the default error handler.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   */
  def apply[K, V](builder: StreamsBuilder, topic: KTopic[K, V]): KStream[K, V] =
    apply(builder, topic, LogErrorHandler("kafka.streams." + topic.name))

  /**
   * Creates a stream for the given topic and error handler.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param handler  a handler of stream errors
   */
  def apply[K, V](builder: StreamsBuilder,
                  topic: KTopic[K, V],
                  handler: ErrorHandler): KStream[K, V] = {
    val deserializer = topic.valSerde.deserializer
    val stream: KafkaStream[K, V] = builder
      .stream(topic.name, Consumed.`with`(topic.keySerde, Serdes.ByteArray))
      .filter { (key, value) =>
        Try(deserializer.deserialize(topic.name, value))
          .map(_ => true)
          .recover { case error => handler.onDeserializeError(error, topic, key, value); false }
          .get
      }
      .mapValues(deserializer.deserialize(topic.name, _))
    StreamWrapper(topic, stream, builder, handler)
  }

  /**
   * Represents a wrapper for the record stream.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   */
  trait Wrapper[K, V] {

    /**
     * Returns a Kafka topic for this stream.
     *
     * @note The name of topic is absent for the streams which are created by any intermediate operations,
     *       e.g. [[org.eu.fuzzy.kafka.streams.functions.FilterFunctions.filter(* filter]],
     *       [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.map[KR,VR]* map]], etc.
     */
    def topic: KTopic[K, V]

    /** Returns an underlying instance of Kafka Stream. */
    private[streams] def internalStream: KafkaStream[K, V]
  }
}
