package org.eu.fuzzy.kafka.streams

import scala.util.Try
import java.util.Collections

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.apache.kafka.streams.kstream.{JoinWindows, KStream => KafkaStream}

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
   * @param keySerde  a serialization format for the output record key
   * @param valueSerde  a serialization format for the output record value
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
   * @param serde  a serialization format for the output record value
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
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#mapValues]]
   */
  @inline def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR] =
    flatMapValues(mapper.andThen(Seq(_)))

  /**
   * Returns a new stream with a zero or more records for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute the new output records
   * @param keySerde  a serialization format for the output record key
   * @param valueSerde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#flatMap]]
   */
  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                     (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR]

  /**
   * Returns a new stream with a zero or more records with unmodified keys and new values for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute the new output values
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#flatMapValues]]
   */
  def flatMapValues[VR](mapper: V => Iterable[VR])(implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the global table using non-windowed inner join.
   *
   * Two records are only joined if the key that's calculated by a `mapper` function is equal to the key in the global table.
   * If a record key or value is `null` the stream record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * @tparam GK  a type of record key in the global table
   * @tparam GV  a type of record value in the global table
   * @tparam VR  a value type of the result stream
   *
   * @param globalTable  a global table to be joined with this stream
   * @param mapper  a function to map the record of this stream to the key of the global table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  def join[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                      (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the table using non-windowed inner join.
   *
   * Two records are only joined if the stream record has processed and the record key is equal to the key in the table.
   * If a record key or value is `null` the stream record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * <table border='1'>
   *  <caption>Example</caption>
   *  <tr>
   *    <th>KStream</th>
   *    <th>KTable</th>
   *    <th>state</th>
   *    <th>result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:C&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:joiner(C, b)&gt;</td>
   *  </tr>
   * </table>
   *
   * @note Both input streams (or to be more precise, their underlying source topics) need to have the same number of partitions.
   * If this is not the case, you would need to call [[through()]] for this stream before doing the join,
   * using a pre-created topic with the "correct" number of partitions.
   * Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
   * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
   * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
   *
   * Repartitioning can happen only for this stream but not for the given table.
   * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
   * records to it, and rereading all records from it, such that the join input stream is partitioned correctly on its key.
   *
   * @tparam VT  a value type of the table
   * @tparam VR  a value type of the result stream
   *
   * @param table  a table to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   */
  def join[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using windowed inner join.
   *
   * Two records are only joined if their keys are equal and timestamps are close to each other as defined
   * by the given time window.
   * If an input record key or value is `null` the record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * <table border='1'>
   *  <caption>Example (assuming all input records belong to the correct windows)</caption>
   *  <tr>
   *    <th>this</th>
   *    <th>other</th>
   *    <th>result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K2:B&gt;</td>
   *    <td>&lt;K2:b&gt;</td>
   *    <td>&lt;K2:joiner(B, b)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K3:c&gt;</td>
   *    <td></td>
   *  </tr>
   * </table>
   *
   * @note Both input streams (or to be more precise, their underlying source topics) need to have the same number of partitions.
   * If this is not the case, you would need to call [[through()]] (for one input stream) before doing the
   * join, using a pre-created topic with the "correct" number of partitions.
   * Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
   * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
   * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
   *
   * Repartitioning can happen for one or both of the joining streams.
   * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
   * records to it, and rereading all records from it, such that the join input stream is partitioned correctly on its key.
   *
   * Both of the joining streams will be materialized in local state stores with auto-generated store names.
   * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
   *
   * @tparam VO  a value type of the other stream
   * @tparam VR  a value type of the result stream
   *
   * @param otherStream  a stream to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param windows  a time window
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  def join[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                  (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the global table using non-windowed left join.
   *
   * The `joiner` function will be called to compute a new value for each record from the stream whether or not it finds
   * a record with equal keys in the global table.
   * If no record in the global table was found during lookup, a `null` value will be provided to the `joiner` function.
   * If an record key or value is `null` then the stream record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * @tparam GK  a type of record key in the global table
   * @tparam GV  a type of record value in the global table
   * @tparam VR  a value type of the result stream
   *
   * @param globalTable  a global table to be joined with this stream
   * @param mapper  a function to map the record of this stream to the key of the global table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#leftJoin]]
   */
  def leftJoin[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                          (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the table using non-windowed left join.
   *
   * The `joiner` function will be called to compute a new value for each record from the stream whether or not it finds
   * a record with equal key in the table.
   * If no record in the table was found during lookup, a `null` value will be provided to the `joiner` function.
   * If an record key or value is `null` then the stream record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * <table border='1'>
   *  <caption>Example</caption>
   *  <tr>
   *    <th>KStream</th>
   *    <th>KTable</th>
   *    <th>state</th>
   *    <th>result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td></td>
   *   </tr>
   *  <tr>
   *    <td>&lt;K1:C&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:joiner(C, b)&gt;</td>
   *  </tr>
   * </table>
   *
   * @note Both input streams (or to be more precise, their underlying source topics) need to have the same number of partitions.
   * If this is not the case, you would need to call [[through()]] for this stream before doing the join,
   * using a pre-created topic with the "correct" number of partitions.
   * Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
   * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
   * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
   *
   * Repartitioning can happen only for this stream but not for the given table.
   * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
   * records to it, and rereading all records from it, such that the join input stream is partitioned correctly on its key.
   *
   * @tparam VT  a value type of the table
   * @tparam VR  a value type of the result stream
   *
   * @param table  a table to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#leftJoin]]
   */
  def leftJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using windowed left join.
   *
   * Two records are only joined if their keys are equal and timestamps are close to each other as defined
   * by the given time window, otherwise a `null` value will be provided to the `joiner` function for the other stream.
   * If an input record key or value is `null` the record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * <table border='1'>
   *  <caption>Example (assuming all input records belong to the correct windows)</caption>
   *  <tr>
   *    <th>this</th>
   *    <th>other</th>
   *    <th>result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K2:B&gt;</td>
   *    <td>&lt;K2:b&gt;</td>
   *    <td>&lt;K2:joiner(B, b)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K3:c&gt;</td>
   *    <td></td>
   *  </tr>
   * </table>
   *
   * @note Both input streams (or to be more precise, their underlying source topics) need to have the same number of partitions.
   * If this is not the case, you would need to call [[through()]] (for one input stream) before doing the
   * join, using a pre-created topic with the "correct" number of partitions.
   * Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
   * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
   * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
   *
   * Repartitioning can happen for one or both of the joining streams.
   * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
   * records to it, and rereading all records from it, such that the join input stream is partitioned correctly on its key.
   *
   * Both of the joining streams will be materialized in local state stores with auto-generated store names.
   * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
   *
   * @tparam VO  a value type of the other stream
   * @tparam VR  a value type of the result stream
   *
   * @param otherStream  a stream to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param windows  a time window
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  def lefJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                     (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using windowed outer join.
   *
   * Two records are only joined if their keys are equal and timestamps are close to each other as defined
   * by the given time window, otherwise a `null` value will be provided to the `joiner` function for the this/other stream.
   * If an input record key or value is `null` the record will not be included in the join operation and thus no
   * output record will be added to the resulting stream.
   *
   * <table border='1'>
   *  <caption>Example (assuming all input records belong to the correct windows)</caption>
   *  <tr>
   *    <th>this</th>
   *    <th>other</th>
   *    <th>result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K2:B&gt;</td>
   *    <td>&lt;K2:b&gt;</td>
   *    <td>&lt;K2:joiner(null, b)&gt;<br />&lt;K2:joiner(B, b)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K3:c&gt;</td>
   *    <td>&lt;K3:joiner(null, c)&gt;</td>
   *  </tr>
   * </table>
   *
   * @note Both input streams (or to be more precise, their underlying source topics) need to have the same number of partitions.
   * If this is not the case, you would need to call [[through()]] (for one input stream) before doing the
   * join, using a pre-created topic with the "correct" number of partitions.
   * Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
   * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
   * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
   *
   * Repartitioning can happen for one or both of the joining streams.
   * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
   * records to it, and rereading all records from it, such that the join input stream is partitioned correctly on its key.
   *
   * Both of the joining streams will be materialized in local state stores with auto-generated store names.
   * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
   *
   * @tparam VO  a value type of the other stream
   * @tparam VR  a value type of the result stream
   *
   * @param otherStream  a stream to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param windows  a time window
   * @param serde  a serialization format for the output record value
   */
  def outerJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                       (implicit serde: ValueSerde[VR]): KStream[K, VR]

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
