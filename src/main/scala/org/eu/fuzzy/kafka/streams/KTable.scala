package org.eu.fuzzy.kafka.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Materialized, KTable => KafkaTable}
import org.apache.kafka.streams.state.KeyValueStore

import org.eu.fuzzy.kafka.streams.error.ErrorHandler
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde
import org.eu.fuzzy.kafka.streams.support.LogErrorHandler
import com.typesafe.scalalogging.Logger

/**
 * Represents an abstraction of a changelog stream from a primary-keyed table.
 *
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * Records from the source topic that have null keys are dropped.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of value
 *
 * @see [[org.apache.kafka.streams.kstream.KTable]]
 */
trait KTable[K, V] extends StreamFunctions[K, V, KTable] {
  /** Represents a type of the local state store. */
  type StateStore = KeyValueStore[Bytes, Array[Byte]]

  /** Returns an underlying instance of Kafka Table. */
  private[streams] def internalTable: KafkaTable[K, V]

  /** Returns a name of the local state store that can be used to query this table. */
  def queryableStoreName: String

  /**
   * Returns a new table that consists of all records of this table which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   * For each record that don't satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#filter]]
   */
  def filter(predicate: (K, V) => Boolean, options: Materialized[K, V, StateStore]): KTable[K, V]

  /**
   * Returns a new table that consists of all records of this table which don't satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   * For each record that does satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#filterNot]]
   */
  def filterNot(predicate: (K, V) => Boolean, options: Materialized[K, V, StateStore]): KTable[K, V] =
    filter((key, value) => !predicate(key, value), options)

  /**
   * Returns a new table that consists of all records of this table which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   * For each record that don't satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record key
   * @param options  a set of options to use when materializing to the local state store
   */
  def filterKeys(predicate: K => Boolean, options: Materialized[K, V, StateStore]): KTable[K, V] =
    filter((key, _) => predicate(key), options)

  /**
   * Returns a new table that consists of all records of this table which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   * For each record that don't satisfy the predicate  a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record value
   * @param options  a set of options to use when materializing to the local state store
   */
  def filterValues(predicate: V => Boolean, options: Materialized[K, V, StateStore]): KTable[K, V] =
    filter((_, value) => predicate(value), options)

  /**
   * Returns a new table with a new value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new value for each record
   * @param options  a set of options to use when materializing to the local state store
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#mapValues]]
   */
  def mapValues[VR](mapper: V => VR, options: Materialized[K, VR, StateStore])
                   (implicit serde: ValueSerde[VR]): KTable[K, VR]

  /**
   * Converts this table to the stream.
   *
   * @note This is a logical operation and only changes the "interpretation" of the stream, i.e.,
   *       each record of this changelog stream is no longer treated as an update record.
   */
  def toStream: KStream[K, V]

  /**
   * Materializes this table to a topic and creates a new table from the topic.
   *
   * @param topic  a name of topic to write
   * @param options  a set of options to use when producing to the topic
   */
  def through(topic: String, options: Materialized[K, V, StateStore]): KTable[K, V]
}

object KTable {
  /**
   * Creates a table for the given topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param handler  an optional handler of stream errors
   */
  def apply[K, V](builder: StreamsBuilder, topic: KTopic[K, V])(implicit handler: ErrorHandler = null): KTable[K, V] = {
    lazy val streamLogger = Logger("kafka.streams." + topic.name)
    val errorHandler = if (handler == null) new LogErrorHandler[KStream[K, V]](streamLogger) else handler
    KStream(builder, topic)(errorHandler).reduceByKey((_, newValue) => newValue)
  }
}
