package org.eu.fuzzy.kafka.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{KTable => KafkaTable}
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}

import org.eu.fuzzy.kafka.streams.state.{StoreOptions, recordStoreOptions}
import org.eu.fuzzy.kafka.streams.support.LogErrorHandler
import org.eu.fuzzy.kafka.streams.functions.{
  JoinFunctions,
  IterativeFunctions,
  MaterializeFunctions
}
import org.eu.fuzzy.kafka.streams.functions.ktable.{
  FilterFunctions,
  TransformFunctions,
  GroupFunctions,
  BasicJoinFunctions,
  JoinFunctions => TableJoinFunctions
}
import org.eu.fuzzy.kafka.streams.internals.getStateStoreName

/**
 * Represents an abstraction of a changelog stream from a primary-keyed table with a set of table operations.
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 *
 * @note Records from the source topic that have null keys are dropped.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of value
 *
 * @see [[org.apache.kafka.streams.kstream.KTable]]
 */
trait KTable[K, V]
    extends KTable.Wrapper[K, V]
    with FilterFunctions[K, V]
    with MaterializeFunctions[K, V]
    with TransformFunctions[K, V]
    with IterativeFunctions[K, V]
    with JoinFunctions[K, V, TableJoinFunctions, BasicJoinFunctions]
    with GroupFunctions[K, V]

/**
 * Represents an abstraction of a changelog stream from a primary-keyed table.
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 *
 * @note Records from the source topic that have null keys are dropped.
 *
 * @see [[org.apache.kafka.streams.kstream.KTable]]
 */
object KTable {

  /**
   * Represents a set of options for table materializing to the local state store.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   */
  type Options[K, V] = StoreOptions[K, V, KeyValueStore[Bytes, Array[Byte]]]

  /**
   * Represents a set of options for storing the windowed aggregated values to the local state store.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   */
  type WindowOptions[K, V] = StoreOptions[K, V, WindowStore[Bytes, Array[Byte]]]

  /**
   * Represents a set of options for storing the aggregated values of sessions to the local state store.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   */
  type SessionOptions[K, V] = StoreOptions[K, V, SessionStore[Bytes, Array[Byte]]]

  /**
   * Creates a table for the given topic.
   *
   * [[org.eu.fuzzy.kafka.streams.support.LogErrorHandler LogErrorHandler]] will be used as the default error handler.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   */
  def apply[K, V](builder: StreamsBuilder, topic: KTopic[K, V]): KTable[K, V] =
    apply(builder, topic, LogErrorHandler("kafka.streams." + topic.name))

  /**
   * Creates a table for the given topic an error handler.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param handler  a handler of stream errors
   */
  def apply[K, V](builder: StreamsBuilder,
                  topic: KTopic[K, V],
                  handler: ErrorHandler): KTable[K, V] = {
    val stateStore = recordStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde)
    apply(builder, topic, stateStore, handler)
  }

  /**
   * Creates a table for the given topic.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param options  a set of options to use when materializing to the local state store
   * @param handler  a handler of stream errors
   */
  def apply[K, V](builder: StreamsBuilder,
                  topic: KTopic[K, V],
                  options: Options[K, V],
                  handler: ErrorHandler): KTable[K, V] =
    KStream(builder, topic, handler).groupByKey.reduce((_, newValue) => newValue, options)

  /**
   * Represents a wrapper for the changelog stream.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of value
   */
  trait Wrapper[K, V] {

    /**
     * Returns a Kafka topic for this table.
     *
     * @note The name of topic is absent for the streams which are created by any intermediate operations,
     *       e.g. [[org.eu.fuzzy.kafka.streams.functions.FilterFunctions.filter(* filter]],
     *       [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.map[KR,VR]* map]], etc.
     */
    def topic: KTopic[K, V]

    /** Returns a name of the local state store that can be used to query this table. */
    def queryableStoreName: String

    /**
     * Converts this table to the stream.
     *
     * @note This is a logical operation and only changes the "interpretation" of the stream,
     *       i.e. each record of this changelog stream is no longer treated as an update record.
     */
    def toStream: KStream[K, V]

    /** Returns an underlying instance of Kafka Table. */
    private[streams] def internalTable: KafkaTable[K, V]
  }
}
