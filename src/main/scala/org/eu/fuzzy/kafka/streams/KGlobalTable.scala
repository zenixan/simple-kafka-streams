package org.eu.fuzzy.kafka.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{GlobalKTable, Materialized}
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Represents an abstraction of a changelog stream from a primary-keyed table.
 *
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * Records from the source topic that have null keys are dropped.
 *
 * Global table can only be used as right-hand side input for [[org.eu.fuzzy.kafka.streams.KStream stream]]-table joins.
 * Note that in contrast to [[org.eu.fuzzy.kafka.streams.KTable KTable]] a [[org.eu.fuzzy.kafka.streams.KGlobalTable KGlobalTable's]]
 * state holds a full copy of the underlying topic, thus all keys can be queried locally.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of value
 */
trait KGlobalTable[K, V] {
  /** Returns an underlying instance of Kafka Table. */
  private[streams] def internalTable: GlobalKTable[K, V]
}

object KGlobalTable {
  /** Represents a type of the local state store. */
  type StateStore = KeyValueStore[Bytes, Array[Byte]]

  /**
   * Creates a table for the given topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   */
  def apply[K, V](builder: StreamsBuilder, topic: KTopic[K, V]): KGlobalTable[K, V] =
    apply(builder, topic, Materialized.`with`[K, V, StateStore](topic.keySerde, topic.valueSerde))

  /**
   * Creates a table for the given topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param options  a set of options to use when materializing to the local state store
   */
  def apply[K, V](
                   builder: StreamsBuilder,
                   topic: KTopic[K, V],
                   options: Materialized[K, V, StateStore]): KGlobalTable[K, V] = {
    val table = builder.globalTable(topic.name, options)
    new KGlobalTable[K, V] {
      override private[streams] def internalTable = table
    }
  }
}
