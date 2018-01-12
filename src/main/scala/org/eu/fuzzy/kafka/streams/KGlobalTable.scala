package org.eu.fuzzy.kafka.streams

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.eu.fuzzy.kafka.streams.state.recordStoreOptions
import org.eu.fuzzy.kafka.streams.internals.getStateStoreName

/**
 * Represents an abstraction of a changelog stream from a primary-keyed table.
 *
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * Records from the source topic that have null keys are dropped.
 *
 * Global table can only be used as right-hand side input for [[org.eu.fuzzy.kafka.streams.KStream stream]]-table joins.
 * Note that in contrast to [[org.eu.fuzzy.kafka.streams.KTable KTable]]
 * a [[org.eu.fuzzy.kafka.streams.KGlobalTable KGlobalTable's]] state holds a full copy of the underlying topic,
 * thus all keys can be queried locally.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of value
 */
trait KGlobalTable[K, V] {

  /**
   * Returns a Kafka topic for this table.
   */
  def topic: KTopic[K, V]

  /** Returns an underlying instance of Kafka Table. */
  private[streams] def internalTable: GlobalKTable[K, V]
}

object KGlobalTable {

  /**
   * Creates a global table for the given topic.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   */
  def apply[K, V](builder: StreamsBuilder, topic: KTopic[K, V]): KGlobalTable[K, V] =
    apply(builder, topic, recordStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde))

  /**
   * Creates a global table for the given topic.
   *
   * @tparam K  a type of primary key
   * @tparam V  a type of record value
   *
   * @param builder  a builder of Kafka Streams topology
   * @param topic  an identity of Kafka topic
   * @param options  a set of options to use when materializing to the local state store
   */
  def apply[K, V](builder: StreamsBuilder,
                  topic: KTopic[K, V],
                  options: KTable.Options[K, V]): KGlobalTable[K, V] = {
    val table = builder.globalTable(topic.name, options.toMaterialized)
    val kafkaTopic = topic
    new KGlobalTable[K, V] {
      override def topic: KTopic[K, V] = kafkaTopic
      override private[streams] def internalTable = table
    }
  }
}
