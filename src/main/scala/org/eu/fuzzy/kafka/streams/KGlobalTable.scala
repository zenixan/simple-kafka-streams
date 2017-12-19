package org.eu.fuzzy.kafka.streams

import org.apache.kafka.streams.kstream.GlobalKTable

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
