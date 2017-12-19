package org.eu.fuzzy.kafka.streams

import org.apache.kafka.streams.kstream.{KTable => KafkaTable}

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
trait KTable[K, V] {

  /** Returns an underlying instance of Kafka Table. */
  private[streams] def internalTable: KafkaTable[K, V]

  /**
   * Returns a Kafka topic for this table.
   *
   * @note The name of topic is absent for the streams which are created by any intermediate operations,
   *       e.g. [[filter()]], [[map()]], etc.
   */
  def topic: KTopic[K, V]

}
