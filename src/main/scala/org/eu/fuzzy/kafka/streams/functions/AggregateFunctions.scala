package org.eu.fuzzy.kafka.streams.functions

import org.eu.fuzzy.kafka.streams.{KTable, KTopic}
import org.eu.fuzzy.kafka.streams.serialization.LongValueSerde
import org.eu.fuzzy.kafka.streams.internals.storeOptions

/**
 * Represents a set of aggregation functions for the record/changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait AggregateFunctions[K, V] {

  /**
   * Returns a Kafka topic for this stream.
   *
   * @note The name of topic is absent for the streams which are created by any intermediate operations,
   *       e.g. [[org.eu.fuzzy.kafka.streams.functions.FilterFunctions.filter()]],
   *       [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.map()]], etc.
   */
  def topic: KTopic[K, V]

  /**
   * Returns a new table with unmodified keys and values that represent the latest count
   * (i.e., number of records) for each key.
   *
   * The behavior of this operation is:
   *  - Records with `null` keys or values are ignored for the record streams.
   *  - Input records with null keys are ignored for the changelog streams.
   *    Records with `null` values are not ignored but interpreted as '''tombstones'''
   *    for the corresponding key, which indicate the deletion of the key from the table.
   */
  def count(): KTable[K, Long] = count(storeOptions(topic.keySerde, LongValueSerde))

  /**
   * Returns a new table with unmodified keys and values that represent the latest count
   * (i.e., number of records) for each key.
   *
   * The behavior of this operation is:
   *  - Records with `null` keys or values are ignored for the record streams.
   *  - Input records with null keys are ignored for the changelog streams.
   *    Records with `null` values are not ignored but interpreted as '''tombstones'''
   *    for the corresponding key, which indicate the deletion of the key from the table.
   *
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedStream#count]]
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#count]]
   */
  def count(options: KTable.Options[K, Long]): KTable[K, Long]
}
