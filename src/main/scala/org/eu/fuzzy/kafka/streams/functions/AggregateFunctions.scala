package org.eu.fuzzy.kafka.streams.functions

import scala.language.higherKinds
import org.eu.fuzzy.kafka.streams.KTable

/**
 * Represents a set of aggregation functions for the record/changelog stream.
 *
 * @tparam K  a type of input record key
 * @tparam KR a type of output record key
 * @tparam V  a type of record value
 * @tparam O  a type of options to use when materializing to the local state store, e.g.
 *            [[org.eu.fuzzy.kafka.streams.KTable.Options]]
 */
trait AggregateFunctions[K, KR, V, O[_ <: K, _]] {

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
   * @see [[org.apache.kafka.streams.kstream.KGroupedStream#count]]
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#count]]
   */
  def count(): KTable[KR, Long]

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
  def count(options: O[K, Long]): KTable[KR, Long]
}
