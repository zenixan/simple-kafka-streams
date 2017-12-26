package org.eu.fuzzy.kafka.streams.functions.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde

/**
 * Represents a set of aggregation functions for a changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 *
 * @define aggregateDesc
 * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
 *
 * Aggregating is a generalization of reduce and allows, for example, the aggregate value to have a different type
 * than the input values.
 *
 * The behavior of this operation is:
 *  - Records with `null` keys are ignored.
 *  - When a record key is received for the first time, the initializer is called (and called before the adder).
 *  - When the first non-`null` value is received for a key then only the adder is called.
 *  - When subsequent non-`null` values are received for a key, then
 *    (1) the subtractor is called with the old value as stored in the table and
 *    (2) the adder is called with the new value of the input record that was just received.
 *    The order of execution for the subtractor and adder is not defined.
 *  - When a tombstone record (record with a `null` value) is received for a key then only the subtractor is called.
 *    Note that, whenever the subtractor returns a `null` value itself, then the corresponding key is removed from
 *    the resulting table. If that happens, any next input record for that key will re-initialize its aggregate value.
 *
 * @define reduceDesc
 * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
 *
 * The behavior of this operation is:
 *  - Records with `null` keys are ignored.
 *  - When a record key is received for the first time, the initializer is called (and called before the adder).
 *  - When the first non-`null` value is received for a key then only the adder is called.
 *  - When subsequent non-`null` values are received for a key, then
 *    (1) the subtractor is called with the old value as stored in the table and
 *    (2) the adder is called with the new value of the input record that was just received.
 *    The order of execution for the subtractor and adder is not defined.
 *  - When a tombstone record (record with a `null` value) is received for a key then only the subtractor is called.
 *    Note that, whenever the subtractor returns a `null` value itself, then the corresponding key is removed from
 *    the resulting table. If that happens, any next input record for that key will re-initialize its aggregate value.
 */
trait AggregateFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.AggregateFunctions[K, V] {

  /**
   * $aggregateDesc
   *
   * @tparam VR  a type of the aggregate value
   *
   * @param initializer  a function to provide an initial aggregate result
   * @param adder  a function to add a new record to the aggregation result
   * @param subtractor  a function to remove an old record from the aggregate result
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#aggregate]]
   */
  // format: off
  def aggregate[VR](initializer: () => VR, adder: (K, V, VR) => VR, subtractor: (K, V, VR) => VR)
                   (implicit serde: ValueSerde[VR]): KTable[K, VR]
  // format: on

  /**
   * $aggregateDesc
   *
   * @tparam VR  a type of the aggregate value
   *
   * @param initializer  a function to provide an initial aggregate result
   * @param adder  a function to add a new record to the aggregation result
   * @param subtractor  a function to remove an old record from the aggregate result
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#aggregate]]
   */
  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR,
                    options: Options[K, VR]): KTable[K, VR]

  /**
   * $reduceDesc
   *
   * @param adder  a function to add a new record to the aggregation result
   * @param subtractor  a function to remove an old record from the aggregate result
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#reduce]]
   */
  def reduce(adder: (V, V) => V, subtractor: (V, V) => V): KTable[K, V]

  /**
   * $reduceDesc
   *
   * @param adder  a function to add a new record to the aggregation result
   * @param subtractor  a function to remove an old record from the aggregate result
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#reduce]]
   */
  def reduce(adder: (V, V) => V, subtractor: (V, V) => V, options: Options[K, V]): KTable[K, V]
}
