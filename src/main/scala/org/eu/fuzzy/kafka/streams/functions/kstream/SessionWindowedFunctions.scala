package org.eu.fuzzy.kafka.streams.functions.kstream

import org.apache.kafka.streams.kstream.Windowed
import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.SessionOptions
import org.eu.fuzzy.kafka.streams.serialization.Serde

/**
 * Represents a set of aggregation functions with session-based windowing for a record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @define aggregateDesc
 * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
 *
 * Aggregating is a generalization of reduce and allows, for example, the aggregate value to have
 * a different type than the input values.
 *
 * The behavior of this operation is:
 *  - Records with `null` keys or values are ignored.
 *  - When a record key is received for the first time, the initializer is called.
 *  - When the windows overlap, they are merged using the merger into a single session and
 *    the old sessions are discarded.
 *
 * @define reduceDesc
 * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
 *
 * The behavior of this operation is:
 *  - Records with `null` keys or values are ignored.
 *  - The reducer will be called with the last reduced value and new value if a record with a non-null
 *    value is received.
 */
trait SessionWindowedFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.AggregateFunctions[K,
                                                                    Windowed[K],
                                                                    V,
                                                                    SessionOptions] {

  /**
   * $aggregateDesc
   *
   * @tparam VR  a type of the aggregate value
   *
   * @param initializer  a function to provide an initial intermediate aggregation result
   * @param aggregator  a function to compute a new aggregate result
   * @param merger  a function to merge two existing sessions into one
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.SessionWindowedKStream#aggregate]]
   */
  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    merger: (K, VR, VR) => VR,
                    options: SessionOptions[K, VR]): KTable[Windowed[K], VR]

  /**
   * $aggregateDesc
   *
   * @tparam VR  a type of the aggregate value
   *
   * @param initializer  a function to provide an initial intermediate aggregation result
   * @param aggregator  a function to compute a new aggregate result
   * @param merger  a function to merge two existing sessions into one
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.SessionWindowedKStream#aggregate]]
   */
  // format: off
  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    merger: (K, VR, VR) => VR)
                   (implicit serde: Serde[VR]): KTable[Windowed[K], VR]
  // format: on

  /**
   * $reduceDesc
   *
   * @param reducer  a function to combine the values of records
   *
   * @see [[org.apache.kafka.streams.kstream.SessionWindowedKStream#reduce]]
   */
  def reduce(reducer: (V, V) => V): KTable[Windowed[K], V]

  /**
   * $reduceDesc
   *
   * @param reducer  a function to combine the values of records
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.SessionWindowedKStream#reduce]]
   */
  def reduce(reducer: (V, V) => V, options: SessionOptions[K, V]): KTable[Windowed[K], V]
}
