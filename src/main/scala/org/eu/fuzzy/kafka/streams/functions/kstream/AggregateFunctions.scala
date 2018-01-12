package org.eu.fuzzy.kafka.streams.functions.kstream

import scala.language.higherKinds
import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.DocDummy
import org.eu.fuzzy.kafka.streams.serialization.Serde

/**
 * Represents a set of aggregation functions for a record stream.
 *
 * @tparam K  a type of record key
 * @tparam KR a type of output record key
 * @tparam V  a type of record value
 * @tparam O  a type of options to use when materializing to the local state store, e.g.
 *            [[org.eu.fuzzy.kafka.streams.KTable.Options]]
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
 *
 * @define reduceDesc
 * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
 *
 * The behavior of this operation is:
 *  - Records with `null` keys or values are ignored.
 *  - The reducer will be called with the last reduced value and new value if a record with a non-null
 *    value is received.
 */
trait AggregateFunctions[K, KR, V, O[_ <: K, _]]
    extends org.eu.fuzzy.kafka.streams.functions.AggregateFunctions[K, KR, V, O] with DocDummy {

  /**
   * $aggregateDesc
   *
   * @tparam VR  a type of the aggregate value
   *
   * @param initializer  a function to provide an initial intermediate aggregation result
   * @param aggregator  a function to compute a new aggregate result
   * @param serde  a serialization format for the output record value
   */
  // format: off
  def aggregate[VR](initializer: () => VR, aggregator: (K, V, VR) => VR)
                   (implicit serde: Serde[VR]): KTable[KR, VR]
  // format: on

  /**
   * $aggregateDesc
   *
   * @tparam VR  a type of the aggregate value
   *
   * @param initializer  a function to provide an initial intermediate aggregation result
   * @param aggregator  a function to compute a new aggregate result
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedStream#aggregate]]
   */
  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    options: O[K, VR]): KTable[KR, VR]

  /**
   * $reduceDesc
   *
   * @param reducer  a function to combine the values of records
   *
   * @see [[$kafkaApiMapping/kstream/KGroupedStream.html#reduce-org.apache.kafka.streams.kstream.Reducer- reduce javadoc]]
   */
  def reduce(reducer: (V, V) => V): KTable[KR, V]

  /**
   * $reduceDesc
   *
   * @param reducer  a function to combine the values of records
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedStream#reduce]]
   */
  def reduce(reducer: (V, V) => V, options: O[K, V]): KTable[KR, V]
}
