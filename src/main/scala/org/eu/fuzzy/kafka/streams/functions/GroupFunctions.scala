package org.eu.fuzzy.kafka.streams.functions

import scala.language.higherKinds
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.serialization.KeySerde

/**
 * Represents a set of functions to group the records of record/changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam S  a type of aggregated stream, i.e.
 *            [[org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions]] or
 *            [[org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions]]
 */
trait GroupFunctions[K, V, S[_, _, _, _]] {

  /**
   * Groups the records of this stream by their current key.
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#groupByKey]]
   */
  def groupByKey: S[K, K, V, Options[K, V]]

  /**
   * Groups the records of this stream using the given function.
   *
   * @tparam KR  a new type of record key
   *
   * @param mapper  a function to compute a new grouping key
   * @param serde  a serialization format for the output record key
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#groupBy]]
   * @see [[org.apache.kafka.streams.kstream.KTable#groupBy]]
   */
  def groupBy[KR](mapper: (K, V) => KR)(implicit serde: KeySerde[KR]): S[KR, KR, V, Options[KR, V]]

}
