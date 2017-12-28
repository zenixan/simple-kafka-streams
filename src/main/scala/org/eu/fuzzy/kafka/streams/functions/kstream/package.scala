package org.eu.fuzzy.kafka.streams.functions

import org.apache.kafka.streams.kstream.Windowed
import org.eu.fuzzy.kafka.streams.KTable.{Options, WindowOptions}

package object kstream {

  /**
   * Represents a set of aggregation functions with time-based windowing for a record stream.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   */
  type TimeWindowedFunctions[K, V] = kstream.AggregateFunctions[K, Windowed[K], V, WindowOptions]

  private[streams] type AggFunctions[K, KR, V, _] = kstream.AggregateFunctions[K, K, V, Options]
}
