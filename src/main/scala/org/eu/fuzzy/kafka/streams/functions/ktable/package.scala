package org.eu.fuzzy.kafka.streams.functions

package object ktable {
  private[streams] type AggFunctions[K, KR, V, _] = ktable.AggregateFunctions[K, V]
}
