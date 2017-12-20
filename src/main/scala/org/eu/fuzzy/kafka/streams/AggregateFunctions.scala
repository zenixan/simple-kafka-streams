package org.eu.fuzzy.kafka.streams

import org.eu.fuzzy.kafka.streams.serialization.ValueSerde

trait AggregateFunctions[K, V] {
  /**
   * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
   *
   * Records with `null` key or value are ignored.
   *
   * @note The result of aggregation is written into a local store (which is basically an ever-updating materialized view)
   * that can be queried using the provided [[KTable.queryableStoreName]].
   * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
   *
   * @tparam VR  a value type of the result table
   *
   * @param initializer  a function to provide an initial intermediate aggregation result
   * @param aggregator  a function to compute a new aggregate result
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KGroupedStream#aggregate]]
   * @see [[org.apache.kafka.streams.kstream.KGroupedTable#aggregate]]
   */
  def aggregateByKey[VR](initializer: () => VR, aggregator: (K, V, VR) => VR)
                        (implicit serde: ValueSerde[VR]): KTable[K, VR]

  /**
   * Returns a new table with unmodified keys and values that represent the latest (rolling) aggregate for each key.
   *
   * Records with `null` key or value are ignored.
   *
   * @note The result of aggregation is written into a local store (which is basically an ever-updating materialized view)
   * that can be queried using the provided [[KTable.queryableStoreName]].
   * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
   *
   * @param reducer  a function to combine the values of records
   */
  def reduceByKey(reducer: (V, V) => V): KTable[K, V]
}
