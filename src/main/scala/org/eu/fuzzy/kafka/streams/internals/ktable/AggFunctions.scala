package org.eu.fuzzy.kafka.streams.internals.ktable

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KGroupedTable

import org.eu.fuzzy.kafka.streams.{KTopic, KTable}
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.ErrorHandler
import org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions
import org.eu.fuzzy.kafka.streams.serialization.{LongSerde, Serde}
import org.eu.fuzzy.kafka.streams.state.recordStoreOptions
import org.eu.fuzzy.kafka.streams.internals.{TableWrapper, getStateStoreName}
import org.eu.fuzzy.kafka.streams.internals.{RichInitializer, RichAggregator, RichReducer}

/**
 * Implements an improved wrapper for the grouped changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic, always without a public name
 * @param internalTable  a native table to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class AggFunctions[K, V](topic: KTopic[K, V],
                                          internalTable: KGroupedTable[K, V],
                                          builder: StreamsBuilder,
                                          errorHandler: ErrorHandler)
    extends AggregateFunctions[K, V] {

  override def aggregate[VR](initializer: () => VR,
                             adder: (K, V, VR) => VR,
                             subtractor: (K, V, VR) => VR,
                             options: Options[K, VR]): KTable[K, VR] = {
    val aggregatedTable = internalTable.aggregate(
      initializer.asInitializer(topic, errorHandler),
      adder.asAggregator(topic, errorHandler),
      subtractor.asAggregator(topic, errorHandler),
      options.toMaterialized
    )
    TableWrapper(options.toTopic, aggregatedTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR,
                             adder: (K, V, VR) => VR,
                             subtractor: (K, V, VR) => VR)
                            (implicit serde: Serde[VR]): KTable[K, VR] = {
    val stateStore = recordStoreOptions(getStateStoreName, topic.keySerde, serde)
    aggregate(initializer, adder, subtractor, stateStore)
  }
  // format: on

  override def reduce(adder: (V, V) => V,
                      subtractor: (V, V) => V,
                      options: Options[K, V]): KTable[K, V] = {
    val aggregatedTable = internalTable.reduce(adder.asReducer(topic, errorHandler),
                                               subtractor.asReducer(topic, errorHandler),
                                               options.toMaterialized)
    TableWrapper(options.toTopic, aggregatedTable, builder, errorHandler)
  }

  override def reduce(adder: (V, V) => V, subtractor: (V, V) => V): KTable[K, V] =
    reduce(adder, subtractor, recordStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde))

  override def count(options: Options[K, Long]) =
    aggregate(() => 0L, (_, _, counter) => counter + 1, (_, _, counter) => counter - 1, options)

  override def count() = count(recordStoreOptions(getStateStoreName, topic.keySerde, LongSerde))
}
