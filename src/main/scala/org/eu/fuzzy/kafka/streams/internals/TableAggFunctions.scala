package org.eu.fuzzy.kafka.streams.internals

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KGroupedTable

import org.eu.fuzzy.kafka.streams.{KTopic, KTable}
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions
import org.eu.fuzzy.kafka.streams.serialization.{ValueSerde, LongValueSerde}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler

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
private[streams] class TableAggFunctions[K, V](topic: KTopic[K, V],
                                               internalTable: KGroupedTable[K, V],
                                               builder: StreamsBuilder,
                                               errorHandler: ErrorHandler)
    extends AggregateFunctions[K, V] {

  override def aggregate[VR](initializer: () => VR,
                             adder: (K, V, VR) => VR,
                             subtractor: (K, V, VR) => VR,
                             options: Options[K, VR]): KTable[K, VR] = {
    val newTopic = toTopic(options)
    val newTable = internalTable.aggregate(initializer.asInitializer(topic, errorHandler),
                                           adder.asAggregator(topic, errorHandler),
                                           subtractor.asAggregator(topic, errorHandler),
                                           options)
    TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR,
                             adder: (K, V, VR) => VR,
                             subtractor: (K, V, VR) => VR)
                            (implicit serde: ValueSerde[VR]): KTable[K, VR] =
    aggregate(initializer, adder, subtractor, storeOptions(topic.keySerde, serde))
  // format: on

  override def reduce(adder: (V, V) => V,
                      subtractor: (V, V) => V,
                      options: Options[K, V]): KTable[K, V] = {
    val newTopic = toTopic(options)
    val newTable = internalTable.reduce(adder.asReducer(topic, errorHandler),
                                        subtractor.asReducer(topic, errorHandler),
                                        options)
    new TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  override def reduce(adder: (V, V) => V, subtractor: (V, V) => V): KTable[K, V] =
    reduce(adder, subtractor, storeOptions(topic.keySerde, topic.valueSerde))

  override def count(options: Options[K, Long]) =
    aggregate(() => 0L, (_, _, counter) => counter + 1, (_, _, counter) => counter - 1, options)

  override def count() = count(storeOptions(topic.keySerde, LongValueSerde))
}
