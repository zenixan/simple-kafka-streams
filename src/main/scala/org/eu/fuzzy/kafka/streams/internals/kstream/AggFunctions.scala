package org.eu.fuzzy.kafka.streams.internals.kstream

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KGroupedStream

import org.eu.fuzzy.kafka.streams.{KTopic, KTable}
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.ErrorHandler
import org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions
import org.eu.fuzzy.kafka.streams.serialization.{LongSerde, Serde}
import org.eu.fuzzy.kafka.streams.state.recordStoreOptions
import org.eu.fuzzy.kafka.streams.internals.{TableWrapper, getStateStoreName}
import org.eu.fuzzy.kafka.streams.internals.{RichInitializer, RichAggregator, RichReducer}

/**
 * Implements an improved wrapper for the grouped record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic, always without a public name
 * @param internalStream  a native stream to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class AggFunctions[K, V](topic: KTopic[K, V],
                                          internalStream: KGroupedStream[K, V],
                                          builder: StreamsBuilder,
                                          errorHandler: ErrorHandler)
    extends AggregateFunctions[K, K, V, Options] {

  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             options: Options[K, VR]): KTable[K, VR] = {
    val newTable = internalStream.aggregate(initializer.asInitializer(topic, errorHandler),
                                            aggregator.asAggregator(topic, errorHandler),
                                            options.toMaterialized)
    val newTopic = KTopic(options.keySerde, options.valSerde)
    TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR, aggregator: (K, V, VR) => VR)
                            (implicit serde: Serde[VR]): KTable[K, VR] =
    aggregate(initializer, aggregator, recordStoreOptions(getStateStoreName, topic.keySerde, serde))
  // format: on

  override def reduce(reducer: (V, V) => V, options: Options[K, V]): KTable[K, V] = {
    val newTable =
      internalStream.reduce(reducer.asReducer(topic, errorHandler), options.toMaterialized)
    TableWrapper(topic, newTable, builder, errorHandler)
  }

  override def reduce(reducer: (V, V) => V): KTable[K, V] =
    reduce(reducer, recordStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde))

  override def count(options: Options[K, Long]): KTable[K, Long] =
    aggregate(() => 0L, (_, _, counter) => counter + 1, options)

  override def count(): KTable[K, Long] =
    count(recordStoreOptions(getStateStoreName, topic.keySerde, LongSerde))
}
