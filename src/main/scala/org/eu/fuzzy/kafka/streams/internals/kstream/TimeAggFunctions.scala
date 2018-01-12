package org.eu.fuzzy.kafka.streams.internals.kstream

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{TimeWindowedKStream, Windowed, Windows}

import org.eu.fuzzy.kafka.streams.{KTopic, KTable}
import org.eu.fuzzy.kafka.streams.KTable.WindowOptions
import org.eu.fuzzy.kafka.streams.ErrorHandler
import org.eu.fuzzy.kafka.streams.functions.kstream.TimeWindowedFunctions
import org.eu.fuzzy.kafka.streams.serialization.{LongSerde, Serde}
import org.eu.fuzzy.kafka.streams.state.windowStoreOptions
import org.eu.fuzzy.kafka.streams.internals.{TableWrapper, toWindowedTopic, getStateStoreName}
import org.eu.fuzzy.kafka.streams.internals.{RichInitializer, RichAggregator, RichReducer}

/**
 * Implements an improved wrapper for the grouped record stream with time-based windowing.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic, always without a public name
 * @param windows  a window specification with time boundaries
 * @param internalStream  a native stream to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class TimeAggFunctions[K, V](topic: KTopic[K, V],
                                              windows: Windows[_],
                                              internalStream: TimeWindowedKStream[K, V],
                                              builder: StreamsBuilder,
                                              errorHandler: ErrorHandler)
    extends TimeWindowedFunctions[K, V] {

  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             options: WindowOptions[K, VR]): KTable[Windowed[K], VR] = {
    val aggregatedTable = internalStream.aggregate(initializer.asInitializer(topic, errorHandler),
                                                   aggregator.asAggregator(topic, errorHandler),
                                                   options.toMaterialized)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, aggregatedTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR, aggregator: (K, V, VR) => VR)
                            (implicit serde: Serde[VR]): KTable[Windowed[K], VR] = {
    val stateStore = windowStoreOptions(getStateStoreName, topic.keySerde, serde, windows)
    aggregate(initializer, aggregator, stateStore)
  }
  // format: on

  override def reduce(reducer: (V, V) => V,
                      options: WindowOptions[K, V]): KTable[Windowed[K], V] = {
    val aggregatedTable =
      internalStream.reduce(reducer.asReducer(topic, errorHandler), options.toMaterialized)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, aggregatedTable, builder, errorHandler)
  }

  override def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] =
    reduce(reducer, windowStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde, windows))

  override def count(options: WindowOptions[K, Long]): KTable[Windowed[K], Long] =
    aggregate(() => 0L, (_, _, counter) => counter + 1, options)

  override def count(): KTable[Windowed[K], Long] =
    count(windowStoreOptions(getStateStoreName, topic.keySerde, LongSerde, windows))
}
