package org.eu.fuzzy.kafka.streams.internals.kstream

import scala.util.Try

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Merger, SessionWindowedKStream, Windowed, SessionWindows}

import org.eu.fuzzy.kafka.streams.{KTopic, KTable}
import org.eu.fuzzy.kafka.streams.KTable.SessionOptions
import org.eu.fuzzy.kafka.streams.ErrorHandler
import org.eu.fuzzy.kafka.streams.functions.kstream.SessionWindowedFunctions
import org.eu.fuzzy.kafka.streams.serialization.{LongSerde, Serde}
import org.eu.fuzzy.kafka.streams.state.sessionStoreOptions
import org.eu.fuzzy.kafka.streams.internals.{TableWrapper, toWindowedTopic, getStateStoreName}
import org.eu.fuzzy.kafka.streams.internals.{RichInitializer, RichAggregator, RichReducer}

/**
 * Implements an improved wrapper for the grouped record stream with session-based windowing.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic, always without a public name
 * @param windows  a window specification without fixed time boundaries
 * @param internalStream  a native stream to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class SessionAggFunctions[K, V](topic: KTopic[K, V],
                                                 windows: SessionWindows,
                                                 internalStream: SessionWindowedKStream[K, V],
                                                 builder: StreamsBuilder,
                                                 errorHandler: ErrorHandler)
    extends SessionWindowedFunctions[K, V] {

  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             merger: (K, VR, VR) => VR,
                             options: SessionOptions[K, VR]): KTable[Windowed[K], VR] = {
    val sessionMerger: Merger[K, VR] = (key, aggValue1, aggValue2) =>
      Try(merger(key, aggValue1, aggValue2)).recover {
        case error => errorHandler.onMergeError(error, topic, key, aggValue1, aggValue2)
      }.get
    val aggregatedTable = internalStream.aggregate(initializer.asInitializer(topic, errorHandler),
                                                   aggregator.asAggregator(topic, errorHandler),
                                                   sessionMerger,
                                                   options.toMaterialized)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, aggregatedTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             merger: (K, VR, VR) => VR)
                            (implicit serde: Serde[VR]): KTable[Windowed[K], VR] = {
    val stateStore = sessionStoreOptions(getStateStoreName, topic.keySerde, serde, windows)
    aggregate(initializer, aggregator, merger, stateStore)
  }
  // format: on

  override def reduce(reducer: (V, V) => V,
                      options: SessionOptions[K, V]): KTable[Windowed[K], V] = {
    val aggregatedTable =
      internalStream.reduce(reducer.asReducer(topic, errorHandler), options.toMaterialized)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, aggregatedTable, builder, errorHandler)
  }

  override def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] =
    reduce(reducer, sessionStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde, windows))

  override def count(options: SessionOptions[K, Long]): KTable[Windowed[K], Long] =
    aggregate(() => 0L,
              (_, _, counter) => counter + 1,
              (_, counter1, counter2) => counter1 + counter2,
              options)

  override def count(): KTable[Windowed[K], Long] =
    count(sessionStoreOptions(getStateStoreName, topic.keySerde, LongSerde, windows))
}
