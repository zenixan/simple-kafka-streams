package org.eu.fuzzy.kafka.streams.internals

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Materialized, Windowed, TimeWindowedKStream}
import org.apache.kafka.streams.state.WindowStore

import org.eu.fuzzy.kafka.streams.{KTable, KTopic}
import org.eu.fuzzy.kafka.streams.KTable.WindowOptions
import org.eu.fuzzy.kafka.streams.functions.kstream.TimeWindowedFunctions
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, LongValueSerde, ValueSerde}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler

/**
 * Implements an improved wrapper for the grouped record stream with time-based windowing.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic, always without a public name
 * @param internalStream  a native stream to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class TimeAggFunctions[K, V](topic: KTopic[K, V],
                                              internalStream: TimeWindowedKStream[K, V],
                                              builder: StreamsBuilder,
                                              errorHandler: ErrorHandler)
    extends TimeWindowedFunctions[K, V] {

  /** Returns the materialization options. */
  @inline private def storeOptions[KR, VR](keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]) =
    Materialized.`with`[KR, VR, WindowStore[Bytes, Array[Byte]]](keySerde, valueSerde)

  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             options: WindowOptions[K, VR]): KTable[Windowed[K], VR] = {
    val newTable = internalStream.aggregate(initializer.asInitializer(topic, errorHandler),
                                            aggregator.asAggregator(topic, errorHandler),
                                            options)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR, aggregator: (K, V, VR) => VR)
                            (implicit serde: ValueSerde[VR]): KTable[Windowed[K], VR] =
    aggregate(initializer, aggregator, storeOptions(topic.keySerde, serde))
  // format: on

  override def reduce(reducer: (V, V) => V,
                      options: WindowOptions[K, V]): KTable[Windowed[K], V] = {
    val newTable = internalStream.reduce(reducer.asReducer(topic, errorHandler), options)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  override def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] =
    reduce(reducer, storeOptions(topic.keySerde, topic.valueSerde))

  override def count(options: WindowOptions[K, Long]): KTable[Windowed[K], Long] =
    aggregate(() => 0L, (_, _, counter) => counter + 1, options)

  override def count(): KTable[Windowed[K], Long] =
    count(storeOptions(topic.keySerde, LongValueSerde))
}
