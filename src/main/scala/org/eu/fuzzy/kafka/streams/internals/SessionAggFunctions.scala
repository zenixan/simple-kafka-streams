package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Materialized, Windowed, SessionWindowedKStream, Merger}
import org.apache.kafka.streams.state.SessionStore

import org.eu.fuzzy.kafka.streams.{KTable, KTopic}
import org.eu.fuzzy.kafka.streams.KTable.SessionOptions
import org.eu.fuzzy.kafka.streams.functions.kstream.SessionWindowedFunctions
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, LongValueSerde, ValueSerde}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler

/**
 * Implements an improved wrapper for the grouped record stream with session-based windowing.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic, always without a public name
 * @param internalStream  a native stream to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class SessionAggFunctions[K, V](topic: KTopic[K, V],
                                                 internalStream: SessionWindowedKStream[K, V],
                                                 builder: StreamsBuilder,
                                                 errorHandler: ErrorHandler)
    extends SessionWindowedFunctions[K, V] {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  /** Returns the materialization options. */
  @inline private def storeOptions[KR, VR](keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]) =
    Materialized.`with`[KR, VR, SessionStore[Bytes, Array[Byte]]](keySerde, valueSerde)

  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             merger: (K, VR, VR) => VR,
                             options: SessionOptions[K, VR]): KTable[Windowed[K], VR] = {
    val sessionMerger: Merger[K, VR] = (key, aggValue1, aggValue2) =>
      Try(merger(key, aggValue1, aggValue2))
        .recover(errorHandler.handle(topic, MergeOperation, key, aggValue1, aggValue2))
        .get
    val newTable = internalStream.aggregate(initializer.asInitializer(topic, errorHandler),
                                            aggregator.asAggregator(topic, errorHandler),
                                            sessionMerger,
                                            options)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  // format: off
  override def aggregate[VR](initializer: () => VR,
                             aggregator: (K, V, VR) => VR,
                             merger: (K, VR, VR) => VR)
                            (implicit serde: ValueSerde[VR]): KTable[Windowed[K], VR] =
    aggregate(initializer, aggregator, merger, storeOptions(topic.keySerde, serde))
  // format: on

  override def reduce(reducer: (V, V) => V,
                      options: SessionOptions[K, V]): KTable[Windowed[K], V] = {
    val newTable = internalStream.reduce(reducer.asReducer(topic, errorHandler), options)
    val newTopic = toWindowedTopic(options)
    TableWrapper(newTopic, newTable, builder, errorHandler)
  }

  override def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] =
    reduce(reducer, storeOptions(topic.keySerde, topic.valueSerde))

  override def count(options: SessionOptions[K, Long]): KTable[Windowed[K], Long] =
    aggregate(() => 0L,
              (_, _, counter) => counter + 1,
              (_, counter1, counter2) => counter1 + counter2,
              options)

  override def count(): KTable[Windowed[K], Long] =
    count(storeOptions(topic.keySerde, LongValueSerde))
}
