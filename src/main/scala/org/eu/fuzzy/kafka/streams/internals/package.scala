package org.eu.fuzzy.kafka.streams

import scala.util.Try

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{KeyValueMapper, ValueJoiner, Windowed}
import org.apache.kafka.streams.kstream.{Aggregator, Initializer, Materialized, Reducer}
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.KeyValueStore

import org.eu.fuzzy.kafka.streams.error.ErrorHandler
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, WindowedKeySerde, ValueSerde}

package object internals {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  private[streams] implicit class RichKeyMapper[K, V, KR](f: (K, V) => KR) {

    /** Wraps a key-mapper function by the given error handler.  */
    @inline def asKeyMapper(topic: KTopic[K, V], handler: ErrorHandler): KeyValueMapper[K, V, KR] =
      (key, value) =>
        Try(f(key, value))
          .recover(handler.handle(topic, MapOperation, key, value))
          .get
  }

  private[streams] implicit class RichJoiner[V, VO, VR](f: (V, VO) => VR) {

    /** Wraps an inner joiner function by the given error handler. */
    @inline def asInnerJoiner[K](topic: KTopic[K, V],
                                 handler: ErrorHandler): ValueJoiner[V, VO, VR] =
      (value1, value2) =>
        Try(f(value1, value2))
          .recover(handler.handle(topic, InnerJoinOperation, value1, value2))
          .get

    /** Wraps a left joiner function by the given error handler. */
    @inline def asLeftJoiner[K](topic: KTopic[K, V],
                                handler: ErrorHandler): ValueJoiner[V, VO, VR] =
      (value1, value2) =>
        Try(f(value1, value2)).recover(handler.handle(topic, LeftJoinOperation, value1, value2)).get

    /** Wraps an outer joiner function by the given error handler. */
    @inline def asOuterJoiner[K](topic: KTopic[K, V],
                                 handler: ErrorHandler): ValueJoiner[V, VO, VR] =
      (value1, value2) =>
        Try(f(value1, value2))
          .recover(handler.handle(topic, OuterJoinOperation, value1, value2))
          .get
  }

  private[streams] implicit class RichInitializer[VR](f: () => VR) {

    /** Wraps an aggregate supplier by the given error handler. */
    @inline def asInitializer[K, V](topic: KTopic[K, V], handler: ErrorHandler): Initializer[VR] =
      () => Try(f()).recover(handler.handle(topic, InitializerOperation)).get
  }

  private[streams] implicit class RichReducer[V](f: (V, V) => V) {

    /** Wraps a reducer function by the given error handler. */
    @inline def asReducer[K](topic: KTopic[K, V], handler: ErrorHandler): Reducer[V] =
      (aggregate, value) =>
        Try(f(aggregate, value))
          .recover(handler.handle(topic, ReduceOperation, aggregate, value))
          .get
  }

  private[streams] implicit class RichAggregator[K, V, VR](f: (K, V, VR) => VR) {

    /** Wraps an aggregate function by the given error handler. */
    @inline def asAggregator(topic: KTopic[K, V], handler: ErrorHandler): Aggregator[K, V, VR] =
      (key, value, aggregate) =>
        Try(f(key, value, aggregate))
          .recover(handler.handle(topic, AggregateOperation, key, value, aggregate))
          .get
  }

  /** Returns an anonymous topic for the given materializing options. */
  @inline private[streams] def toTopic[K, V](
      options: Materialized[K, V, _ <: StateStore]): KTopic[K, V] = {
    val materialized = new InternalMaterialized(options)
    KTopic(materialized.keySerde, materialized.valueSerde)
  }

  /** Returns an anonymous windowed topic for the given materializing options. */
  @inline private[streams] def toWindowedTopic[K, V](
      options: Materialized[K, V, _ <: StateStore]): KTopic[Windowed[K], V] = {
    val materialized = new InternalMaterialized(options)
    KTopic(WindowedKeySerde(materialized.keySerde), materialized.valueSerde)
  }

  /** Returns the materialization options. */
  @inline private[streams] def storeOptions[K, V](keySerde: KeySerde[K],
                                                  valueSerde: ValueSerde[V]) =
    Materialized.`with`[K, V, KeyValueStore[Bytes, Array[Byte]]](keySerde, valueSerde)
}
