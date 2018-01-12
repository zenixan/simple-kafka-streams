package org.eu.fuzzy.kafka.streams

import scala.util.Try
import scala.util.Random

import org.apache.kafka.streams.kstream.{Predicate, KeyValueMapper, ValueJoiner}
import org.apache.kafka.streams.kstream.{Initializer, Aggregator, Reducer, Windowed}

import org.eu.fuzzy.kafka.streams.serialization.WindowedKeySerde
import org.eu.fuzzy.kafka.streams.state.StoreOptions

package object internals {

  /** Wraps a predicate by the given error handler.  */
  private[streams] implicit class RichPredicate[K, V](f: (K, V) => Boolean) {
    @inline def asPredicate(topic: KTopic[K, V], handler: ErrorHandler): Predicate[K, V] =
      (key, value) =>
        Try(f(key, value)).recover { case error => handler.onFilterError(error, topic, key, value) }.get
  }

  /** Wraps a key-mapper function by the given error handler.  */
  private[streams] implicit class RichKeyMapper[K, V, KR](f: (K, V) => KR) {
    @inline def asKeyMapper(topic: KTopic[K, V], handler: ErrorHandler): KeyValueMapper[K, V, KR] =
      (key, value) =>
        Try(f(key, value)).recover { case error => handler.onGroupError(error, topic, key, value) }.get
  }

  /** Wraps an inner joiner function by the given error handler. */
  private[streams] implicit class RichInnerJoiner[V, VO, VR](f: (V, VO) => VR) {
    @inline def asInnerJoiner[K](topic1: KTopic[_, V],
                                 topic2: KTopic[_, VO],
                                 handler: ErrorHandler): ValueJoiner[V, VO, VR] =
      (value1, value2) =>
        Try(f(value1, value2)).recover {
          case error => handler.onInnerJoinError(error, topic1, topic2, value1, value2)
        }.get
  }

  /** Wraps a left joiner function by the given error handler. */
  private[streams] implicit class RichLeftJoiner[V, VO, VR](f: (V, Option[VO]) => VR) {
    @inline def asLeftJoiner[K](topic1: KTopic[_, V],
                                topic2: KTopic[_, VO],
                                handler: ErrorHandler): ValueJoiner[V, VO, VR] =
      (value1, value2) =>
        Try(f(value1, Some(value2))).recover {
          case error => handler.onLeftJoinError(error, topic1, topic2, value1, value2)
        }.get
  }

  /** Wraps a left joiner function by the given error handler. */
  private[streams] implicit class RichFullJoiner[V, VO, VR](f: (Option[V], Option[VO]) => VR) {
    @inline def asFullJoiner[K](topic1: KTopic[_, V],
                                topic2: KTopic[_, VO],
                                handler: ErrorHandler): ValueJoiner[V, VO, VR] =
      (value1, value2) =>
        Try(f(Some(value1), Some(value2))).recover {
          case error => handler.onFullJoinError(error, topic1, topic2, value1, value2)
        }.get
  }

  /** Wraps an aggregate supplier by the given error handler. */
  private[streams] implicit class RichInitializer[VR](f: () => VR) {
    @inline def asInitializer[K, V](topic: KTopic[K, V], handler: ErrorHandler): Initializer[VR] =
      () =>
        Try(f()).recover {
          case error => handler.onInitializeError(error, topic)
        }.get
  }

  /** Wraps an aggregate function by the given error handler. */
  private[streams] implicit class RichAggregator[K, V, VR](f: (K, V, VR) => VR) {
    @inline def asAggregator(topic: KTopic[K, V], handler: ErrorHandler): Aggregator[K, V, VR] =
      (key, value, aggregate) =>
        Try(f(key, value, aggregate)).recover {
          case error => handler.onAggregateError(error, topic, key, value, aggregate)
        }.get
  }

  /** Wraps a reducer function by the given error handler. */
  private[streams] implicit class RichReducer[V](f: (V, V) => V) {
    @inline def asReducer[K](topic: KTopic[K, V], handler: ErrorHandler): Reducer[V] =
      (aggregate, value) =>
        Try(f(aggregate, value)).recover {
          case error => handler.onReduceError(error, topic, aggregate, value)
        }.get
  }

  /** Returns a tombstone record according to the given type. */
  @inline private[streams] def tombstone[T]: T = null.asInstanceOf[T]

  /** Returns an anonymous windowed topic for the given materializing options. */
  @inline private[streams] def toWindowedTopic[K, V](
      options: StoreOptions[K, V, _]): KTopic[Windowed[K], V] =
    KTopic(WindowedKeySerde(options.keySerde), options.valSerde)

  /** Returns a random name for the state store. */
  @inline private[streams] def getStateStoreName: String =
    "STATE-STORE-" + Random.alphanumeric.take(10).mkString
}
