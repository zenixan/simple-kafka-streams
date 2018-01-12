package org.eu.fuzzy.kafka.streams

import java.lang.Iterable
import org.apache.kafka.streams.KeyValue

/**
 * Represents a strategy for handling stream errors.
 */
trait ErrorHandler {

  /**
   * Handles a deserialization error of record value.
   *
   * @param error  a cause of deserialization error
   * @param topic  an identity of stream that's received an invalid record
   * @param key  a key of invalid record
   * @param value  a value of invalid record
   */
  def onDeserializeError(error: Throwable, topic: KTopic[_, _], key: Any, value: Array[Byte]): Unit

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.FilterFunctions.filter(* filter]] operation.
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback value for this operation
   */
  def onFilterError(error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Boolean

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.mapKeys[KR]* mapKeys]] operation.
   *
   * @tparam R  a type of fallback key
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback key for this operation
   */
  def onMapKeysError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): R

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.mapValues[VR]* mapValues]] operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback value for this operation
   */
  def onMapValuesError[R](error: Throwable, topic: KTopic[_, _], value: Any): R

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.map[KR,VR]* map]] operation.
   *
   * @tparam K  a type of fallback key
   * @tparam V  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback record for this operation
   */
  def onMapError[K, V](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): KeyValue[K, V]

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.kstream.TransformFunctions.flatMap[KR,VR]* flatMap]] operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback value for this operation
   */
  def onFlatMapError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Iterable[R]

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.kstream.TransformFunctions.flatMapValues[VR]* flatMapValues]] operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback value for this operation
   */
  def onFlatMapValuesError[R](error: Throwable, topic: KTopic[_, _], value: Any): Iterable[R]

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.IterativeFunctions.foreach(* foreach]] operation.
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   */
  def onForeachError(error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Unit

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.kstream.IterativeFunctions.peek(* peek]] operation.
   *
   * @param topic  an identity of stream to be processed
   * @param error  a cause of error
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   */
  def onPeekError(error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Unit

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.kstream.FlowFunctions.branch(* filter]] operation.
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback value for this operation
   */
  def onBranchError(error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Boolean

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.JoinFunctions.combineBy[JK]* join]] operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @return a fallback value for this operation
   */
  def onCombineError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): R

  /**
   * Handles an error caused by an inner join operation.
   *
   * @tparam R  a type of fallback value
   * @param error  a cause of error
   * @param topic1  an identity of stream that contains a first value
   * @param topic2  an identity of stream that contains a second value
   * @param value1  the first value for joining
   * @param value2  the second value for joining
   *
   * @return a fallback value for this operation
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.JoinFunctions.innerJoin()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.BasicJoinFunctions.innerJoin()]]
   */
  def onInnerJoinError[R](error: Throwable,
                          topic1: KTopic[_, _],
                          topic2: KTopic[_, _],
                          value1: Any,
                          value2: Any): R

  /**
   * Handles an error caused by a left join operation.
   *
   * @tparam R  a type of fallback value
   * @param error  a cause of error
   * @param topic1  an identity of stream that contains a first value
   * @param topic2  an identity of stream that contains a second value
   * @param value1  the first value for joining
   * @param value2  the second value for joining
   *
   * @return a fallback value for this operation
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.JoinFunctions.lefJoin()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.BasicJoinFunctions.leftJoin()]]
   */
  def onLeftJoinError[R](error: Throwable,
                         topic1: KTopic[_, _],
                         topic2: KTopic[_, _],
                         value1: Any,
                         value2: Any): R

  /**
   * Handles an error caused by an outer join operation.
   *
   * @tparam R  a type of fallback value
   * @param error  a cause of error
   * @param topic1  an identity of stream that contains a first value
   * @param topic2  an identity of stream that contains a second value
   * @param value1  the first value for joining
   * @param value2  the second value for joining
   *
   * @return a fallback value for this operation
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.JoinFunctions.fullJoin()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.JoinFunctions.fullJoin()]]
   */
  def onFullJoinError[R](error: Throwable,
                         topic1: KTopic[_, _],
                         topic2: KTopic[_, _],
                         value1: Any,
                         value2: Any): R

  /**
   * Handles an error caused by a
   * [[org.eu.fuzzy.kafka.streams.functions.GroupFunctions.groupBy[KR]* group]] operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   *
   * @see
   */
  def onGroupError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): R

  /**
   * Handles an error caused by a supplier of initial aggregate value.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   *
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions.aggregate()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions.aggregate()]]
   */
  def onInitializeError[R](error: Throwable, topic: KTopic[_, _]): R

  /**
   * Handles an error caused by an aggregate operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param value  a value of record to be processed
   * @param aggregate  an aggregated value to be processed
   *
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions.aggregate()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions.aggregate()]]
   */
  def onAggregateError[R](error: Throwable,
                          topic: KTopic[_, _],
                          key: Any,
                          value: Any,
                          aggregate: R): R

  /**
   * Handles an error caused by a merge function of two aggregated values.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param key  a key of record to be processed
   * @param aggregate1  the first aggregated value for merging
   * @param aggregate2  the second aggregated value for merging
   *
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions.aggregate()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions.aggregate()]]
   */
  def onMergeError[R](error: Throwable,
                      topic: KTopic[_, _],
                      key: Any,
                      aggregate1: R,
                      aggregate2: R): R

  /**
   * Handles an error caused by a reduce operation.
   *
   * @tparam R  a type of fallback value
   *
   * @param error  a cause of error
   * @param topic  an identity of stream to be processed
   * @param aggregate  an aggregated value to be processed
   * @param value  a value of record to be processed
   *
   * @see [[org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions.reduce()]]
   * @see [[org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions.reduce()]]
   */
  def onReduceError[R](error: Throwable, topic: KTopic[_, _], aggregate: R, value: R): R
}
