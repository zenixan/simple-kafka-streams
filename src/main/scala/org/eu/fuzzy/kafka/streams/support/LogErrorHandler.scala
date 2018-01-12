package org.eu.fuzzy.kafka.streams.support

import java.lang.Iterable
import java.util.Collections
import com.typesafe.scalalogging.Logger
import org.apache.kafka.streams.KeyValue
import org.eu.fuzzy.kafka.streams.{KTopic, ErrorHandler}
import org.eu.fuzzy.kafka.streams.internals.tombstone

/**
 * A default error handler that logs any error and then signals the processing pipeline
 * to continue processing more records.
 */
class LogErrorHandler(logger: Logger) extends ErrorHandler {
  override def onDeserializeError(error: Throwable,
                                  topic: KTopic[_, _],
                                  key: Any,
                                  value: Array[Byte]): Unit = {
    logger.error(s"Unable to deserialize a record value with a key $key", error)
  }

  override def onFilterError(error: Throwable,
                             topic: KTopic[_, _],
                             key: Any,
                             value: Any): Boolean = {
    logger.error(s"The filter function is failed for the record <$key:$value>", error)
    false
  }

  override def onMapKeysError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): R = {
    logger.error(
      s"The mapKeys function is failed to compute a new key for the record <$key:$value>",
      error)
    tombstone[R]
  }

  override def onMapValuesError[R](error: Throwable, topic: KTopic[_, _], value: Any): R = {
    logger.error(s"The mapValues function is failed to compute a new value for the value: $value",
                 error)
    tombstone[R]
  }

  override def onMapError[K, V](error: Throwable,
                                topic: KTopic[_, _],
                                key: Any,
                                value: Any): KeyValue[K, V] = {
    logger.error(s"The map function is failed to compute a new record for the record <$key:$value>",
                 error)
    tombstone[KeyValue[K, V]]
  }

  override def onFlatMapError[R](error: Throwable,
                                 topic: KTopic[_, _],
                                 key: Any,
                                 value: Any): Iterable[R] = {
    logger.error(s"The flatMap function is failed for the record <$key:$value>", error)
    Collections.emptyList[R]
  }

  override def onFlatMapValuesError[R](error: Throwable,
                                       topic: KTopic[_, _],
                                       value: Any): Iterable[R] = {
    logger.error(s"The flatMapValues function is failed for the value: $value", error)
    Collections.emptyList[R]
  }

  override def onForeachError(error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Unit = {
    logger.error(s"The foreach function is failed for the record <$key:$value>", error)
  }

  override def onPeekError(error: Throwable, topic: KTopic[_, _], key: Any, value: Any): Unit = {
    logger.error(s"The peek function is failed for the record <$key:$value>", error)
  }

  override def onBranchError(error: Throwable,
                             topic: KTopic[_, _],
                             key: Any,
                             value: Any): Boolean = {
    logger.error(s"The branch function is failed for the record <$key:$value>", error)
    false
  }

  override def onCombineError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): R = {
    logger.error(
      s"The join function is failed to calculate a join key for the record <$key:$value>",
      error)
    tombstone[R]
  }

  override def onInnerJoinError[R](error: Throwable,
                                   topic1: KTopic[_, _],
                                   topic2: KTopic[_, _],
                                   value1: Any,
                                   value2: Any): R = {
    logger.error(s"The innerJoin function is failed to merge these values ($value1, $value2)",
                 error)
    tombstone[R]
  }

  override def onLeftJoinError[R](error: Throwable,
                                  topic1: KTopic[_, _],
                                  topic2: KTopic[_, _],
                                  value1: Any,
                                  value2: Any): R = {
    logger.error(s"The leftJoin function is failed to merge these values ($value1, $value2)", error)
    tombstone[R]
  }

  override def onFullJoinError[R](error: Throwable,
                                  topic1: KTopic[_, _],
                                  topic2: KTopic[_, _],
                                  value1: Any,
                                  value2: Any): R = {
    logger.error(s"The fullJoin function is failed to merge these values ($value1, $value2)", error)
    tombstone[R]
  }

  override def onGroupError[R](error: Throwable, topic: KTopic[_, _], key: Any, value: Any): R = {
    logger.error(s"The group function is failed to compute a new key for the record <$key:$value>",
                 error)
    tombstone[R]
  }

  override def onInitializeError[R](error: Throwable, topic: KTopic[_, _]): R = {
    logger.error(s"The aggregate function is failed to calculate an initial value", error)
    tombstone[R]
  }

  override def onAggregateError[R](error: Throwable,
                                   topic: KTopic[_, _],
                                   key: Any,
                                   value: Any,
                                   aggregate: R): R = {
    logger.error(
      s"The aggregate function is failed to compute a new aggregated value for the record <$key:$value> with an old result $aggregate",
      error)
    aggregate
  }

  override def onMergeError[R](error: Throwable,
                               topic: KTopic[_, _],
                               key: Any,
                               aggregate1: R,
                               aggregate2: R): R = {
    logger.error(
      s"The aggregate function is failed to merge a aggregated value $aggregate1 with the aggregated value $aggregate2 for the key $key",
      error)
    if (aggregate1 == null) aggregate2 else aggregate1
  }

  override def onReduceError[R](error: Throwable,
                                topic: KTopic[_, _],
                                aggregate: R,
                                value: R): R = {
    logger.error(
      s"The reduce function is failed to compute a new aggregated value for the value $value with an old result $aggregate",
      error)
    aggregate
  }
}

/** Represents a factory of default error handlers. */
object LogErrorHandler {

  /**
   * Returns a handler with the given name of logger.
   *
   * @param name  a name of logger
   */
  def apply(name: String): LogErrorHandler = apply(Logger(name))

  /**
   * Returns a handler with the given logger.
   *
   * @param logger  a logger of stream errors
   */
  def apply(logger: Logger): LogErrorHandler = new LogErrorHandler(logger)
}
