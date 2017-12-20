package org.eu.fuzzy.kafka.streams.support

import scala.reflect.ClassTag
import java.util.Collections
import com.typesafe.scalalogging.Logger
import org.eu.fuzzy.kafka.streams.KTopic
import org.eu.fuzzy.kafka.streams.error.{CheckedOperation, ErrorHandler}

/**
 * A default error handler that logs any error and then signals the processing pipeline to continue processing more records.
 *
 * @param logger  a logger of stream errors
 */
class LogErrorHandler[T](logger: Logger) extends ErrorHandler {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  override def handle[R : ClassTag](
                                     topic: KTopic[_, _],
                                     operation: CheckedOperation,
                                     args: Any*): PartialFunction[Throwable, R] = {
    case deserializeError if (operation == DeserializeOperation) =>
      logger.error(s"Unable to deserialize a record value with a key ${args.head}", deserializeError)
      false.asInstanceOf[R]

    case filterError if (operation == FilterOperation) =>
      val Seq(key, value) = args
      logger.error(s"The filter function is failed for the record <$key:$value>", filterError)
      false.asInstanceOf[R]

    case mapError if (operation == FlatMapOperation) =>
      val Seq(key, value) = args
      logger.error(s"The flatMap function is failed for the record <$key:$value>", mapError)
      Collections.emptyList.asInstanceOf[R]

    case mapError if (operation == FlatMapValuesOperation) =>
      logger.error(s"The flatMapValues function is failed for the value ${args.head}", mapError)
      Collections.emptyList.asInstanceOf[R]

    case mapError if (operation == MapValuesOperation) =>
      logger.error(s"The mapValues function is failed for the value ${args.head}", mapError)
      null.asInstanceOf[R]

    case joinError if (operation == JoinByKeyOperation) =>
      val Seq(key, value) = args
      logger.error(s"The join function is failed to calculate a join key for the record <$key:$value>", joinError)
      null.asInstanceOf[R]

    case joinError if isJoinOperation(operation) =>
      val Seq(value1, value2) = args
      logger.error(s"The ${operation.name} function is failed for the pair ($value1, $value2)", joinError)
      null.asInstanceOf[R]

    case error if isTerminalOperation(operation) =>
      val Seq(key, value) = args
      logger.error(s"The ${operation.name} function is failed for the record <$key:$value>", error)
      Unit.asInstanceOf[R]

    case initializerError if (operation == InitializerOperation) =>
      logger.error(s"The aggregateByKey function is failed to calculate an initial value", initializerError)
      null.asInstanceOf[R]

    case aggregateError if (operation == AggregateOperation) =>
      val Seq(key, value, aggValue) = args
      logger.error(
        s"The aggregateByKey function is failed for the record <$key:$value> and the following aggregate value $aggValue",
        aggregateError
      )
      aggValue.asInstanceOf[R]

    case reduceError if (operation == ReduceOperation) =>
      val Seq(aggValue, newValue) = args
      logger.error(
        s"The reduceByKey function is failed for the new value $newValue and the following aggregate value $aggValue",
        reduceError
      )
      aggValue.asInstanceOf[R]
  }

  /** Checks whether the specified operation is related to the join operations. */
  @inline private def isJoinOperation(operation: CheckedOperation): Boolean =
    (operation == InnerJoinOperation) || (operation == LeftJoinOperation) || (operation == OuterJoinOperation)

  /** Checks whether the specified operation is related to terminal stream operation. */
  @inline private def isTerminalOperation(operation: CheckedOperation): Boolean =
    (operation == ForeachOperation) || (operation == PeekOperation)
}
