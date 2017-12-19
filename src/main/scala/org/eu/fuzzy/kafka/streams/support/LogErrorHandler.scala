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

  override def handle[R : ClassTag](topic: KTopic[_, _], operation: CheckedOperation)
                                   (key: Any, value: Any): PartialFunction[Throwable, R] = {
    case deserializeError if (operation == DeserializeOperation) =>
      logger.error(s"Unable to deserialize a record value with a key $key", deserializeError)
      false.asInstanceOf[R]

    case filterError if (operation == FilterOperation) =>
      logger.error(s"The ${operation.name} function is failed for the record <$key:$value>", filterError)
      false.asInstanceOf[R]

    case mapError if (operation == FlatMapOperation) =>
      logger.error(s"The ${operation.name} function is failed for the record <$key:$value>", mapError)
      Collections.emptyList.asInstanceOf[R]

    case joinError if (operation == JoinByKeyOperation) =>
      logger.error(s"The join function is failed to calculate a join key for the record <$key:$value>", joinError)
      null.asInstanceOf[R]

    case error =>
      logger.error(s"The ${operation.name} function is failed for the record <$key:$value>", error)
      Unit.asInstanceOf[R]
  }

  override def handle[R : ClassTag]
                     (topic: KTopic[_, _], operation: CheckedOperation, value: Any): PartialFunction[Throwable, R] = {
    case mapError if (operation == FlatMapValuesOperation) =>
      logger.error(s"The ${operation.name} function is failed for the value $value", mapError)
      Collections.emptyList.asInstanceOf[R]

    case joinError if (operation == InnerJoinOperation) =>
      val pair = value.asInstanceOf[(Any, Any)]
      logger.error(s"The ${operation.name} function is failed for the pair (${pair._1}, ${pair._2})", joinError)
      null.asInstanceOf[R]
  }
}
