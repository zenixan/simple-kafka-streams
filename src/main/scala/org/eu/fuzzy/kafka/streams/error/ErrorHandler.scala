package org.eu.fuzzy.kafka.streams.error

import scala.reflect.ClassTag
import org.eu.fuzzy.kafka.streams.KTopic

/**
 * Represents a strategy for handling stream errors.
 */
trait ErrorHandler {
  /**
   * Returns an error handler for the invalid record.
   *
   * @tparam R  a type of fallback value
   *
   * @param topic  a stream that's received an invalid record
   * @param operation  a name of operation that's caused an error
   * @param args  a list of arguments for the operation that's caused an error
   */
  def handle[R : ClassTag](topic: KTopic[_, _], operation: CheckedOperation, args: Any*): PartialFunction[Throwable, R]
}
