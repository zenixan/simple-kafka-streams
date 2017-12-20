package org.eu.fuzzy.kafka.streams.error

/**
 * Represents an identity of operation that's protected by a stream error handler.
 *
 * @param name  a name of operation
 */
sealed abstract class CheckedOperation(val name: String)

object CheckedOperation {
  case object DeserializeOperation extends CheckedOperation("deserialize")

  case object FilterOperation extends CheckedOperation("filter")
  case object FlatMapOperation extends CheckedOperation("flatMap")
  case object FlatMapValuesOperation extends CheckedOperation("flatMapValues")
  case object MapValuesOperation extends CheckedOperation("mapValues")
  case object BranchOperation extends CheckedOperation("branch")
  case object ForeachOperation extends CheckedOperation("foreach")
  case object PeekOperation extends CheckedOperation("peek")

  case object InitializerOperation extends CheckedOperation("aggregateByKey")
  case object AggregateOperation extends CheckedOperation("aggregateByKey")
  case object ReduceOperation extends CheckedOperation("reduceByKey")

  case object LeftJoinOperation extends CheckedOperation("leftJoin")
  case object OuterJoinOperation extends CheckedOperation("outerJoin")
  case object InnerJoinOperation extends CheckedOperation("join")
  case object JoinByKeyOperation extends CheckedOperation("join")
}
