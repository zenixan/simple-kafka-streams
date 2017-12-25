package org.eu.fuzzy.kafka.streams.functions

/**
 * Represents a set of functions to iterate through a record/changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait IterativeFunctions[K, V] {
  /**
   * Performs an action for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @param action  an action to perform on each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#foreach]]
   */
  def foreach(action: (K, V) => Unit): Unit
}
