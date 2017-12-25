package org.eu.fuzzy.kafka.streams.functions.kstream

import org.eu.fuzzy.kafka.streams.KStream

/**
 * Represents a set of functions to iterate through a record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait IterativeFunctions[K, V] extends org.eu.fuzzy.kafka.streams.functions.IterativeFunctions[K, V] {
  /**
   * Performs an action for each input record.
   *
   * This is a stateless record-by-record operation that triggers a side effect (such as logging or statistics collection)
   * and returns an unchanged stream.
   *
   * @param action  an action to perform on each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#peek]]
   */
  def peek(action: (K, V) => Unit): KStream[K, V]
}
