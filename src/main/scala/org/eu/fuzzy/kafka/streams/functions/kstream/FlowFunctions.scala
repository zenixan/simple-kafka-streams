package org.eu.fuzzy.kafka.streams.functions.kstream

import org.eu.fuzzy.kafka.streams.KStream

/**
 * Represents a set of control flow functions for a record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait FlowFunctions[K, V] {
  /**
   * Returns a list of streams from this stream by branching the records in the original stream based on
   * the supplied predicates.
   *
   * Each stream in the result sequence corresponds position-wise (index) to the predicate
   * in the supplied predicates.
   * The branching happens on first-match: a record in the original stream is assigned
   * to the corresponding result stream for the first predicate that evaluates to true,
   * and is assigned to this stream only.
   * A record will be dropped if none of the predicates evaluate to true.
   *
   * This is a stateless record-by-record operation.
   *
   * @param predicates  an ordered list of functions to test an each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#branch]]
   */
  def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]]

  /**
   * Splits this stream in two streams according to a predicate.
   *
   * @param predicate  a function to test an each record
   *
   * @return a pair of streams: the stream that satisfies the predicate and the stream that does not.
   */
  def split(predicate: (K, V) => Boolean): (KStream[K, V], KStream[K, V])

  /**
   * Merges this stream and the given stream into one larger stream.
   *
   * @param stream  a stream to merge
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#merge]]
   */
  def merge(stream: KStream[K, V]): KStream[K, V]
}
