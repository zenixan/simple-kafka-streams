package org.eu.fuzzy.kafka.streams.functions

import scala.language.higherKinds

/**
 * Represents a set of functions to filter the record/changelog stream.
 *
 * @define d dd
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam S  a type of stream, i.e. [[org.eu.fuzzy.kafka.streams.KStream]] or [[org.eu.fuzzy.kafka.streams.KTable]]
 */
trait FilterFunctions[K, V, S[K, V]] {

  /**
   * Returns a new stream that consists of all records of this stream which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note If this stream represents a changelog stream then for each record that don't satisfy the predicate
   *       a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#filter]]
   * @see [[org.apache.kafka.streams.kstream.KTable#filter]]
   */
  def filter(predicate: (K, V) => Boolean): S[K, V]

  /**
   * Returns a new stream that consists of all records of this stream which don't satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note If this stream represents a changelog stream then for each record that does satisfy the predicate
   *       a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#filterNot]]
   * @see [[org.apache.kafka.streams.kstream.KTable#filterNot]]
   */
  def filterNot(predicate: (K, V) => Boolean): S[K, V] =
    filter((key, value) => !predicate(key, value))

  /**
   * Returns a new stream that consists of all records of this stream which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note If this stream represents a changelog stream then for each record that don't satisfy the predicate
   *       a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record key
   */
  def filterKeys(predicate: K => Boolean): S[K, V] = filter((key, _) => predicate(key))

  /**
   * Returns a new stream that consists of all records of this stream which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note If this stream represents a changelog stream then for each record that don't satisfy the predicate
   *       a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record value
   */
  def filterValues(predicate: V => Boolean): S[K, V] = filter((_, value) => predicate(value))
}
