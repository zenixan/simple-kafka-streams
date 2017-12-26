package org.eu.fuzzy.kafka.streams.functions.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.Options

/**
 * Represents a set of functions to filter the changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 */
trait FilterFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.FilterFunctions[K, V, KTable] {

  /**
   * Returns a new table that consists of all records of this table which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note For each record that don't satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#filter]]
   */
  def filter(predicate: (K, V) => Boolean, options: Options[K, V]): KTable[K, V]

  /**
   * Returns a new table that consists of all records of this table which don't satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note For each record that does satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#filterNot]]
   */
  def filterNot(predicate: (K, V) => Boolean, options: Options[K, V]): KTable[K, V] =
    filter((key, value) => !predicate(key, value), options)

  /**
   * Returns a new table that consists of all records of this table which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note For each record that don't satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record key
   * @param options  a set of options to use when materializing to the local state store
   */
  def filterKeys(predicate: K => Boolean, options: Options[K, V]): KTable[K, V] =
    filter((key, _) => predicate(key), options)

  /**
   * Returns a new table that consists of all records of this table which satisfy the predicate.
   *
   * This is a stateless record-by-record operation.
   *
   * @note For each record that don't satisfy the predicate a tombstone record is forwarded.
   *
   * @param predicate  a function to test an each record value
   * @param options  a set of options to use when materializing to the local state store
   */
  def filterValues(predicate: V => Boolean, options: Options[K, V]): KTable[K, V] =
    filter((_, value) => predicate(value), options)
}
