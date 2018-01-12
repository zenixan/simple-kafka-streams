package org.eu.fuzzy.kafka.streams.functions

import scala.language.higherKinds

/**
 * Represents a set of functions to merge a record/changelog stream with another stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam S  a type of joined stream by a primary key, i.e.
 *            [[org.eu.fuzzy.kafka.streams.functions.kstream.JoinFunctions]] or
 *            [[org.eu.fuzzy.kafka.streams.functions.ktable.JoinFunctions]]
 * @tparam T  a type of joined stream by a foreign key, i.e.
 *            [[org.eu.fuzzy.kafka.streams.functions.kstream.TableJoinFunctions]] or
 *            [[org.eu.fuzzy.kafka.streams.functions.ktable.BasicJoinFunctions]]
 */
trait JoinFunctions[K, V, S[_, _, _], T[_, _, _]] {

  /**
   * Combines a records of this stream with records from the given stream using a record key.
   */
  def combineByKey: S[K, V, K]

  /**
   * Combines a records of this stream with records from the given stream using a function.
   *
   * @note The current implementation isn't efficient for the tables.
   *       For more details please see the ticket [[https://issues.apache.org/jira/browse/KAFKA-3705 KAFKA-3705]].
   *
   * @tparam JK  a type of record key in the stream to be joined
   *
   * @param mapper  a function to map the record of this stream to the key of stream to be joined
   */
  def combineBy[JK](mapper: (K, V) => JK): T[K, V, JK]
}
