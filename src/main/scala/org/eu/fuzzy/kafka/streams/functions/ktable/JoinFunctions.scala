package org.eu.fuzzy.kafka.streams.functions.ktable

/**
 * Represents a set of functions to merge a changelog stream with another changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 * @tparam JK a type of primary key in the table to be joined
 */
trait JoinFunctions[K, V, JK] extends BasicJoinFunctions[K, V, JK] with ExtraJoinFunctions[K, V]
