package org.eu.fuzzy.kafka.streams.functions.ktable

/**
 * Represents a set of functions to group the records of changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 */
trait GroupFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.GroupFunctions[K, V, AggFunctions]
