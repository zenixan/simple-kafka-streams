package org.eu.fuzzy.kafka.streams.functions.kstream

import org.apache.kafka.streams.kstream.Produced
import org.eu.fuzzy.kafka.streams.KStream

/**
 * Represents a set of functions to materialize a record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait MaterializeFunctions[K, V] extends org.eu.fuzzy.kafka.streams.functions.MaterializeFunctions[K, V] {
  /**
   * Materializes this stream to a topic and creates a new stream from the topic.
   *
   * @param topic  a name of topic to write
   */
  def through(topic: String): KStream[K, V]

  /**
   * Materializes this stream to a topic and creates a new stream from the topic.
   *
   * @param topic  a name of topic to write
   * @param options  a set of options to use when producing to the topic
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#through]]
   */
  def through(topic: String, options: Produced[K, V]): KStream[K, V]
}
