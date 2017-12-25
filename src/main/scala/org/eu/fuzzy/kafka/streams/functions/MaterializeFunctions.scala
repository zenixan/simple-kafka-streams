package org.eu.fuzzy.kafka.streams.functions

import org.apache.kafka.streams.kstream.Produced
import org.eu.fuzzy.kafka.streams.KTopic

/**
 * Represents a set of functions to materialize a record/changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait MaterializeFunctions[K, V] {
  /**
   * Returns a Kafka topic for this stream.
   *
   * @note The name of topic is absent for the streams which are created by any intermediate operations, e.g.
   *       [[org.eu.fuzzy.kafka.streams.functions.FilterFunctions.filter()]],
   *       [[org.eu.fuzzy.kafka.streams.functions.TransformFunctions.map()]], etc.
   */
  def topic: KTopic[K, V]

  /**
   * Materializes this stream to a topic.
   *
   * @param topic  a name of topic to write
   */
  def to(topic: String): Unit = to(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))

  /**
   * Materializes this stream to a topic.
   *
   * @param topic  a name of topic to write
   * @param options  a set of options to use when producing to the topic
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#to]]
   */
  def to(topic: String, options: Produced[K, V]): Unit
}
