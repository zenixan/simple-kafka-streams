package org.eu.fuzzy.kafka.streams

import scala.language.higherKinds
import org.apache.kafka.streams.kstream.Produced
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde

/**
 * Represents a common set of functions for the record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam S  a type of stream, i.e. [[KStream]] or [[KTable]]
 */
trait StreamFunctions[K, V, S[K, V]] {
  /**
   * Returns a Kafka topic for this stream.
   *
   * @note The name of topic is absent for the streams which are created by any intermediate operations,
   *       e.g. [[filter()]], [[mapValues()]], etc.
   */
  def topic: KTopic[K, V]

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
  def filterNot(predicate: (K, V) => Boolean): S[K, V] = filter((key, value) => !predicate(key, value))

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

  /**
   * Returns a new stream with a new value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new value for each record
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#mapValues]]
   * @see [[org.apache.kafka.streams.kstream.KTable#mapValues]]
   */
  def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): S[K, VR]

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

  /**
   * Materializes this stream to a topic and creates a new stream from the topic.
   *
   * @param topic  a name of topic to write
   */
  def through(topic: String): S[K, V] = through(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))

  /**
   * Materializes this stream to a topic and creates a new stream from the topic.
   *
   * @param topic  a name of topic to write
   * @param options  a set of options to use when producing to the topic
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#through]]
   */
  def through(topic: String, options: Produced[K, V]): S[K, V]
}
