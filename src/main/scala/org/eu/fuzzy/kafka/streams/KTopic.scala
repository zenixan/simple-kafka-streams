package org.eu.fuzzy.kafka.streams

import java.util.Objects.requireNonNull
import serialization.{KeySerde, ValueSerde}

/**
 * Represents an identity of Kafka topic.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
sealed trait KTopic[K, V] {

  /** Returns a name of topic. */
  def name: String

  /**
   * Checks whether this identity has a name.
   * @return `true` if the topic doesn't have a public name.
   */
  def isAnonymous: Boolean

  /** Returns a serialization format for the record key. */
  def keySerde: KeySerde[K]

  /** Returns a serialization format for the record value. */
  def valueSerde: ValueSerde[V]
}

object KTopic {

  /**
   * Creates an identity of Kafka topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param name  a name of topic
   * @param keySerde  a serialization format for the record key
   * @param valueSerde  a serialization format for the record value
   */
  def apply[K, V](name: String)(implicit keySerde: KeySerde[K],
                                valueSerde: ValueSerde[V]): KNamedTopic[K, V] = {
    require(name.nonEmpty, "name cannot be empty")
    requireNonNull(keySerde, "A serialization format for the record key cannot be null")
    requireNonNull(valueSerde, "A serialization format for the record value cannot be null")
    KNamedTopic(name, keySerde, valueSerde)
  }

  /**
   * Creates an anonymous identity of Kafka topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param keySerde  a serialization format for the record key
   * @param valueSerde  a serialization format for the record value
   */
  def apply[K, V](implicit
                  keySerde: KeySerde[K],
                  valueSerde: ValueSerde[V]): KAnonymousTopic[K, V] = {
    requireNonNull(keySerde, "A serialization format for the record key cannot be null")
    requireNonNull(valueSerde, "A serialization format for the record value cannot be null")
    KAnonymousTopic(keySerde, valueSerde)
  }
}

/**
 * Represents an identity of Kafka topic without a public name.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param keySerde  a serialization format for the record key
 * @param valueSerde  a serialization format for the record value
 */
final case class KAnonymousTopic[K, V] private (keySerde: KeySerde[K], valueSerde: ValueSerde[V])
    extends KTopic[K, V] {
  override def name: String =
    throw new UnsupportedOperationException("Anonymous topic doesn't have a public name")
  override def isAnonymous: Boolean = true
}

/**
 * Represents an identity of Kafka topic with a public name.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param name  a name of topic
 * @param keySerde  a serialization format for the record key
 * @param valueSerde  a serialization format for the record value
 */
final case class KNamedTopic[K, V] private (name: String,
                                            keySerde: KeySerde[K],
                                            valueSerde: ValueSerde[V])
    extends KTopic[K, V] {
  override def isAnonymous: Boolean = false
}
