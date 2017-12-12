package org.eu.fuzzy.kafka.streams

import scala.reflect.ClassTag
import serialization.{KeySerde, ValueSerde}

/**
 * Represents an identity of Kafka topic.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param name  a name of topic
 * @param keySerde  a serialization format for the record key
 * @param valueSerde  a serialization format for the record value
 */
case class KTopic[K, V](name: String, keySerde: KeySerde[K], valueSerde: ValueSerde[V])

object KTopic {
  /**
   * Creates an identity of Kafka topic.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param name  a name of topic
   */
  def apply[K : ClassTag, V : ClassTag](name: String)(implicit keySerde: KeySerde[K], valueSerde: ValueSerde[V]): KTopic[K, V] =
    KTopic(name, keySerde, valueSerde)
}
