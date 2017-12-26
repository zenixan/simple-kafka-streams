package org.eu.fuzzy.kafka.streams.serialization

import scala.annotation.implicitNotFound
import org.apache.kafka.common.serialization.{Deserializer, Serializer, Serde}

/**
 * A wrapper for a serializer and deserializer of the given key type.
 *
 * @tparam T  a type to be serialized/deserialized
 */
@implicitNotFound("Unable to find a serializer for the key type ${T}")
trait KeySerde[T] extends Serde[T] with Configurable

object KeySerde {

  /**
   * Creates a wrapper for a serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param serde  a source wrapper to copy
   */
  def apply[T](serde: Serde[T]): KeySerde[T] = KeySerde(serde.serializer, serde.deserializer)

  /**
   * Creates a wrapper for a serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param kafkaSerializer  an interface for converting objects to bytes
   * @param kafkaDeserializer  an interface for converting bytes to objects
   */
  def apply[T](kafkaSerializer: Serializer[T], kafkaDeserializer: Deserializer[T]): KeySerde[T] =
    new KeySerde[T]() {
      override def deserializer(): Deserializer[T] = kafkaDeserializer
      override def serializer(): Serializer[T] = kafkaSerializer
    }
}
