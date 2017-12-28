package org.eu.fuzzy.kafka.streams.serialization

import org.apache.kafka.common.serialization.{Serde, Serializer, Deserializer}
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.{WindowedSerializer, WindowedDeserializer}

/**
 * Provides a windowed serializer and deserializer of the given key type.
 */
object WindowedKeySerde {

  /**
   * Creates a wrapper for a windowed serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param serde  a source wrapper to copy
   */
  def apply[T](serde: Serde[T]): KeySerde[Windowed[T]] = apply(serde.serializer, serde.deserializer)

  /**
   * Creates a wrapper for a windowed serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param serializer  an interface for converting objects to bytes
   * @param deserializer  an interface for converting bytes to objects
   */
  def apply[T](serializer: Serializer[T], deserializer: Deserializer[T]): KeySerde[Windowed[T]] = {
    val windowedSerializer = new WindowedSerializer(serializer)
    val windowedDeserializer = new WindowedDeserializer(deserializer)
    new KeySerde[Windowed[T]]() {
      override def deserializer(): Deserializer[Windowed[T]] = windowedDeserializer
      override def serializer(): Serializer[Windowed[T]] = windowedSerializer
    }
  }

}
