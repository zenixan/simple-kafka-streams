package org.eu.fuzzy.kafka.streams.serialization

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.{WindowedDeserializer, WindowedSerializer}

/**
 * Provides a windowed serializer and deserializer of the given type.
 */
object WindowedKeySerde {

  /**
   * Creates a wrapper for a windowed serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param serde  a source wrapper to copy
   */
  def apply[T](serde: Serde[T]): Serde[Windowed[T]] = {
    implicit val tag: TypeTag[T] = serde.typeRef
    Serde(typeTag[Windowed[T]],
          new WindowedSerializer(serde.serializer),
          new WindowedDeserializer(serde.deserializer))
  }

}
