package org.eu.fuzzy.kafka.streams.serialization

import scala.annotation.implicitNotFound
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, Serde => KafkaSerde}

/**
 * A wrapper for a serializer and deserializer of the given type.
 *
 * @tparam T  a type to be serialized/deserialized
 */
@implicitNotFound("Unable to find a serializer for the type ${T}")
trait Serde[T] extends KafkaSerde[T] with Configurable {

  /** Returns a reference to the type `T`. */
  def typeRef: TypeTag[T]
}

/** Represents a factory of serializers and deserializers. */
object Serde {

  /**
   * Creates a wrapper for a serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param serializer  an interface for converting objects to bytes
   * @param deserializer  an interface for converting bytes to objects
   */
  def apply[T: TypeTag](serializer: Serializer[T], deserializer: Deserializer[T]): Serde[T] =
    apply(typeTag[T], serializer, deserializer)

  /**
   * Creates a wrapper for a serializer and deserializer.
   *
   * @tparam T  a type to be serialized/deserialized
   *
   * @param tag   a reference to the type `T`
   * @param kafkaSerializer  an interface for converting objects to bytes
   * @param kafkaDeserializer  an interface for converting bytes to objects
   */
  def apply[T](tag: TypeTag[T],
               kafkaSerializer: Serializer[T],
               kafkaDeserializer: Deserializer[T]): Serde[T] =
    new Serde[T]() {
      override def typeRef: TypeTag[T] = tag
      override def deserializer(): Deserializer[T] = kafkaDeserializer
      override def serializer(): Serializer[T] = kafkaSerializer
    }
}
