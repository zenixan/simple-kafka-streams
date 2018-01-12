package org.eu.fuzzy.kafka.streams

import scala.reflect.runtime.universe.TypeTag
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.streams.kstream.Windowed
import org.eu.fuzzy.kafka.streams.support.JsonSerde

/**
 * Provides a set of default serializers/deserializers for keys and values.
 */
package object serialization {
  implicit val IntSerde: Serde[Int] =
    Serde(Serializers.IntSerializer, Deserializers.IntDeserializer)
  implicit val LongSerde: Serde[Long] =
    Serde(Serializers.LongSerializer, Deserializers.LongDeserializer)
  implicit val FloatSerde: Serde[Float] =
    Serde(Serializers.FloatSerializer, Deserializers.FloatDeserializer)
  implicit val DoubleSerde: Serde[Double] =
    Serde(Serializers.DoubleSerializer, Deserializers.DoubleDeserializer)
  implicit val StringSerde: Serde[String] =
    Serde(new StringSerializer, new StringDeserializer)
  implicit val BytesSerde: Serde[Array[Byte]] =
    Serde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit def WindowedSerde[T](implicit serde: Serde[T]): Serde[Windowed[T]] =
    WindowedKeySerde(serde)
  implicit def JsonValueSerde[T: TypeTag]: Serde[T] = JsonSerde[T]
}
