package org.eu.fuzzy.kafka.streams

import scala.reflect.ClassTag
import org.apache.kafka.common.serialization._
import org.eu.fuzzy.kafka.streams.support.JsonSerde

/**
 * Provides a set of default serializers/deserializers for keys and values.
 */
package object serialization {
  implicit val IntKeySerde: KeySerde[Int] =
    KeySerde(Serializers.IntSerializer, Deserializers.IntDeserializer)
  implicit val LongKeySerde: KeySerde[Long] =
    KeySerde(Serializers.LongSerializer, Deserializers.LongDeserializer)
  implicit val FloatKeySerde: KeySerde[Float] =
    KeySerde(Serializers.FloatSerializer, Deserializers.FloatDeserializer)
  implicit val DoubleKeySerde: KeySerde[Double] =
    KeySerde(Serializers.DoubleSerializer, Deserializers.DoubleDeserializer)
  implicit val StringKeySerde: KeySerde[String] =
    KeySerde(new StringSerializer, new StringDeserializer)
  implicit val BytesKeySerde: KeySerde[Array[Byte]] =
    KeySerde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit val IntValueSerde: ValueSerde[Int] =
    ValueSerde(Serializers.IntSerializer, Deserializers.IntDeserializer)
  implicit val LongValueSerde: ValueSerde[Long] =
    ValueSerde(Serializers.LongSerializer, Deserializers.LongDeserializer)
  implicit val FloatValueSerde: ValueSerde[Float] =
    ValueSerde(Serializers.FloatSerializer, Deserializers.FloatDeserializer)
  implicit val DoubleValueSerde: ValueSerde[Double] =
    ValueSerde(Serializers.DoubleSerializer, Deserializers.DoubleDeserializer)
  implicit val StringValueSerde: ValueSerde[String] =
    ValueSerde(new StringSerializer, new StringDeserializer)
  implicit val BytesValueSerde: ValueSerde[Array[Byte]] =
    ValueSerde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit def JsonValueSerde[T](implicit classTag: ClassTag[T]): ValueSerde[T] =
    ValueSerde(JsonSerde(classTag))
}
