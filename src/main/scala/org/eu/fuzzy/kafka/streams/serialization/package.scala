package org.eu.fuzzy.kafka.streams

import scala.reflect.ClassTag
import org.apache.kafka.common.serialization._
import org.eu.fuzzy.kafka.streams.support.JsonSerde

/**
 * Provides a set of default serializers/deserializers for keys and values.
 */
package object serialization {
  implicit val IntKeySerde = KeySerde(Serializers.IntSerializer, Deserializers.IntDeserializer)
  implicit val LongKeySerde = KeySerde(Serializers.LongSerializer, Deserializers.LongDeserializer)
  implicit val FloatKeySerde = KeySerde(Serializers.FloatSerializer, Deserializers.FloatDeserializer)
  implicit val DoubleKeySerde = KeySerde(Serializers.DoubleSerializer, Deserializers.DoubleDeserializer)
  implicit val StringKeySerde = KeySerde(new StringSerializer, new StringDeserializer)
  implicit val BytesKeySerde = KeySerde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit val IntValueSerde = ValueSerde(Serializers.IntSerializer, Deserializers.IntDeserializer)
  implicit val LongValueSerde = ValueSerde(Serializers.LongSerializer, Deserializers.LongDeserializer)
  implicit val FloatValueSerde = ValueSerde(Serializers.FloatSerializer, Deserializers.FloatDeserializer)
  implicit val DoubleValueSerde = ValueSerde(Serializers.DoubleSerializer, Deserializers.DoubleDeserializer)
  implicit val StringValueSerde = ValueSerde(new StringSerializer, new StringDeserializer)
  implicit val BytesValueSerde = ValueSerde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit def JsonValueSerde[T](implicit classTag: ClassTag[T]): ValueSerde[T] = ValueSerde(JsonSerde(classTag))
}
