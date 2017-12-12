package org.eu.fuzzy.kafka.streams

import scala.reflect.ClassTag
import org.apache.kafka.common.serialization._
import org.eu.fuzzy.kafka.streams.support.JsonSerde

/**
 * Provides a set of default serializers/deserializers.
 */
package object serialization {

  implicit val shortKeySerde = KeySerde(new ShortSerializer, new ShortDeserializer)
  implicit val intKeySerde = KeySerde(new IntegerSerializer, new IntegerDeserializer)
  implicit val longKeySerde = KeySerde(new LongSerializer, new LongDeserializer)
  implicit val floatKeySerde = KeySerde(new FloatSerializer, new FloatDeserializer)
  implicit val doubleKeySerde = KeySerde(new DoubleSerializer, new DoubleDeserializer)
  implicit val stringKeySerde = KeySerde(new StringSerializer, new StringDeserializer)
  implicit val bytesKeySerde = KeySerde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit val shortValueSerde = ValueSerde(new ShortSerializer, new ShortDeserializer)
  implicit val intValueSerde = ValueSerde(new IntegerSerializer, new IntegerDeserializer)
  implicit val longValueSerde = ValueSerde(new LongSerializer, new LongDeserializer)
  implicit val floatValueSerde = ValueSerde(new FloatSerializer, new FloatDeserializer)
  implicit val doubleValueSerde = ValueSerde(new DoubleSerializer, new DoubleDeserializer)
  implicit val stringValueSerde = ValueSerde(new StringSerializer, new StringDeserializer)
  implicit val bytesValueSerde = ValueSerde(new ByteArraySerializer, new ByteArrayDeserializer)

  implicit def jsonValueSerde[T](implicit classTag: ClassTag[T]): ValueSerde[T] = ValueSerde(JsonSerde(classTag))

}
