package org.eu.fuzzy.kafka.streams.support

import scala.reflect.ClassTag
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.eu.fuzzy.kafka.streams.serialization.Configurable

object JsonSerde {
  /**
   * Provides serialization and deserialization in the JSON format.
   *
   * @tparam T  a target type for serialization/deserialization
   *
   * @param classTag  a reference to the target type
   * @param mapper  an optional JSON mapper
   */
  def apply[T](classTag: ClassTag[T])(implicit mapper: ObjectMapper = null): Serde[T] = {
    val objectMapper = getObjectMapper(mapper)
    val objectSerde = new Serializer[T] with Deserializer[T] with Configurable {
        override def serialize(topic: String, data: T): Array[Byte] = objectMapper.writeValueAsBytes(data)
        override def deserialize(topic: String, data: Array[Byte]): T =
          objectMapper.readValue(data, classTag.runtimeClass.asInstanceOf[Class[T]])
      }
    new Serde[T] with Configurable {
      override def deserializer(): Deserializer[T] = objectSerde
      override def serializer(): Serializer[T] = objectSerde
    }
  }

  /**
   * Returns a pre-configured object mapper.
   */
  private def getObjectMapper(implicit defaultMapper: ObjectMapper = null): ObjectMapper = {
    try {
      Class.forName("com.fasterxml.jackson.module.scala.DefaultScalaModule")
    }
    catch { case _: ClassNotFoundException =>
      throw new SerializationException("Unable to find a jackson-module-scala artifact in the project")
    }
    if (defaultMapper == null) {
      new ObjectMapper()
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }
    else {
      defaultMapper.copy().findAndRegisterModules()
    }
  }
}
