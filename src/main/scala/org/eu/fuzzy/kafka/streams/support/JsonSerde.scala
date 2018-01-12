package org.eu.fuzzy.kafka.streams.support

import scala.reflect.runtime.universe._
import scala.util.Try
import java.lang.reflect.{Type => JavaType, ParameterizedType}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.eu.fuzzy.kafka.streams.serialization.{Configurable, Serde}

/** Represents a factory of JSON serializers and deserializers. */
object JsonSerde {

  /**
   * Returns a serializer and deserializer for the JSON format.
   *
   * @tparam T  a target type for serialization/deserialization
   *
   * @param tag  a reference to the target type
   * @param mapper  an optional JSON mapper
   */
  def apply[T](implicit tag: TypeTag[T], mapper: ObjectMapper = null): Serde[T] = {
    val typeRef = new TypeReference[T] {
      override def getType: JavaType = toJavaType(tag.tpe)
    }
    val objectMapper = getObjectMapper(mapper)
    val objectSerde = new Serializer[T] with Deserializer[T] with Configurable {
      override def serialize(topic: String, data: T): Array[Byte] =
        objectMapper.writeValueAsBytes(data)
      override def deserialize(topic: String, data: Array[Byte]): T =
        if (data == null) null.asInstanceOf[T] else objectMapper.readValue(data, typeRef)
    }
    Serde(objectSerde, objectSerde)
  }

  /**
   * Returns a serializer and deserializer of [[scala.Tuple2 tuple]] in the JSON format.
   *
   * @param tag1  a type reference for the first element of tuple
   * @param tag2  a type reference for the second element of tuple
   */
  def apply[T1, T2](implicit tag1: TypeTag[T1], tag2: TypeTag[T2]): Serde[(T1, T2)] =
    apply[(T1, T2)]

  /** Returns a pre-configured object mapper. */
  private def getObjectMapper(defaultMapper: ObjectMapper): ObjectMapper = {
    if (!classExists("com.fasterxml.jackson.module.scala.DefaultScalaModule")) {
      throw new SerializationException(
        "Unable to find a jackson-module-scala artifact in the project")
    }
    if (defaultMapper == null) {
      new ObjectMapper()
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    } else {
      defaultMapper.copy().findAndRegisterModules()
    }
  }

  /** Checks whether the specified class is exist. */
  private def classExists(name: String): Boolean =
    Try(Class.forName(name)).map(_ => true).getOrElse(false)

  /** Returns a java type reference for the given type tag. */
  private def toJavaType(tpe: Type): ParameterizedType = tpe match {
    case TypeRef(_, sig, args) =>
      new ParameterizedType {
        def getRawType: JavaType =
          runtimeMirror(getClass.getClassLoader).runtimeClass(sig.asType.toType)
        def getActualTypeArguments: Array[JavaType] = args.map(toJavaType).toArray
        def getOwnerType: JavaType = null
      }
  }
}
