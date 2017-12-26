package org.eu.fuzzy.kafka.streams.serialization

import org.apache.kafka.common.serialization.Serializer

/**
 * Provides a set of builtin serializers.
 */
object Serializers {
  val IntSerializer: Serializer[Int] = new Serializer[Int] with Configurable {
    override def serialize(topic: String, data: Int): Array[Byte] = intToBytes(data)
  }

  val LongSerializer: Serializer[Long] = new Serializer[Long] with Configurable {
    override def serialize(topic: String, data: Long): Array[Byte] = longToBytes(data)
  }

  val FloatSerializer: Serializer[Float] = new Serializer[Float] with Configurable {
    override def serialize(topic: String, data: Float): Array[Byte] =
      intToBytes(java.lang.Float.floatToRawIntBits(data))
  }

  val DoubleSerializer: Serializer[Double] = new Serializer[Double] with Configurable {
    override def serialize(topic: String, data: Double): Array[Byte] =
      longToBytes(java.lang.Double.doubleToLongBits(data))
  }

  @inline private def intToBytes(number: Int): Array[Byte] =
    Array((number >>> 24).toByte, (number >>> 16).toByte, (number >>> 8).toByte, number.toByte)

  @inline private def longToBytes(number: Long): Array[Byte] =
    Array(
      (number >>> 56).toByte,
      (number >>> 48).toByte,
      (number >>> 40).toByte,
      (number >>> 32).toByte,
      (number >>> 24).toByte,
      (number >>> 16).toByte,
      (number >>> 8).toByte,
      number.toByte
    )
}
