package org.eu.fuzzy.kafka.streams.serialization

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

/**
 * Provides a set of builtin deserializers.
 */
object Deserializers {
  val IntDeserializer: Deserializer[Int] = new Deserializer[Int] with Configurable {
    override def deserialize(topic: String, data: Array[Byte]): Int = bytesToInt(data)
  }

  val LongDeserializer: Deserializer[Long] = new Deserializer[Long] with Configurable {
    override def deserialize(topic: String, data: Array[Byte]): Long = bytesToLong(data)
  }

  val FloatDeserializer: Deserializer[Float] = new Deserializer[Float] with Configurable {
    override def deserialize(topic: String, data: Array[Byte]): Float =
      java.lang.Float.intBitsToFloat(bytesToInt(data))
  }

  val DoubleDeserializer: Deserializer[Double] = new Deserializer[Double] with Configurable {
    override def deserialize(topic: String, data: Array[Byte]): Double =
      java.lang.Double.longBitsToDouble(bytesToLong(data))
  }

  @inline private def bytesToInt(data: Array[Byte]): Int = data match {
    case null => 0
    case _ if (data.length == 4) =>
      var value = 0
      for (byte <- data) {
        value <<= 8
        value |= byte & 0xFF
      }
      value
    case _ =>
      throw new SerializationException(
        s"Integer contains 4 bytes but received ${data.length} bytes")
  }

  @inline private def bytesToLong(data: Array[Byte]): Long = data match {
    case null => 0
    case _ if (data.length == 4) =>
      var value = 0
      for (byte <- data) {
        value <<= 8
        value |= byte & 0xFF
      }
      value
    case _ =>
      throw new SerializationException(
        s"Integer contains 4 bytes but received ${data.length} bytes")
  }
}
