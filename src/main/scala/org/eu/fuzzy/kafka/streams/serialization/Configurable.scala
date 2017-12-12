package org.eu.fuzzy.kafka.streams.serialization

import java.io.Closeable
import java.util.Map

/**
 * Represents a dummy configuration for the serializer and deserializer.
 */
trait Configurable extends Closeable {
  /**
   * @see [[org.apache.kafka.common.serialization.Serializer#configure]]
   * @see [[org.apache.kafka.common.serialization.Deserializer#configure]]
   */
  def configure(configs: Map[String, _], isKey: Boolean): Unit = ()

  /**
   * @see [[org.apache.kafka.common.serialization.Serializer#close]]
   * @see [[org.apache.kafka.common.serialization.Deserializer#close]]
   */
  override def close(): Unit = ()
}
