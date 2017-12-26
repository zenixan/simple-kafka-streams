package org.eu.fuzzy.kafka.streams.support

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import java.util.UUID

import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}
import org.apache.kafka.test.ProcessorTopologyTestDriver
import org.eu.fuzzy.kafka.streams.KTopic

/**
 * Provides an unit-test processing topologies of Kafka Streams applications.
 */
object KMockBuilder {

  /**
   * Initializes a test topology driver with default stream configuration.
   *
   * @param topology  a function to create a stream topology
   */
  def apply(topology: StreamsBuilder => Unit): Builder = apply(topology, Map())

  /**
   * Initializes a test topology driver.
   *
   * @param topology  a function to create a stream topology
   * @param config  a stream configuration for the topology
   */
  def apply(topology: StreamsBuilder => Unit, config: Map[String, _]): Builder = {
    val streamConfig = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> s"stream-mock-${UUID.randomUUID()}",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092") ++ config
    val builder = new StreamsBuilder()
    topology(builder)
    Builder(
      new ProcessorTopologyTestDriver(new StreamsConfig(streamConfig.asJava), builder.build()))
  }

  final case class Builder(driver: ProcessorTopologyTestDriver) {

    /**
     * Sends an input message to the specified topic.
     *
     * @tparam K  a type of record key
     * @tparam V  a type of record value
     *
     * @param topic  a destination topic
     * @param record  a message to send
     */
    @inline def input[K, V](topic: KTopic[K, V], record: (K, V)): Builder = {
      driver.process(topic.name,
                     record._1,
                     record._2,
                     topic.keySerde.serializer,
                     topic.valueSerde.serializer)
      this
    }

    /**
     * Sends a set of messages to the specified topic.
     *
     * @tparam K  a type of record key
     * @tparam V  a type of record value
     *
     * @param topic  a destination topic
     * @param records  a set of messages to send
     */
    def input[K, V](topic: KTopic[K, V], records: Seq[(K, V)]): Builder = {
      records.foreach(input(topic, _))
      this
    }

    /**
     * Reads a set of records from the specified topic.
     *
     * @tparam K  a type of record key
     * @tparam V  a type of record value
     *
     * @param topic  a source topic to read
     */
    def output[K, V](topic: KTopic[K, V]): Seq[(K, V)] = {
      @tailrec
      def read(records: Seq[(K, V)]): Seq[(K, V)] = {
        val record =
          driver.readOutput(topic.name, topic.keySerde.deserializer, topic.valueSerde.deserializer)
        if (record == null) records else read(records :+ (record.key(), record.value()))
      }
      read(Nil)
    }
  }
}
