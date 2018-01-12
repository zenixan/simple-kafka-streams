package org.eu.fuzzy.kafka.streams

import scala.collection.mutable.Buffer
import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.streams.StreamsBuilder
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class IterativeFunctionsSpec extends FlatSpec with Matchers {

  private val TestInputTopic = KTopic[Int, String]("test-input-topic")
  private val TestOutputTopic = KTopic[Int, String]("test-output-topic")

  "peek function" should "execute an action for each record in the stream" in {
    val buffer = Buffer[String]()
    val streamTopology = (topology: StreamsBuilder) =>
      KStream(topology, TestInputTopic)
        .peek((_, value) => buffer.append(value.toString))
        .to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, null))
      .input(TestInputTopic, (3, "Record 3"))
      .output(TestOutputTopic)
    records shouldEqual Seq((1, "Record 1"), (2, null), (3, "Record 3"))
    buffer shouldEqual Seq("Record 1", "Record 3")
  }

  "foreach function" should behave like {
    it should "execute an action for each record in the stream" in {
      val buffer = Buffer[String]()
      val streamTopology = (topology: StreamsBuilder) =>
        KStream(topology, TestInputTopic).foreach((_, value) => buffer.append(value.toString))
      checkTopology(streamTopology, buffer)
    }

    it should "execute an action for each record in the table" in {
      val buffer = Buffer[String]()
      val streamTopology = (topology: StreamsBuilder) =>
        KTable(topology, TestInputTopic).foreach((_, value) => buffer.append(value.toString))
      checkTopology(streamTopology, buffer)
    }
  }

  private def checkTopology(topology: StreamsBuilder => Unit, buffer: Buffer[String]): Unit = {
    KMockBuilder(topology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, null))
      .input(TestInputTopic, (3, "Record 3"))
    buffer shouldEqual Seq("Record 1", "Record 3")
  }
}
