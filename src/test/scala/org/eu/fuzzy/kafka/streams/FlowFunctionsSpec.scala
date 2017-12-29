package org.eu.fuzzy.kafka.streams

import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.streams.StreamsBuilder
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class FlowFunctionsSpec extends FlatSpec with Matchers {

  private val TestInputTopic = KTopic[Int, String]("test-input-topic")

  "branch function" should "split the stream into two streams" in {
    val outputTopics = Seq(
      KTopic[Int, String]("test-output-topic1"),
      KTopic[Int, String]("test-output-topic2")
    )
    val streamTopology = (topology: StreamsBuilder) =>
      KStream(topology, TestInputTopic)
        .branch((key, _) => key == 1, (key, value) => value.nonEmpty && key > 1)
        .zipWithIndex
        .foreach(entry => entry._1.to(outputTopics(entry._2).name))
    val mockStream = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .input(TestInputTopic, (3, null))
    mockStream.output(outputTopics.head) shouldEqual Seq((1, "Record 1"))
    mockStream.output(outputTopics.last) shouldEqual Seq((2, "Record 2"))
  }

  "split function" should behave like {
    it should "split the stream into two streams" in {
      val outputTopics = Seq(
        KTopic[Int, String]("test-output-topic1"),
        KTopic[Int, String]("test-output-topic2")
      )
      val streamTopology = (topology: StreamsBuilder) => {
        val pair = KStream(topology, TestInputTopic).split((key, _) => key == 1)
        pair._1.to(outputTopics.head.name)
        pair._2.to(outputTopics.last.name)
      }
      val mockStream = KMockBuilder(streamTopology)
        .input(TestInputTopic, (1, "Record 1"))
        .input(TestInputTopic, (2, "Record 2"))
        .input(TestInputTopic, (3, "Record 3"))
      mockStream.output(outputTopics.head) shouldEqual Seq((1, "Record 1"))
      mockStream.output(outputTopics.last) shouldEqual Seq((2, "Record 2"), (3, "Record 3"))
    }

    it should "remove the record from the sub-stream if the any error has occurred" in {
      val outputTopics = Seq(
        KTopic[Int, String]("test-output-topic1"),
        KTopic[Int, String]("test-output-topic2")
      )
      val streamTopology = (topology: StreamsBuilder) => {
        val pair =
          KStream(topology, TestInputTopic).split((key, value) => value.nonEmpty && key == 2)
        pair._1.to(outputTopics.head.name)
        pair._2.to(outputTopics.last.name)
      }
      val mockStream = KMockBuilder(streamTopology)
        .input(TestInputTopic, (1, "Record 1"))
        .input(TestInputTopic, (2, "Record 2"))
        .input(TestInputTopic, (3, null))
      mockStream.output(outputTopics.head) shouldEqual Seq((2, "Record 2"))
      mockStream.output(outputTopics.last) shouldEqual Seq((1, "Record 1"))
    }
  }
}
