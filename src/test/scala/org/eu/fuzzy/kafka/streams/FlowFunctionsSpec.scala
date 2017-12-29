package org.eu.fuzzy.kafka.streams

import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.streams.StreamsBuilder
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class FlowFunctionsSpec extends FlatSpec with Matchers {

  private val TestInputTopic1 = KTopic[Int, String]("test-input-topic1")
  private val TestInputTopic2 = KTopic[Int, String]("test-input-topic2")
  private val TestOutputTopic1 = KTopic[Int, String]("test-output-topic1")
  private val TestOutputTopic2 = KTopic[Int, String]("test-output-topic2")
  private val TestOutputTopics = Seq(TestOutputTopic1, TestOutputTopic2)

  "branch function" should "split the stream into two streams" in {
    val streamTopology = (topology: StreamsBuilder) =>
      KStream(topology, TestInputTopic1)
        .branch((key, _) => key == 1, (key, value) => value.nonEmpty && key > 1)
        .zipWithIndex
        .foreach(entry => entry._1.to(TestOutputTopics(entry._2).name))
    val mockStream = KMockBuilder(streamTopology)
      .input(TestInputTopic1, (1, "Record 1"))
      .input(TestInputTopic1, (2, "Record 2"))
      .input(TestInputTopic1, (3, null))
    mockStream.output(TestOutputTopic1) shouldEqual Seq((1, "Record 1"))
    mockStream.output(TestOutputTopic2) shouldEqual Seq((2, "Record 2"))
  }

  "split function" should behave like {
    it should "split the stream into two streams" in {
      val streamTopology = (topology: StreamsBuilder) => {
        val (stream1, stream2) = KStream(topology, TestInputTopic1).split((key, _) => key == 1)
        stream1.to(TestOutputTopic1.name)
        stream2.to(TestOutputTopic2.name)
      }
      val mockStream = KMockBuilder(streamTopology)
        .input(TestInputTopic1, (1, "Record 1"))
        .input(TestInputTopic1, (2, "Record 2"))
        .input(TestInputTopic1, (3, "Record 3"))
      mockStream.output(TestOutputTopic1) shouldEqual Seq((1, "Record 1"))
      mockStream.output(TestOutputTopic2) shouldEqual Seq((2, "Record 2"), (3, "Record 3"))
    }

    it should "remove the record from the sub-stream if the any error has occurred" in {
      val streamTopology = (topology: StreamsBuilder) => {
        val (stream1, stream2) =
          KStream(topology, TestInputTopic1).split((key, value) => value.nonEmpty && key == 2)
        stream1.to(TestOutputTopic1.name)
        stream2.to(TestOutputTopic2.name)
      }
      val mockStream = KMockBuilder(streamTopology)
        .input(TestInputTopic1, (1, "Record 1"))
        .input(TestInputTopic1, (2, "Record 2"))
        .input(TestInputTopic1, (3, null))
      mockStream.output(TestOutputTopic1) shouldEqual Seq((2, "Record 2"))
      mockStream.output(TestOutputTopic2) shouldEqual Seq((1, "Record 1"))
    }
  }

  "merge function" should "merges two streams" in {
    val streamTopology = (topology: StreamsBuilder) => {
      KStream(topology, TestInputTopic1)
        .merge(KStream(topology, TestInputTopic2))
        .through(TestOutputTopic1.name)
      ()
    }
    val mockStream = KMockBuilder(streamTopology)
      .input(TestInputTopic1, (1, "Record 1"))
      .input(TestInputTopic2, (2, "Record 2"))
    mockStream.output(TestOutputTopic1) shouldEqual Seq((1, "Record 1"), (2, "Record 2"))
  }
}
