package org.eu.fuzzy.kafka.streams

import org.apache.kafka.streams.StreamsBuilder
import org.scalatest.{FlatSpec, Matchers}
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class KStreamSpec extends FlatSpec with Matchers {

  import KStreamSpec._

  "filter function" should "remove the record from the stream" in {
    val streamTopology = (topology: StreamsBuilder) => KStream(topology, TestInputTopic)
      .filter((key, _) => key > 1).to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .output(TestOutputTopic)
    records shouldEqual Seq((2, "Record 2"))
  }

  "filter function" should "skip record if the error has occurred" in {
    val streamTopology = (topology: StreamsBuilder) => KStream(topology, TestInputTopic)
      .filter((_, _) => throw new NullPointerException).to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .output(TestOutputTopic)
    records shouldEqual Seq()
  }

  "filterNot function" should "remove the record from the stream" in {
    val streamTopology = (topology: StreamsBuilder) => KStream(topology, TestInputTopic)
      .filterNot((key, _) => key == 1).to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .output(TestOutputTopic)
    records shouldEqual Seq((2, "Record 2"))
  }

  "filterKeys function" should "remove the record from the stream" in {
    val streamTopology = (topology: StreamsBuilder) => KStream(topology, TestInputTopic)
      .filterKeys(_ > 1).to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .output(TestOutputTopic)
    records shouldEqual Seq((2, "Record 2"))
  }

  "filterValues function" should "remove the record from the stream" in {
    val streamTopology = (topology: StreamsBuilder) => KStream(topology, TestInputTopic)
      .filterValues(_ == "Record 2").to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .input(TestInputTopic, (3, "Record 3"))
      .output(TestOutputTopic)
    records shouldEqual Seq((2, "Record 2"))
  }

  "branch function" should "split the stream into two streams" in {
    val outputTopics = Seq(
      KTopic[Int, String]("test-output-topic1"), KTopic[Int, String]("test-output-topic2")
    )
    val streamTopology = (topology: StreamsBuilder) => KStream(topology, TestInputTopic)
      .branch((key, _) => key == 1, (key, _) => key == 2)
      .zipWithIndex.foreach(entry => entry._1.to(outputTopics(entry._2).name))
    val mockStream = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .input(TestInputTopic, (3, "Record 3"))
    mockStream.output(outputTopics(0)) shouldEqual Seq((1, "Record 1"))
    mockStream.output(outputTopics(1)) shouldEqual Seq((2, "Record 2"))
  }

  "split function" should "split the stream into two streams" in {
    val outputTopics = Seq(
      KTopic[Int, String]("test-output-topic1"), KTopic[Int, String]("test-output-topic2")
    )
    val streamTopology = (topology: StreamsBuilder) => {
      val pair = KStream(topology, TestInputTopic).split((key, _) => key == 1)
      pair._1.to(outputTopics(0).name)
      pair._2.to(outputTopics(1).name)
    }
    val mockStream = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .input(TestInputTopic, (3, "Record 3"))
    mockStream.output(outputTopics(0)) shouldEqual Seq((1, "Record 1"))
    mockStream.output(outputTopics(1)) shouldEqual Seq((2, "Record 2"), (3, "Record 3"))
  }

}

object KStreamSpec {

  val TestInputTopic = KTopic[Int, String]("test-input-topic")
  val TestOutputTopic = KTopic[Int, String]("test-output-topic")

}