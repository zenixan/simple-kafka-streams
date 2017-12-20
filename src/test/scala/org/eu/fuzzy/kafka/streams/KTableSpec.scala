package org.eu.fuzzy.kafka.streams

import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.streams.StreamsBuilder
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class KTableSpec extends FlatSpec with Matchers {

  import KTableSpec._

  "filter function" should "remove the record from the table" in {
    val streamTopology = (topology: StreamsBuilder) => KTable(topology, TestInputTopic)
      .filter((key, _) => key > 1).to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .input(TestInputTopic, (2, "Record 2"))
      .output(TestOutputTopic)
    records shouldEqual Seq((1, null), (2, "Record 2"))
  }

  "filter function" should "remove the record from the table if the error has occurred" in {
    val streamTopology = (topology: StreamsBuilder) => KTable(topology, TestInputTopic)
      .filter((_, _) => throw new NullPointerException).to(TestOutputTopic.name)
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic, (1, "Record 1"))
      .output(TestOutputTopic)
    records shouldEqual Seq((1, null))
  }

}

object KTableSpec {

  val TestInputTopic = KTopic[Int, String]("test-input-topic")
  val TestOutputTopic = KTopic[Int, String]("test-output-topic")

}
