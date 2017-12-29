package org.eu.fuzzy.kafka.streams

import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.streams.StreamsBuilder
import org.eu.fuzzy.kafka.streams.functions.{FilterFunctions, MaterializeFunctions}
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class FilterFunctionsSpec extends FlatSpec with Matchers {

  type StreamFilter = FilterFunctions[Int, String, KStream] => MaterializeFunctions[Int, String]
  type TableFilter = FilterFunctions[Int, String, KTable] => MaterializeFunctions[Int, String]

  private val TestInputTopic = KTopic[Int, String]("test-input-topic")
  private val TestOutputTopic = KTopic[Int, String]("test-output-topic")

  "filter function" should behave like filterableStream(
    _.filter((key, value) => value.nonEmpty && key > 1),
    _.filter((key, value) => value.nonEmpty && key > 1))

  "filterNot function" should behave like filterableStream(
    _.filterNot((key, value) => value.nonEmpty && key == 1),
    _.filterNot((key, value) => value.nonEmpty && key == 1))

  "filterKeys function" should behave like filterableStream(
    _.filterKeys(key => assertTrue(key == 2)),
    _.filterKeys(key => assertTrue(key == 2)))

  "filterValues function" should behave like filterableStream(
    _.filterValues(value => value.nonEmpty && value == "Record 2"),
    _.filterValues(value => value.nonEmpty && value == "Record 2"))

  private def filterableStream(streamOperation: StreamFilter, tableOperation: TableFilter): Unit = {
    it should "remove the record from the stream" in {
      val streamTopology = (topology: StreamsBuilder) =>
        streamOperation(KStream(topology, TestInputTopic)).to(TestOutputTopic.name)
      val records = KMockBuilder(streamTopology)
        .input(TestInputTopic, (1, "Record 1"))
        .input(TestInputTopic, (2, "Record 2"))
        .input(TestInputTopic, (3, null))
        .output(TestOutputTopic)
      records shouldEqual Seq((2, "Record 2"))
    }

    it should "remove the record from the table" in {
      val streamTopology = (topology: StreamsBuilder) =>
        tableOperation(KTable(topology, TestInputTopic)).to(TestOutputTopic.name)
      val records = KMockBuilder(streamTopology)
        .input(TestInputTopic, (1, "Record 1"))
        .input(TestInputTopic, (2, "Record 2"))
        .input(TestInputTopic, (3, null))
        .output(TestOutputTopic)
      records shouldEqual Seq((1, null), (2, "Record 2"), (3, null))
    }
  }

  private def assertTrue(assertion: Boolean): Boolean = {
    assert(assertion)
    true
  }
}
