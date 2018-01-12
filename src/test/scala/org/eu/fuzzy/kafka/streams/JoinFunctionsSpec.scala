package org.eu.fuzzy.kafka.streams

import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.streams.StreamsBuilder
import org.eu.fuzzy.kafka.streams.functions.MaterializeFunctions
import org.eu.fuzzy.kafka.streams.functions.ktable.{JoinFunctions, BasicJoinFunctions}
import org.eu.fuzzy.kafka.streams.support.KMockBuilder

class JoinFunctionsSpec extends FlatSpec with Matchers {

  type IntTableJoiner =
    (KTable[Int, String], JoinFunctions[Int, String, Int]) => MaterializeFunctions[Int, String]
  type StringTableJoiner =
    (KTable[String, String],
     BasicJoinFunctions[Int, String, String]) => MaterializeFunctions[Int, String]

  private val TestInputTopic1 = KTopic[Int, String]("test-input-topic1")
  private val TestInputTopic2 = KTopic[Int, String]("test-input-topic2")
  private val TestInputTopic3 = KTopic[String, String]("test-input-topic3")
  private val TestOutputTopic = KTopic[Int, String]("test-output-topic1")

  "innerJoin function" should behave like joinedStream(
    (otherTable, operation) => {
      operation.innerJoin(otherTable) { (value1, value2) =>
        if (value1 != "Record 3") value1 + " + " + value2 else throw new NullPointerException
      }
    },
    (otherTable, operation) => {
      operation.innerJoin(otherTable) { (value1, value2) =>
        if (value1 != "Record 3") value1 + " + " + value2 else throw new NullPointerException
      }
    },
    Seq((1, "Record 1 + Record 2"), (2, null))
  )

  "leftJoin function" should behave like joinedStream(
    (otherTable, operation) => {
      operation.leftJoin(otherTable) { (value1, value2) =>
        if (value1 != "Record 3") value1 + " + " + value2.orNull else throw new NullPointerException
      }
    },
    (otherTable, operation) => {
      operation.leftJoin(otherTable) { (value1, value2) =>
        if (value1 != "Record 3") value1 + " + " + value2.orNull else throw new NullPointerException
      }
    },
    Seq((1, "Record 1 + null"),
        (1, "Record 1 + Record 2"),
        (2, null),
        (2, null),
        (3, "Record 5 + null"))
  )

  "fullJoin function" should "join two tables by a primary key" in {
    val streamTopology = (topology: StreamsBuilder) => {
      val otherTable = KTable(topology, TestInputTopic2)
      KTable(topology, TestInputTopic1).combineByKey
        .fullJoin(otherTable) { (value1, value2) =>
          if (value1 != Some("Record 3")) value1.orNull + " + " + value2.orNull
          else throw new NullPointerException
        }
        .to(TestOutputTopic.name)
    }
    val records = KMockBuilder(streamTopology)
      .input(TestInputTopic1, (1, "Record 1"))
      .input(TestInputTopic2, (1, "Record 2"))
      .input(TestInputTopic1, (2, "Record 3"))
      .input(TestInputTopic2, (2, "Record 4"))
      .input(TestInputTopic1, (3, "Record 5"))
      .input(TestInputTopic2, (4, "Record 6"))
      .output(TestOutputTopic)
    records shouldEqual Seq((1, "Record 1 + null"),
                            (1, "Record 1 + Record 2"),
                            (2, null),
                            (2, null),
                            (3, "Record 5 + null"),
                            (4, "null + Record 6"))
  }

  private def joinedStream(operationByKey: IntTableJoiner,
                           operationByValue: StringTableJoiner,
                           expectedResult: Seq[_]): Unit = {
    it should "join two tables by a primary key" in {
      val streamTopology = (topology: StreamsBuilder) => {
        val otherTable = KTable(topology, TestInputTopic2)
        operationByKey(otherTable, KTable(topology, TestInputTopic1).combineByKey)
          .to(TestOutputTopic.name)
      }
      val records = KMockBuilder(streamTopology)
        .input(TestInputTopic1, (1, "Record 1"))
        .input(TestInputTopic2, (1, "Record 2"))
        .input(TestInputTopic1, (2, "Record 3"))
        .input(TestInputTopic2, (2, "Record 4"))
        .input(TestInputTopic1, (3, "Record 5"))
        .input(TestInputTopic2, (4, "Record 6"))
        .output(TestOutputTopic)
      records shouldEqual expectedResult
    }

    it should "join two tables by a record value" in {
      val streamTopology = (topology: StreamsBuilder) => {
        val otherTable = KTable(topology, TestInputTopic3)
        operationByValue(otherTable,
                         KTable(topology, TestInputTopic1).combineBy((_, value) => value))
          .to(TestOutputTopic.name)
      }
      val records = KMockBuilder(streamTopology)
        .input(TestInputTopic1, (1, "Record 1"))
        .input(TestInputTopic3, ("Record 1", "Record 2"))
        .input(TestInputTopic1, (2, "Record 3"))
        .input(TestInputTopic3, ("Record 3", "Record 4"))
        .input(TestInputTopic1, (3, "Record 5"))
        .input(TestInputTopic3, ("Record 6", "Record 6"))
        .output(TestOutputTopic)
      records shouldEqual expectedResult
    }
  }

}
