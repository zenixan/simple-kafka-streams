package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try
import scala.collection.JavaConverters._
import java.util.Collections.{singletonList, emptyList}

import org.apache.kafka.streams.{StreamsBuilder, KeyValue}
import org.apache.kafka.streams.kstream.{Produced, Joined, ValueJoiner, Predicate, KeyValueMapper}
import org.apache.kafka.streams.kstream.{Windows, SessionWindows, Window, JoinWindows, Serialized}
import org.apache.kafka.streams.kstream.{KStream => KafkaStream}

import org.eu.fuzzy.kafka.streams.{KGlobalTable, KTable, KTopic, KStream}
import org.eu.fuzzy.kafka.streams.ErrorHandler
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.support.JsonSerde
import org.eu.fuzzy.kafka.streams.functions.kstream.{JoinFunctions, TableJoinFunctions}
import org.eu.fuzzy.kafka.streams.functions.kstream.{
  SessionWindowedFunctions,
  AggregateFunctions,
  TimeWindowedFunctions
}
import org.eu.fuzzy.kafka.streams.serialization.Serde
import org.eu.fuzzy.kafka.streams.internals.kstream.{
  AggFunctions,
  TimeAggFunctions,
  SessionAggFunctions
}

/**
 * Implements an improved wrapper for the record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic
 * @param internalStream  a native stream to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] final case class StreamWrapper[K, V](topic: KTopic[K, V],
                                                      internalStream: KafkaStream[K, V],
                                                      builder: StreamsBuilder,
                                                      errorHandler: ErrorHandler)
    extends KStream[K, V] {

  /** Represents an anonymous copy of current topic. */
  private lazy val anonymousTopic: KTopic[K, V] = KTopic(topic.keySerde, topic.valSerde)

  override def filter(predicate: (K, V) => Boolean): KStream[K, V] = {
    val filteredStream = internalStream.filter(predicate.asPredicate(topic, errorHandler))
    this.copy(anonymousTopic, filteredStream)
  }

  override def mapKeys[KR](mapper: (K, V) => KR)(implicit serde: Serde[KR]): KStream[KR, V] = {
    val transformedStream = internalStream.flatMap[KR, V] { (key, value) =>
      Try(mapper(key, value))
        .map(newKey => singletonList(new KeyValue(newKey, value)))
        .recover {
          case error =>
            Option(errorHandler.onMapKeysError[KR](error, topic, key, value))
              .map(fallbackKey => singletonList(new KeyValue(fallbackKey, value)))
              .getOrElse(emptyList[KeyValue[KR, V]])
        }
        .get
    }
    val newTopic = KTopic(serde, topic.valSerde)
    this.copy(newTopic, transformedStream)
  }

  override def mapValues[VR](mapper: V => VR)(implicit serde: Serde[VR]): KStream[K, VR] = {
    val transformedStream = internalStream.flatMapValues[VR] { value =>
      Try(singletonList(mapper(value))).recover {
        case error =>
          Option(errorHandler.onMapValuesError[VR](error, topic, value))
            .map(fallbackValue => singletonList(fallbackValue))
            .getOrElse(emptyList[VR])
      }.get
    }
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, transformedStream)
  }

  // format: off
  override def map[KR, VR](mapper: (K, V) => (KR, VR))
                          (implicit keySerde: Serde[KR], valSerde: Serde[VR]): KStream[KR, VR] = {
    val transformedStream = internalStream.flatMap[KR, VR] { (key, value) =>
      Try(mapper(key, value))
        .map(record => singletonList(new KeyValue(record._1, record._2)))
        .recover {
          case error =>
            Option(errorHandler.onMapError(error, topic, key, value))
              .map(fallbackValue => singletonList(fallbackValue))
              .getOrElse(emptyList[KeyValue[KR, VR]])
        }
        .get
    }
    val newTopic = KTopic(keySerde, valSerde)
    this.copy(newTopic, transformedStream)
  }
  // format: on

  // format: off
  override def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                              (implicit keySerde: Serde[KR],
                               valSerde: Serde[VR]): KStream[KR, VR] = {
    val transformedStream = internalStream.flatMap[KR, VR] { (key, value) =>
      Try(mapper(key, value).map(record => new KeyValue(record._1, record._2)))
        .map(_.asJavaCollection)
        .recover {
          case error => errorHandler.onFlatMapError(error, topic, key, value)
        }
        .get
    }
    val newTopic = KTopic(keySerde, valSerde)
    this.copy(newTopic, transformedStream)
  }
  // format: on

  // format: off
  override def flatMapValues[VR](mapper: V => Iterable[VR])
                                (implicit serde: Serde[VR]): KStream[K, VR] = {
    val transformedStream = internalStream.flatMapValues[VR] { value =>
      Try(mapper(value).asJavaCollection).recover {
        case error => errorHandler.onFlatMapValuesError(error, topic, value)
      }.get
    }
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, transformedStream)
  }
  // format: on

  override def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]] = {
    val kafkaPredicates = predicates.map(predicate =>
      new Predicate[K, V] {
        override def test(key: K, value: V): Boolean =
          Try(predicate(key, value)).recover {
            case error => errorHandler.onBranchError(error, topic, key, value)
          }.get
    })
    internalStream
      .branch(kafkaPredicates: _*)
      .view
      .map(this.copy(anonymousTopic, _))
  }

  override def split(predicate: (K, V) => Boolean): (KStream[K, V], KStream[K, V]) = {
    val streams =
      branch((key, value) => predicate(key, value), (key, value) => !predicate(key, value))
    (streams.head, streams.last)
  }

  override def merge(stream: KStream[K, V]): KStream[K, V] =
    this.copy(topic, internalStream.merge(stream.internalStream))

  override def foreach(action: (K, V) => Unit): Unit = internalStream.foreach { (key, value) =>
    Try(action(key, value)).recover {
      case error => errorHandler.onForeachError(error, topic, key, value)
    }.get
  }

  override def peek(action: (K, V) => Unit): KStream[K, V] = {
    val newStream = internalStream.peek { (key, value) =>
      Try(action(key, value)).recover {
        case error => errorHandler.onPeekError(error, topic, key, value)
      }.get
    }
    this.copy(topic, newStream)
  }

  override def to(topic: String, options: Produced[K, V]): Unit = internalStream.to(topic, options)

  override def through(topic: String, options: Produced[K, V]): KStream[K, V] = {
    val newStream = internalStream.through(topic, options)
    val newTopic = KTopic(topic)(this.topic.keySerde, this.topic.valSerde)
    this.copy(newTopic, newStream)
  }

  override def through(topic: String): KStream[K, V] =
    through(topic, Produced.`with`(this.topic.keySerde, this.topic.valSerde))

  /** An identity function for the record key. */
  private lazy val keyIdentity: KeyValueMapper[K, V, K] = (key, _) => key

  // format: off
  override def combineByKey: JoinFunctions[K, V, K] = new JoinFunctions[K, V, K] {
    override def innerJoin[GV, VR](globalTable: KGlobalTable[K, GV])
                                  (joiner: (V, GV) => VR)
                                  (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinedStream: KafkaStream[K, VR] = internalStream
        .join(globalTable.internalTable,
              keyIdentity,
              joiner.asInnerJoiner(topic, globalTable.topic, errorHandler))
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }

    override def innerJoin[VT, VR](table: KTable[K, VT])
                                  (joiner: (V, VT) => VR)
                                  (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinSerde = Joined.`with`(topic.keySerde, topic.valSerde, table.topic.valSerde)
      val joinedStream: KafkaStream[K, VR] = internalStream
        .join(table.internalTable,
              joiner.asInnerJoiner(topic, table.topic, errorHandler),
              joinSerde)
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }

    override def innerJoin[VO, VR](otherStream: KStream[K, VO], windows: JoinWindows)
                                  (joiner: (V, VO) => VR)
                                  (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinSerde = Joined.`with`(topic.keySerde, topic.valSerde, otherStream.topic.valSerde)
      val joinedStream: KafkaStream[K, VR] = internalStream
        .join(otherStream.internalStream,
              joiner.asInnerJoiner(topic, otherStream.topic, errorHandler),
              windows,
              joinSerde)
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }

    override def leftJoin[GV, VR](globalTable: KGlobalTable[K, GV])
                                 (joiner: (V, Option[GV]) => VR)
                                 (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinedStream: KafkaStream[K, VR] = internalStream
        .leftJoin(globalTable.internalTable,
                  keyIdentity,
                  joiner.asLeftJoiner(topic, globalTable.topic, errorHandler))
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }

    override def leftJoin[VT, VR](table: KTable[K, VT])
                                 (joiner: (V, Option[VT]) => VR)
                                 (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinSerde = Joined.`with`(topic.keySerde, topic.valSerde, table.topic.valSerde)
      val joinedStream: KafkaStream[K, VR] = internalStream
        .leftJoin(table.internalTable,
                  joiner.asLeftJoiner(topic, table.topic, errorHandler),
                  joinSerde)
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }

    override def lefJoin[VO, VR](otherStream: KStream[K, VO], windows: JoinWindows)
                                (joiner: (V, Option[VO]) => VR)
                                (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinSerde = Joined.`with`(topic.keySerde, topic.valSerde, otherStream.topic.valSerde)
      val joinedStream: KafkaStream[K, VR] = internalStream
        .leftJoin(otherStream.internalStream,
                  joiner.asLeftJoiner(topic, otherStream.topic, errorHandler),
                  windows,
                  joinSerde)
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }

    override def fullJoin[VO, VR](otherStream: KStream[K, VO], windows: JoinWindows)
                                 (joiner: (Option[V], Option[VO]) => VR)
                                 (implicit serde: Serde[VR]): KStream[K, VR] = {
      val joinSerde = Joined.`with`(topic.keySerde, topic.valSerde, otherStream.topic.valSerde)
      val joinedStream: KafkaStream[K, VR] = internalStream
        .outerJoin(otherStream.internalStream,
                   joiner.asFullJoiner(topic, otherStream.topic, errorHandler),
                   windows,
                   joinSerde)
      val newTopic = KTopic(topic.keySerde, serde)
      copy(newTopic, joinedStream)
    }
  }
  // format: on

  // format: off
  override def combineBy[JK](mapper: (K, V) => JK): TableJoinFunctions[K, V, JK] =
    new TableJoinFunctions[K, V, JK] {
      override def innerJoin[GV, VR](globalTable: KGlobalTable[JK, GV])
                                    (joiner: (V, GV) => VR)
                                    (implicit serde: Serde[VR]): KStream[K, VR] = {
        val joinedStream: KafkaStream[K, VR] = internalStream
          .join(globalTable.internalTable,
                mapper.asKeyMapper(topic, errorHandler),
                joiner.asInnerJoiner(topic, globalTable.topic, errorHandler))
        val newTopic = KTopic(topic.keySerde, serde)
        copy(newTopic, joinedStream)
      }

      override def innerJoin[VT, VR](table: KTable[JK, VT])
                                    (joiner: (V, VT) => VR)
                                    (implicit serde: Serde[VR]): KStream[K, VR] = {
        val streamJoiner: ValueJoiner[(K, V), VT, (K, VR)] = (streamRecord, tableValue) => {
          val (key, value) = streamRecord
          (key, joiner(value, tableValue))
        }
        val joinSerde = Joined.`with`(
          table.topic.keySerde,
          JsonSerde(topic.keySerde.typeRef, topic.valSerde.typeRef),
          table.topic.valSerde
        )
        val joinedStream = foreignKeyedStream(mapper)
          .join[VT, (K, VR)](table.internalTable, streamJoiner, joinSerde)
          .map[K, VR]((_, value) => new KeyValue(value._1, value._2))
        val newTopic = KTopic(topic.keySerde, serde)
        copy(newTopic, joinedStream)
      }

      override def leftJoin[GV, VR](globalTable: KGlobalTable[JK, GV])
                                   (joiner: (V, Option[GV]) => VR)
                                   (implicit serde: Serde[VR]): KStream[K, VR] = {
        val joinedStream: KafkaStream[K, VR] = internalStream
          .leftJoin(globalTable.internalTable,
                    mapper.asKeyMapper(topic, errorHandler),
                    joiner.asLeftJoiner(topic, globalTable.topic, errorHandler))
        val newTopic = KTopic(topic.keySerde, serde)
        copy(newTopic, joinedStream)
      }

      override def leftJoin[VT, VR](table: KTable[JK, VT])
                                   (joiner: (V, Option[VT]) => VR)
                                   (implicit serde: Serde[VR]): KStream[K, VR] = {
        val streamJoiner: ValueJoiner[(K, V), VT, (K, VR)] = (streamRecord, tableValue) => {
          val (key, value) = streamRecord
          (key, joiner(value, Option(tableValue)))
        }
        val joinSerde = Joined.`with`(
          table.topic.keySerde,
          JsonSerde(topic.keySerde.typeRef, topic.valSerde.typeRef),
          table.topic.valSerde
        )
        val joinedStream = foreignKeyedStream(mapper)
          .leftJoin[VT, (K, VR)](table.internalTable, streamJoiner, joinSerde)
          .map[K, VR]((_, value) => new KeyValue(value._1, value._2))
        val newTopic = KTopic(topic.keySerde, serde)
        copy(newTopic, joinedStream)
      }
    }
  // format: on

  /** Returns a stream with a key required for the further join operation. */
  private def foreignKeyedStream[JK, VT, VR](mapper: (K, V) => JK): KafkaStream[JK, (K, V)] = {
    internalStream.flatMap { (key, value) =>
      Try(mapper(key, value))
        .map(newKey => singletonList(new KeyValue(newKey, (key, value))))
        .recover {
          case error =>
            Some(errorHandler.onCombineError[JK](error, topic, key, value))
              .map(fallbackKey => singletonList(new KeyValue(fallbackKey, (key, value))))
              .getOrElse(emptyList[KeyValue[JK, (K, V)]])
        }
        .get
    }
  }

  override def groupByKey: AggregateFunctions[K, K, V, Options] = {
    val groupedStream =
      internalStream.groupByKey(Serialized.`with`(topic.keySerde, topic.valSerde))
    new AggFunctions(anonymousTopic, internalStream.groupByKey, builder, errorHandler)
  }

  // format: off
  override def groupBy[KR](mapper: (K, V) => KR)
                          (implicit serde: Serde[KR]): AggregateFunctions[KR, KR, V, Options] = {
    val groupedStream = internalStream.groupBy(mapper.asKeyMapper(topic, errorHandler))
    val newTopic = KTopic(serde, topic.valSerde)
    new AggFunctions(newTopic, groupedStream, builder, errorHandler)
  }
  // format: on

  override def windowedByKey(windows: SessionWindows): SessionWindowedFunctions[K, V] = {
    val groupedStream =
      internalStream
        .groupByKey(Serialized.`with`(topic.keySerde, topic.valSerde))
        .windowedBy(windows)
    new SessionAggFunctions(anonymousTopic, windows, groupedStream, builder, errorHandler)
  }

  // format: off
  override def windowedBy[KR](mapper: (K, V) => KR, windows: SessionWindows)
                             (implicit serde: Serde[KR]): SessionWindowedFunctions[KR, V] =
    map((key, value) => (mapper(key, value), value))(serde, topic.valSerde).windowedByKey(windows)
  // format: on

  override def windowedByKey[W <: Window](windows: Windows[W]): TimeWindowedFunctions[K, V] = {
    val groupedStream = internalStream
      .groupByKey(Serialized.`with`(topic.keySerde, topic.valSerde))
      .windowedBy(windows)
    new TimeAggFunctions(anonymousTopic, windows, groupedStream, builder, errorHandler)
  }

  // format: off
  override def windowedBy[KR, W <: Window](mapper: (K, V) => KR, windows: Windows[W])
                                          (implicit serde: Serde[KR]): TimeWindowedFunctions[KR, V] =
    map((key, value) => (mapper(key, value), value))(serde, topic.valSerde).windowedByKey(windows)
  // format: on
}
