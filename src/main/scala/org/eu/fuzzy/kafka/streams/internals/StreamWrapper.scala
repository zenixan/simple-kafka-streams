package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try
import scala.collection.JavaConverters._

import org.apache.kafka.streams.kstream.{Predicate, KeyValueMapper, Joined, Produced, Serialized}
import org.apache.kafka.streams.kstream.{Window, Windows, JoinWindows, SessionWindows}
import org.apache.kafka.streams.kstream.{KStream => KafkaStream}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}

import org.eu.fuzzy.kafka.streams.{KGlobalTable, KStream, KTable, KTopic}
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.functions.kstream.AggregateFunctions
import org.eu.fuzzy.kafka.streams.functions.kstream.SessionWindowedFunctions
import org.eu.fuzzy.kafka.streams.functions.kstream.TimeWindowedFunctions
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler

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

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  /** Represents an anonymous copy of current topic. */
  private lazy val anonymousTopic: KTopic[K, V] = KTopic(topic.keySerde, topic.valueSerde)

  override def filter(predicate: (K, V) => Boolean): KStream[K, V] = {
    val filteredStream = internalStream.filter { (key, value) =>
      Try(predicate(key, value))
        .recover(errorHandler.handle(topic, FilterOperation, key, value))
        .get
    }
    this.copy(anonymousTopic, filteredStream)
  }

  // format: off
  override def map[KR, VR](mapper: (K, V) => (KR, VR))
                          (implicit keySerde: KeySerde[KR],
                           valueSerde: ValueSerde[VR]): KStream[KR, VR] =
    flatMap((key, value) => Seq(mapper(key, value)))
  // format: on

  override def mapKeys[KR](mapper: K => KR)(implicit serde: KeySerde[KR]): KStream[KR, V] =
    map((key, value) => (mapper(key), value))(serde, topic.valueSerde)

  override def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR] =
    flatMapValues(mapper.andThen(Seq(_)))

  // format: off
  override def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                              (implicit keySerde: KeySerde[KR],
                               valueSerde: ValueSerde[VR]): KStream[KR, VR] = {
  // format: on
    val transformedStream = internalStream.flatMap[KR, VR] { (key, value) =>
      Try(
        mapper(key, value)
          .map(record => new KeyValue(record._1, record._2)))
        .map(_.asJavaCollection)
        .recover(errorHandler.handle(topic, FlatMapOperation, key, value))
        .get
    }
    val newTopic = KTopic(keySerde, valueSerde)
    this.copy(newTopic, transformedStream)
  }

  // format: off
  override def flatMapValues[VR](mapper: V => Iterable[VR])
                                (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
  // format: on
    val transformedStream = internalStream.flatMapValues[VR] { value =>
      Try(mapper(value).asJavaCollection)
        .recover(errorHandler.handle(topic, FlatMapValuesOperation, value))
        .get
    }
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, transformedStream)
  }

  override def innerJoin[GK, GV, VR](
      globalTable: KGlobalTable[GK, GV],
      mapper: (K, V) => GK,
      joiner: (V, GV) => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val keyMapper: KeyValueMapper[K, V, GK] = (key, value) =>
      Try(mapper(key, value))
        .recover(errorHandler.handle(topic, JoinByKeyOperation, key, value))
        .get
    val joinedStream: KafkaStream[K, VR] = internalStream.join(
      globalTable.internalTable,
      keyMapper,
      joiner.asInnerJoiner(topic, errorHandler))
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  // format: off
  override def innerJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)
                                (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
  // format: on
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, table.topic.valueSerde)
    val joinedStream: KafkaStream[K, VR] =
      internalStream.join(table.internalTable, joiner.asInnerJoiner(topic, errorHandler), joinSerde)
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  // format: off
  override def innerJoin[VO, VR](otherStream: KStream[K, VO],
                                 joiner: (V, VO) => VR,
                                 windows: JoinWindows)
                                (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
  // format: on
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val joinedStream: KafkaStream[K, VR] = internalStream.join(
      otherStream.internalStream,
      joiner.asInnerJoiner(topic, errorHandler),
      windows,
      joinSerde)
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  // format: off
  override def leftJoin[GK, GV, VR](globalTable: KGlobalTable[GK, GV],
                                    mapper: (K, V) => GK,
                                    joiner: (V, GV) => VR)
                                   (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
  // format: on
    val keyMapper: KeyValueMapper[K, V, GK] = (key, value) =>
      Try(mapper(key, value))
        .recover(errorHandler.handle(topic, JoinByKeyOperation, key, value))
        .get
    val joinedStream: KafkaStream[K, VR] = internalStream.leftJoin(
      globalTable.internalTable,
      keyMapper,
      joiner.asLeftJoiner(topic, errorHandler))
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  // format: off
  override def leftJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)
                               (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
  // format: on
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, table.topic.valueSerde)
    val joinedStream: KafkaStream[K, VR] = internalStream.leftJoin(
      table.internalTable,
      joiner.asLeftJoiner(topic, errorHandler),
      joinSerde)
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  // format: off
  override def lefJoin[VO, VR](otherStream: KStream[K, VO],
                               joiner: (V, VO) => VR,
                               windows: JoinWindows)
                              (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
  // format: on
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val joinedStream: KafkaStream[K, VR] = internalStream.leftJoin(
      otherStream.internalStream,
      joiner.asLeftJoiner(topic, errorHandler),
      windows,
      joinSerde)
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  // format: off
  override def outerJoin[VO, VR](otherStream: KStream[K, VO],
                                 joiner: (V, VO) => VR,
                                 windows: JoinWindows)
                                (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    // format: on
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val joinedStream: KafkaStream[K, VR] = internalStream.outerJoin(
      otherStream.internalStream,
      joiner.asOuterJoiner(topic, errorHandler),
      windows,
      joinSerde)
    val newTopic = KTopic(topic.keySerde, serde)
    this.copy(newTopic, joinedStream)
  }

  override def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]] = {
    val kafkaPredicates = predicates.map(predicate =>
      new Predicate[K, V] {
        override def test(key: K, value: V): Boolean =
          Try(predicate(key, value))
            .recover(errorHandler.handle(topic, BranchOperation, value))
            .get
    })
    internalStream
      .branch(kafkaPredicates: _*)
      .view
      .map(this.copy(anonymousTopic, _))
  }

  override def split(predicate: (K, V) => Boolean): (KStream[K, V], KStream[K, V]) = {
    val streams =
      branch((key, value) => predicate(key, value), (key, value) => !predicate(key, value))
    (streams(0), streams(1))
  }

  override def merge(stream: KStream[K, V]): KStream[K, V] =
    this.copy(topic, internalStream.merge(stream.internalStream))

  override def foreach(action: (K, V) => Unit): Unit = internalStream.foreach { (key, value) =>
    Try(action(key, value)).recover(errorHandler.handle(topic, ForeachOperation, value)).get
  }

  override def peek(action: (K, V) => Unit): KStream[K, V] = {
    val newStream = internalStream.peek { (key, value) =>
      Try(action(key, value)).recover(errorHandler.handle(topic, PeekOperation, value)).get
    }
    this.copy(topic, newStream)
  }

  override def to(topic: String, options: Produced[K, V]): Unit = internalStream.to(topic, options)

  override def through(topic: String, options: Produced[K, V]): KStream[K, V] = {
    val newStream = internalStream.through(topic, options)
    val newTopic = KTopic(topic)(this.topic.keySerde, this.topic.valueSerde)
    this.copy(newTopic, newStream)
  }

  override def through(topic: String): KStream[K, V] =
    through(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))

  override def groupByKey: AggregateFunctions[K, K, V, Options] = {
    val groupedStream =
      internalStream.groupByKey(Serialized.`with`(topic.keySerde, topic.valueSerde))
    new StreamAggFunctions(anonymousTopic, internalStream.groupByKey, builder, errorHandler)
  }

  // format: off
  override def groupBy[KR](mapper: (K, V) => KR)
                          (implicit serde: KeySerde[KR]): AggregateFunctions[KR, KR, V, Options] =
    map((key, value) => (mapper(key, value), value))(serde, topic.valueSerde).groupByKey
  // format: on

  override def windowedByKey(windows: SessionWindows): SessionWindowedFunctions[K, V] = {
    val groupedStream =
      internalStream
        .groupByKey(Serialized.`with`(topic.keySerde, topic.valueSerde))
        .windowedBy(windows)
    new SessionAggFunctions(anonymousTopic, groupedStream, builder, errorHandler)
  }

  // format: off
  override def windowedBy[KR](mapper: (K, V) => KR, windows: SessionWindows)
                             (implicit serde: KeySerde[KR]): SessionWindowedFunctions[KR, V] =
    map((key, value) => (mapper(key, value), value))(serde, topic.valueSerde).windowedByKey(windows)
  // format: on

  override def windowedByKey[W <: Window](windows: Windows[W]): TimeWindowedFunctions[K, V] = {
    val groupedStream = internalStream
      .groupByKey(Serialized.`with`(topic.keySerde, topic.valueSerde))
      .windowedBy(windows)
    new TimeAggFunctions(anonymousTopic, groupedStream, builder, errorHandler)
  }

  // format: off
  override def windowedBy[KR, W <: Window](mapper: (K, V) => KR, windows: Windows[W])
                                          (implicit
                                           serde: KeySerde[KR]): TimeWindowedFunctions[KR, V] =
    map((key, value) => (mapper(key, value), value))(serde, topic.valueSerde).windowedByKey(windows)
  // format: on
}
