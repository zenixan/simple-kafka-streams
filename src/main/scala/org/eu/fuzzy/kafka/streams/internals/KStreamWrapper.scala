package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try
import scala.collection.JavaConverters._

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{JoinWindows, Joined, KeyValueMapper, Predicate, Produced, ValueJoiner}
import org.apache.kafka.streams.kstream.{KStream => KafkaStream}

import org.eu.fuzzy.kafka.streams.error.{CheckedOperation, ErrorHandler}
import org.eu.fuzzy.kafka.streams.{KGlobalTable, KStream, KTable, KTopic}
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}

/**
 * A default implementation of improved wrapper for the stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic
 * @param internalStream  a native stream to wrap
 * @param errorHandler  a handler of stream errors
 */
private[streams] class KStreamWrapper[K, V](
    val topic: KTopic[K, V],
    val internalStream: KafkaStream[K, V],
    errorHandler: ErrorHandler)
  extends KStream[K, V] {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  /** Represents an anonymous copy of current topic. */
  private val anonymousTopic: KTopic[K, V] = KTopic(null)(topic.keySerde, topic.valueSerde)

  override def filter(predicate: (K, V) => Boolean): KStream[K, V] = {
    val filteredStream = internalStream.filter { (key, value) =>
      Try(predicate(key, value)).recover(errorHandler.handle(topic, FilterOperation)(key, value)).get
    }
    new KStreamWrapper(anonymousTopic, filteredStream, errorHandler)
  }

  override def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                              (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR] = {
    val newStream = internalStream.flatMap[KR, VR] { (key, value) =>
      Try(mapper(key, value).map(record => new KeyValue(record._1, record._2)))
        .map(_.asJavaCollection)
        .recover(errorHandler.handle(topic, FlatMapOperation)(key, value)).get
    }
    new KStreamWrapper(KTopic(null)(keySerde, valueSerde), newStream, errorHandler)
  }

  override def flatMapValues[VR](mapper: V => Iterable[VR])(implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val newStream = internalStream.flatMapValues[VR] { value =>
      Try(mapper(value).asJavaCollection).recover(errorHandler.handle(topic, FlatMapValuesOperation, value)).get
    }
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  /** Returns a safe wrapper for the join function. */
  @inline def valueJoiner[V1, V2, VR](operation: CheckedOperation, joiner: (V1, V2) => VR): ValueJoiner[V1, V2, VR] =
    (value1, value2) => Try(joiner(value1, value2)).recover(errorHandler.handle(topic, operation, (value1, value2))).get

  override def join[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                               (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val keyMapper: KeyValueMapper[K, V, GK] = (key, value) =>
      Try(mapper(key, value)).recover(errorHandler.handle(topic, JoinByKeyOperation)(key, value)).get
    val newStream: KafkaStream[K, VR] = internalStream.join(
      globalTable.internalTable, keyMapper, valueJoiner(InnerJoinOperation, joiner)
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def join[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)
                           (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, table.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.join(
      table.internalTable, valueJoiner(InnerJoinOperation, joiner), joinSerde
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def join[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                           (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.join(
      otherStream.internalStream, valueJoiner(InnerJoinOperation, joiner), windows, joinSerde
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def leftJoin[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                                   (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val keyMapper: KeyValueMapper[K, V, GK] = (key, value) =>
      Try(mapper(key, value)).recover(errorHandler.handle(topic, JoinByKeyOperation)(key, value)).get
    val newStream: KafkaStream[K, VR] = internalStream.leftJoin(
      globalTable.internalTable, keyMapper, valueJoiner(LeftJoinOperation, joiner)
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def leftJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)
                               (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, table.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.leftJoin(
      table.internalTable, valueJoiner(LeftJoinOperation, joiner), joinSerde
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def lefJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                              (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.leftJoin(
      otherStream.internalStream, valueJoiner(LeftJoinOperation, joiner), windows, joinSerde
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def outerJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                                (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.outerJoin(
      otherStream.internalStream, valueJoiner(OuterJoinOperation, joiner), windows, joinSerde
    )
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, errorHandler)
  }

  override def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]] = {
    val kafkaPredicates = predicates.map(predicate => new Predicate[K, V] {
      override def test(key: K, value: V): Boolean =
        Try(predicate(key, value)).recover(errorHandler.handle(topic, BranchOperation, value)).get
    })
    internalStream.branch(kafkaPredicates:_*).view
      .map(new KStreamWrapper(anonymousTopic, _, errorHandler))
  }

  override def merge(stream: KStream[K, V]): KStream[K, V] =
    new KStreamWrapper(topic, internalStream.merge(stream.internalStream), errorHandler)

  override def foreach(action: (K, V) => Unit): Unit = internalStream.foreach { (key, value) =>
    Try(action(key, value)).recover(errorHandler.handle(topic, ForeachOperation, value)).get
  }

  override def peek(action: (K, V) => Unit): KStream[K, V] = {
    val newStream = internalStream.peek { (key, value) =>
      Try(action(key, value)).recover(errorHandler.handle(topic, PeekOperation, value)).get
    }
    new KStreamWrapper(topic, newStream, errorHandler)
  }

  override def to(topic: String): Unit = internalStream.to(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))

  override def through(topic: String): KStream[K, V] = {
    val newStream = internalStream.through(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))
    new KStreamWrapper(KTopic(topic)(this.topic.keySerde, this.topic.valueSerde), newStream, errorHandler)
  }
}
