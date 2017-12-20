package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try
import scala.collection.JavaConverters._

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{JoinWindows, Joined, KeyValueMapper, Predicate, Produced, ValueJoiner}
import org.apache.kafka.streams.kstream.{Aggregator, Initializer, Materialized, Reducer}
import org.apache.kafka.streams.kstream.{KStream => KafkaStream}
import org.apache.kafka.streams.state.KeyValueStore

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
    builder: StreamsBuilder,
    errorHandler: ErrorHandler)
  extends KStream[K, V] {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  /** Represents an anonymous copy of current topic. */
  private lazy val anonymousTopic: KTopic[K, V] = KTopic(null)(topic.keySerde, topic.valueSerde)

  override def filter(predicate: (K, V) => Boolean): KStream[K, V] = {
    val filteredStream = internalStream.filter { (key, value) =>
      Try(predicate(key, value)).recover(errorHandler.handle(topic, FilterOperation, key, value)).get
    }
    new KStreamWrapper(anonymousTopic, filteredStream, builder, errorHandler)
  }

  override def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR] =
    flatMapValues(mapper.andThen(Seq(_)))

  override def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                              (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR] = {
    val newStream = internalStream.flatMap[KR, VR] { (key, value) =>
      Try(mapper(key, value).map(record => new KeyValue(record._1, record._2)))
        .map(_.asJavaCollection)
        .recover(errorHandler.handle(topic, FlatMapOperation, key, value)).get
    }
    val newTopic = KTopic(null)(keySerde, valueSerde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def flatMapValues[VR](mapper: V => Iterable[VR])(implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val newStream = internalStream.flatMapValues[VR] { value =>
      Try(mapper(value).asJavaCollection).recover(errorHandler.handle(topic, FlatMapValuesOperation, value)).get
    }
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  /** Returns a safe wrapper for the join function. */
  @inline private def valueJoiner[V1, V2, VR](operation: CheckedOperation, joiner: (V1, V2) => VR): ValueJoiner[V1, V2, VR] =
    (value1, value2) => Try(joiner(value1, value2)).recover(errorHandler.handle(topic, operation, value1, value2)).get

  override def join[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                               (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val keyMapper: KeyValueMapper[K, V, GK] = (key, value) =>
      Try(mapper(key, value)).recover(errorHandler.handle(topic, JoinByKeyOperation, key, value)).get
    val newStream: KafkaStream[K, VR] = internalStream.join(
      globalTable.internalTable, keyMapper, valueJoiner(InnerJoinOperation, joiner)
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def join[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)
                           (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, table.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.join(
      table.internalTable, valueJoiner(InnerJoinOperation, joiner), joinSerde
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def join[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                           (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.join(
      otherStream.internalStream, valueJoiner(InnerJoinOperation, joiner), windows, joinSerde
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def leftJoin[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                                   (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val keyMapper: KeyValueMapper[K, V, GK] = (key, value) =>
      Try(mapper(key, value)).recover(errorHandler.handle(topic, JoinByKeyOperation, key, value)).get
    val newStream: KafkaStream[K, VR] = internalStream.leftJoin(
      globalTable.internalTable, keyMapper, valueJoiner(LeftJoinOperation, joiner)
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def leftJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)
                               (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, table.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.leftJoin(
      table.internalTable, valueJoiner(LeftJoinOperation, joiner), joinSerde
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def lefJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                              (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.leftJoin(
      otherStream.internalStream, valueJoiner(LeftJoinOperation, joiner), windows, joinSerde
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def outerJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                                (implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val joinSerde = Joined.`with`(topic.keySerde, topic.valueSerde, otherStream.topic.valueSerde)
    val newStream: KafkaStream[K, VR] = internalStream.outerJoin(
      otherStream.internalStream, valueJoiner(OuterJoinOperation, joiner), windows, joinSerde
    )
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]] = {
    val kafkaPredicates = predicates.map(predicate => new Predicate[K, V] {
      override def test(key: K, value: V): Boolean =
        Try(predicate(key, value)).recover(errorHandler.handle(topic, BranchOperation, value)).get
    })
    internalStream.branch(kafkaPredicates:_*).view
      .map(new KStreamWrapper(anonymousTopic, _, builder, errorHandler))
  }

  override def merge(stream: KStream[K, V]): KStream[K, V] =
    new KStreamWrapper(topic, internalStream.merge(stream.internalStream), builder, errorHandler)

  override def foreach(action: (K, V) => Unit): Unit = internalStream.foreach { (key, value) =>
    Try(action(key, value)).recover(errorHandler.handle(topic, ForeachOperation, value)).get
  }

  override def peek(action: (K, V) => Unit): KStream[K, V] = {
    val newStream = internalStream.peek { (key, value) =>
      Try(action(key, value)).recover(errorHandler.handle(topic, PeekOperation, value)).get
    }
    new KStreamWrapper(topic, newStream, builder, errorHandler)
  }

  override def to(topic: String, options: Produced[K, V]): Unit = internalStream.to(topic, options)

  override def through(topic: String, options: Produced[K, V]): KStream[K, V] = {
    val newStream = internalStream.through(topic, options)
    val newTopic = KTopic(topic)(this.topic.keySerde, this.topic.valueSerde)
    new KStreamWrapper(newTopic, newStream, builder, errorHandler)
  }

  override def aggregateByKey[VR](initializer: () => VR, aggregator: (K, V, VR) => VR)
                                 (implicit serde: ValueSerde[VR]): KTable[K, VR] = {
    val zeroValue: Initializer[VR] = () => Try(initializer.apply()).recover(errorHandler.handle(topic, InitializerOperation)).get
    val streamAggregator: Aggregator[K, V, VR] = (key, value, aggregate) =>
      Try(aggregator(key, value, aggregate)).recover(errorHandler.handle(topic, AggregateOperation, key, value, aggregate)).get
    val materialized = Materialized.`with`[K, VR, KeyValueStore[Bytes, Array[Byte]]](topic.keySerde, serde)
    val newTable = internalStream.groupByKey.aggregate(zeroValue, streamAggregator, materialized)
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KTableWrapper(newTopic, newTable, builder, errorHandler)
  }

  override def reduceByKey(reducer: (V, V) => V): KTable[K, V] = {
    val streamReducer: Reducer[V] = (aggValue, newValue) =>
      Try(reducer(aggValue, newValue)).recover(errorHandler.handle(topic, ReduceOperation, aggValue, newValue)).get
    val materialized = Materialized.`with`[K, V, KeyValueStore[Bytes, Array[Byte]]](topic.keySerde, topic.valueSerde)
    val newTable = internalStream.groupByKey.reduce(streamReducer, materialized)
    new KTableWrapper(topic, newTable, builder, errorHandler)
  }

}
