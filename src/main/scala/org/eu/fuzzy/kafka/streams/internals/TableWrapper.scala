package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try

import org.apache.kafka.streams.kstream.{KeyValueMapper, Predicate, Produced, Serialized, ValueMapper, KTable => KafkaTable}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}

import org.eu.fuzzy.kafka.streams.{KStream, KTable, KTopic}
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.functions.ktable.AggregateFunctions
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler

/**
 * Implements an improved wrapper for the changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic
 * @param internalTable  a native table to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] case class TableWrapper[K, V](
    topic: KTopic[K, V],
    internalTable: KafkaTable[K, V],
    builder: StreamsBuilder,
    errorHandler: ErrorHandler)
  extends KTable[K, V] {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  override def queryableStoreName: String = internalTable.queryableStoreName

  override def filter(predicate: (K, V) => Boolean, options: Options[K, V]): KTable[K, V] = {
    val newTopic = toTopic(options)
    val tableFilter: Predicate[K, V] = (key, value) =>
      Try(predicate(key, value)).recover(errorHandler.handle(topic, FilterOperation, key, value)).get
    val filteredTable = internalTable.filter(tableFilter, options)
    this.copy(newTopic, filteredTable)
  }

  override def filter(predicate: (K, V) => Boolean): KTable[K, V] =
    filter(predicate, storeOptions(topic.keySerde, topic.valueSerde))

  override def map[KR, VR](mapper: (K, V) => (KR, VR), options: Options[KR, VR]): KTable[KR, VR] = {
    val newTopic = toTopic(options)
    toStream.map(mapper)(newTopic.keySerde, newTopic.valueSerde).reduce((_, newValue) => newValue, options)
  }

  override def map[KR, VR](mapper: (K, V) => (KR, VR))
                          (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KTable[KR, VR] =
    map(mapper, storeOptions(keySerde, valueSerde))

  override def mapKeys[KR](mapper: K => KR, options: Options[KR, V]): KTable[KR, V] =
    map((key, value) => (mapper(key), value), options)

  override def mapKeys[KR](mapper: K => KR)(implicit serde: KeySerde[KR]): KTable[KR, V] =
    map((key, value) => (mapper(key), value))(serde, topic.valueSerde)

  override def mapValues[VR](mapper: V => VR, options: Options[K, VR]): KTable[K, VR] = {
    val newTopic = toTopic(options)
    val tableMapper: ValueMapper[V, VR] = (value) =>
      Try(mapper(value)).recover(errorHandler.handle(topic, MapValuesOperation, value)).get
    val transformedTable = internalTable.mapValues[VR](tableMapper, options)

    this.copy(newTopic, transformedTable)
  }

  override def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): KTable[K, VR] =
    mapValues(mapper, storeOptions(topic.keySerde, serde))

  override def innerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR, options: Options[K, VR]): KTable[K, VR] = {
    val newTopic = toTopic(options)
    val joinedTable: KafkaTable[K, VR] = internalTable.join(
      other.internalTable, joiner.asInnerJoiner(topic, errorHandler), options
    )
    this.copy(newTopic, joinedTable)
  }

  override def innerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR)
                                (implicit serde: ValueSerde[VR]): KTable[K, VR] =
    innerJoin(other, joiner, storeOptions(topic.keySerde, serde))

  override def leftJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR, options: Options[K, VR]): KTable[K, VR] = {
    val newTopic = toTopic(options)
    val joinedTable: KafkaTable[K, VR] = internalTable.leftJoin(
      other.internalTable, joiner.asLeftJoiner(topic, errorHandler), options
    )
    this.copy(newTopic, joinedTable)
  }

  override def leftJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR)
                               (implicit serde: ValueSerde[VR]): KTable[K, VR] =
    leftJoin(other, joiner, storeOptions(topic.keySerde, serde))

  override def outerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR, options: Options[K, VR]): KTable[K, VR] = {
    val newTopic = toTopic(options)
    val joinedTable: KafkaTable[K, VR] = internalTable.outerJoin(
      other.internalTable, joiner.asOuterJoiner(topic, errorHandler), options
    )
    this.copy(newTopic, joinedTable)
  }

  override def outerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR)
                                (implicit serde: ValueSerde[VR]): KTable[K, VR] =
    outerJoin(other, joiner, storeOptions(topic.keySerde, serde))

  override def toStream: KStream[K, V] = StreamWrapper(topic, internalTable.toStream, builder, errorHandler)

  override def foreach(action: (K, V) => Unit): Unit = toStream.foreach(action)

  override def to(topic: String, options: Produced[K, V]): Unit = toStream.to(topic, options)

  def groupBy[KR, VR](mapper: (K, V) => (KR, VR))
                     (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): AggregateFunctions[KR, VR] = {
    val tableMapper: KeyValueMapper[K, V, KeyValue[KR, VR]] = (key, value) =>
      Try(mapper(key, value))
        .map(record => new KeyValue(record._1, record._2))
        .recover(errorHandler.handle(topic, MapOperation, key, value)).get
    val groupedTable = internalTable.groupBy(tableMapper, Serialized.`with`(keySerde, valueSerde))
    val newTopic = KTopic(keySerde, valueSerde)
    new TableAggFunctions(newTopic, groupedTable, builder, errorHandler)
  }
}
