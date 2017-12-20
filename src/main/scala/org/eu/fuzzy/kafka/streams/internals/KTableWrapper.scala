package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try

import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Materialized, Predicate, Produced, ValueMapper, KTable => KafkaTable}

import org.eu.fuzzy.kafka.streams.{KStream, KTable, KTopic}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde

/**
 * A default implementation of improved wrapper for the changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic
 * @param internalTable  a native table to wrap
 * @param builder  a builder of Kafka Streams topology
 * @param errorHandler  a handler of stream errors
 */
private[streams] class KTableWrapper[K, V](
    val topic: KTopic[K, V],
    val internalTable: KafkaTable[K, V],
    builder: StreamsBuilder,
    errorHandler: ErrorHandler)
  extends KTable[K, V] {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  override def queryableStoreName: String = internalTable.queryableStoreName

  /** Represents an anonymous copy of current topic. */
  private lazy val anonymousTopic: KTopic[K, V] = KTopic(null)(topic.keySerde, topic.valueSerde)

  override def filter(predicate: (K, V) => Boolean): KTable[K, V] =
    filter(predicate, Materialized.`with`(topic.keySerde, topic.valueSerde))

  override def filter(predicate: (K, V) => Boolean, options: Materialized[K, V, StateStore]): KTable[K, V] = {
    val tableFilter: Predicate[K, V] = (key, value) =>
      Try(predicate(key, value)).recover(errorHandler.handle(topic, FilterOperation, key, value)).get
    val filteredTable = internalTable.filter(tableFilter, options)
    new KTableWrapper(anonymousTopic, filteredTable, builder, errorHandler)
  }

  override def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): KTable[K, VR] =
    mapValues(mapper, Materialized.`with`[K, VR, StateStore](topic.keySerde, serde))(serde)

  override def mapValues[VR](mapper: V => VR, options: Materialized[K, VR, StateStore])
                            (implicit serde: ValueSerde[VR]): KTable[K, VR] = {
    val tableMapper: ValueMapper[V, VR] = (value) =>
      Try(mapper(value)).recover(errorHandler.handle(topic, MapValuesOperation, value)).get
    val newTable = internalTable.mapValues[VR](tableMapper, options)
    val newTopic = KTopic(null)(topic.keySerde, serde)
    new KTableWrapper(newTopic, newTable, builder, errorHandler)
  }

  override def toStream: KStream[K, V] = new KStreamWrapper(topic, internalTable.toStream, builder, errorHandler)

  override def foreach(action: (K, V) => Unit): Unit = toStream.foreach(action)

  override def to(topic: String, options: Produced[K, V]): Unit = toStream.to(topic, options)

  override def through(topic: String, options: Produced[K, V]): KTable[K, V] = {
    to(topic, options)
    val newTable = builder.table(topic, Consumed.`with`(this.topic.keySerde, this.topic.valueSerde))
    val newTopic = KTopic(topic)(this.topic.keySerde, this.topic.valueSerde)
    new KTableWrapper(newTopic, newTable, builder, errorHandler)
  }

  override def through(topic: String, options: Materialized[K, V, StateStore]): KTable[K, V] = {
    to(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))
    val newTable = builder.table(topic, options)
    val newTopic = KTopic(topic)(this.topic.keySerde, this.topic.valueSerde)
    new KTableWrapper(newTopic, newTable, builder, errorHandler)
  }
}
