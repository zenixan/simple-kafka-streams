package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try

import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{KeyValueMapper, ValueMapper}
import org.apache.kafka.streams.kstream.{Produced, Serialized, KTable => KafkaTable}

import org.eu.fuzzy.kafka.streams.{KTopic, KStream, KTable, ErrorHandler}
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.functions.ktable.{
  AggregateFunctions,
  JoinFunctions,
  BasicJoinFunctions
}
import org.eu.fuzzy.kafka.streams.internals.ktable.{AbstractJoinFunctions, AggFunctions}
import org.eu.fuzzy.kafka.streams.serialization.Serde
import org.eu.fuzzy.kafka.streams.support.JsonSerde
import org.eu.fuzzy.kafka.streams.state.recordStoreOptions

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
private[streams] final case class TableWrapper[K, V](topic: KTopic[K, V],
                                                     internalTable: KafkaTable[K, V],
                                                     builder: StreamsBuilder,
                                                     errorHandler: ErrorHandler)
    extends KTable[K, V] {

  override def queryableStoreName: String = internalTable.queryableStoreName

  override def filter(predicate: (K, V) => Boolean, options: Options[K, V]): KTable[K, V] = {
    val filteredTable =
      internalTable.filter(predicate.asPredicate(topic, errorHandler), options.toMaterialized)
    val newTopic = KTopic(options.keySerde, options.valSerde)
    this.copy(newTopic, filteredTable)
  }

  override def filter(predicate: (K, V) => Boolean): KTable[K, V] =
    filter(predicate, recordStoreOptions(getStateStoreName, topic.keySerde, topic.valSerde))

  /** Transforms an each input record in this table. */
  private def internalMap[KR, VR](mapper: KeyValueMapper[K, V, KeyValue[KR, VR]],
                                  options: Options[KR, VR]): KTable[KR, VR] = {
    val serialized = Serialized.`with`(options.keySerde, options.valSerde)
    val transformedTable = internalTable
      .groupBy(mapper, serialized)
      .reduce((_: VR, newValue: VR) => newValue,
              (_: VR, _: VR) => tombstone[VR],
              options.toMaterialized)
    this.copy(options.toTopic, transformedTable)
  }

  override def mapKeys[KR](mapper: (K, V) => KR, options: Options[KR, V]): KTable[KR, V] = {
    val tableMapper: KeyValueMapper[K, V, KeyValue[KR, V]] = (key, value) =>
      Try(mapper(key, value))
        .map(newKey => new KeyValue(newKey, value))
        .recover {
          case error =>
            Some(errorHandler.onMapKeysError[KR](error, topic, key, value))
              .map(fallbackKey => new KeyValue(fallbackKey, value))
              .getOrElse(new KeyValue(tombstone[KR], value))
        }
        .get
    internalMap(tableMapper, options)
  }

  override def mapKeys[KR](mapper: (K, V) => KR)(implicit serde: Serde[KR]): KTable[KR, V] =
    mapKeys(mapper, recordStoreOptions(getStateStoreName, serde, topic.valSerde))

  override def mapValues[VR](mapper: V => VR, options: Options[K, VR]): KTable[K, VR] = {
    val tableMapper: ValueMapper[V, VR] = (value) =>
      Try(mapper(value)).recover {
        case error => errorHandler.onMapValuesError(error, topic, value)
      }.get
    val transformedTable = internalTable.mapValues[VR](tableMapper, options.toMaterialized)
    val newTopic = KTopic(options.keySerde, options.valSerde)
    this.copy(newTopic, transformedTable)
  }

  override def mapValues[VR](mapper: V => VR)(implicit serde: Serde[VR]): KTable[K, VR] =
    mapValues(mapper, recordStoreOptions(getStateStoreName, topic.keySerde, serde))

  override def map[KR, VR](mapper: (K, V) => (KR, VR), options: Options[KR, VR]): KTable[KR, VR] = {
    val tableMapper: KeyValueMapper[K, V, KeyValue[KR, VR]] = (key, value) =>
      Try(mapper(key, value))
        .map(record => new KeyValue(record._1, record._2))
        .recover {
          case error =>
            Some(errorHandler.onMapError[KR, VR](error, topic, key, value))
              .getOrElse(new KeyValue(tombstone[KR], tombstone[VR]))
        }
        .get
    internalMap(tableMapper, options)
  }

  // format: off
  override def map[KR, VR](mapper: (K, V) => (KR, VR))
                          (implicit keySerde: Serde[KR], valSerde: Serde[VR]): KTable[KR, VR] =
    map(mapper, recordStoreOptions(getStateStoreName, keySerde, valSerde))
  // format: on

  // format: off
  override def combineByKey: JoinFunctions[K, V, K] = new AbstractJoinFunctions[K, V, K](this) {
    override def innerJoin[VO, VR](other: KTable[K, VO], options: Options[K, VR])
                                  (joiner: (V, VO) => VR): KTable[K, VR] = {
      val joinedTable: KafkaTable[K, VR] = internalTable
        .join(other.internalTable,
              joiner.asInnerJoiner(topic, other.topic, errorHandler),
              options.toMaterialized)
      copy(options.toTopic, joinedTable)
    }

    override def leftJoin[VO, VR](other: KTable[K, VO], options: Options[K, VR])
                                 (joiner: (V, Option[VO]) => VR): KTable[K, VR] = {
      val joinedTable: KafkaTable[K, VR] = internalTable
        .leftJoin(other.internalTable,
                  joiner.asLeftJoiner(topic, other.topic, errorHandler),
                  options.toMaterialized)
      copy(options.toTopic, joinedTable)
    }

    override def fullJoin[VO, VR](other: KTable[K, VO], options: Options[K, VR])
                                 (joiner: (Option[V], Option[VO]) => VR): KTable[K, VR] = {
      val joinedTable: KafkaTable[K, VR] = internalTable
        .outerJoin(other.internalTable,
                   joiner.asFullJoiner(topic, other.topic, errorHandler),
                   options.toMaterialized)
      copy(options.toTopic, joinedTable)
    }
  }
  // format: on

  /**
   * Prepares a table to support non-key joining:
   *  1. Transform this table to a table with a foreign key `JK`.
   *  2. Join the transformed table with a given table.
   *  3. Transform the joined table to a table with a key of source table.
   */
  private def createLookupTable[JK, VO](mapper: (K, V) => JK,
                                        other: KTable[JK, VO]): KafkaTable[K, VO] = {
    val foreignKeyMapper: KeyValueMapper[K, V, KeyValue[JK, K]] = (key, value) =>
      Try(mapper(key, value))
        .map(newKey => new KeyValue(newKey, key))
        .recover {
          case error =>
            Some(errorHandler.onCombineError[JK](error, topic, key, value))
              .map(fallbackKey => new KeyValue(fallbackKey, key))
              .getOrElse(new KeyValue(tombstone[JK], key))
        }
        .get
    val stateStore = recordStoreOptions(getStateStoreName, other.topic.keySerde, topic.keySerde)
    val foreignKeyedTable = internalMap(foreignKeyMapper, stateStore)
    val lookupSerde = JsonSerde(topic.keySerde.typeRef, other.topic.valSerde.typeRef)
    foreignKeyedTable.combineByKey
      .innerJoin(other)((key, value) => (key, value))(lookupSerde)
      .map((_, record) => record)(topic.keySerde, other.topic.valSerde)
      .internalTable
  }

  // format: off
  override def combineBy[JK](mapper: (K, V) => JK): BasicJoinFunctions[K, V, JK] =
    new AbstractJoinFunctions[K, V, JK](this) {
      override def innerJoin[VO, VR](other: KTable[JK, VO], options: Options[K, VR])
                                    (joiner: (V, VO) => VR): KTable[K, VR] = {
        val lookupTable = createLookupTable(mapper, other)
        val joinedTable: KafkaTable[K, VR] =
          internalTable.join(lookupTable,
                             joiner.asInnerJoiner(topic, other.topic, errorHandler),
                             options.toMaterialized)
        copy(options.toTopic, joinedTable)
      }

      override def leftJoin[VO, VR](other: KTable[JK, VO], options: Options[K, VR])
                                   (joiner: (V, Option[VO]) => VR): KTable[K, VR] = {
        val lookupTable = createLookupTable(mapper, other)
        val joinedTable: KafkaTable[K, VR] =
          internalTable.leftJoin(lookupTable,
                                 joiner.asLeftJoiner(topic, other.topic, errorHandler),
                                 options.toMaterialized)
        copy(options.toTopic, joinedTable)
      }

      override def fullJoin[VO, VR](other: KTable[K, VO], options: Options[K, VR])
                                   (joiner: (Option[V], Option[VO]) => VR): KTable[K, VR] = {
        throw new UnsupportedOperationException
      }
    }
  // format: on

  override def toStream: KStream[K, V] =
    StreamWrapper(topic, internalTable.toStream, builder, errorHandler)

  override def foreach(action: (K, V) => Unit): Unit = toStream.foreach(action)

  override def to(topic: String, options: Produced[K, V]): Unit = toStream.to(topic, options)

  override def groupByKey: AggregateFunctions[K, V] = groupBy((key, _) => key)(topic.keySerde)

  // format: off
  override def groupBy[KR](mapper: (K, V) => KR)
                          (implicit serde: Serde[KR]): AggregateFunctions[KR, V] = {
  // format: on
    val tableMapper = (key: K, value: V) => new KeyValue(mapper(key, value), value)
    val serialized = Serialized.`with`(serde, topic.valSerde)
    val groupedTable =
      internalTable.groupBy(tableMapper.asKeyMapper(topic, errorHandler), serialized)
    val newTopic = KTopic(serde, topic.valSerde)
    new AggFunctions(newTopic, groupedTable, builder, errorHandler)
  }
}
