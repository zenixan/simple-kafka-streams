package org.eu.fuzzy.kafka.streams.internals

import scala.util.Try
import scala.collection.JavaConverters._

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Predicate, Produced, KStream => KafkaStream}
import org.eu.fuzzy.kafka.streams.error.ErrorHandler
import org.eu.fuzzy.kafka.streams.{KStream, KTopic}
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}

/**
 * A default implementation of improved wrapper for the stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 *
 * @param topic  an identity of Kafka topic
 * @param internalStream  a native stream to wrap
 * @param handler  a handler of stream errors
 */
private[streams] class KStreamWrapper[K, V](
    val topic: KTopic[K, V],
    val internalStream: KafkaStream[K, V],
    handler: ErrorHandler) extends KStream[K, V] {

  import org.eu.fuzzy.kafka.streams.error.CheckedOperation._

  override def filter(predicate: (K, V) => Boolean): KStream[K, V] = {
    val filteredStream = internalStream.filter { (key, value) =>
      Try(predicate(key, value))
        .recover(handler.handle(topic, FilterOperation)(key, value)).get
    }
    new KStreamWrapper(KTopic(null)(topic.keySerde, topic.valueSerde), filteredStream, handler)
  }

  override def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                              (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR] = {
    val newStream = internalStream.flatMap[KR, VR] { (key, value) =>
      Try(mapper(key, value).map(record => new KeyValue(record._1, record._2)))
        .map(_.asJavaCollection)
        .recover(handler.handle(topic, FlatMapOperation)(key, value)).get
    }
    new KStreamWrapper(KTopic(null)(keySerde, valueSerde), newStream, handler)
  }

  override def flatMapValues[VR](mapper: V => Iterable[VR])(implicit serde: ValueSerde[VR]): KStream[K, VR] = {
    val newStream = internalStream.flatMapValues[VR] { value =>
      Try(mapper(value).asJavaCollection).recover(handler.handle(topic, FlatMapValuesOperation, value)).get
    }
    new KStreamWrapper(KTopic(null)(topic.keySerde, serde), newStream, handler)
  }

  override def branch(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]] = {
    val anonymousTopic = KTopic(null)(topic.keySerde, topic.valueSerde)
    val kafkaPredicates = predicates.map(predicate => new Predicate[K, V] {
      override def test(key: K, value: V): Boolean =
        Try(predicate(key, value)).recover(handler.handle(topic, BranchOperation, value)).get
    })
    internalStream.branch(kafkaPredicates:_*).view
      .map(new KStreamWrapper(anonymousTopic, _, handler))
  }

  override def merge(stream: KStream[K, V]): KStream[K, V] =
    new KStreamWrapper(topic, internalStream.merge(stream.internalStream), handler)

  override def foreach(action: (K, V) => Unit): Unit = internalStream.foreach { (key, value) =>
    Try(action(key, value)).recover(handler.handle(topic, ForeachOperation, value)).get
  }

  override def peek(action: (K, V) => Unit): KStream[K, V] = {
    val newStream = internalStream.peek { (key, value) =>
      Try(action(key, value)).recover(handler.handle(topic, PeekOperation, value)).get
    }
    new KStreamWrapper(topic, newStream, handler)
  }

  override def to(topic: String): Unit = internalStream.to(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))

  override def through(topic: String): KStream[K, V] = {
    val newStream = internalStream.through(topic, Produced.`with`(this.topic.keySerde, this.topic.valueSerde))
    new KStreamWrapper(KTopic(topic)(this.topic.keySerde, this.topic.valueSerde), newStream, handler)
  }
}
