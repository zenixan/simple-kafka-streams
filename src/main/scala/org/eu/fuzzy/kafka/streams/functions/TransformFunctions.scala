package org.eu.fuzzy.kafka.streams.functions

import scala.language.higherKinds
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}

/**
 * Represents a set of functions to transform the record/changelog stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam S  a type of stream, i.e. [[org.eu.fuzzy.kafka.streams.KStream]] or [[org.eu.fuzzy.kafka.streams.KTable]]
 */
trait TransformFunctions[K, V, S[K, V]] {
  /**
   * Returns a new stream with a new key for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   *
   * @param mapper  a function to compute a new value for each record
   * @param serde  a serialization format for the output record value
   */
  def mapKeys[KR](mapper: K => KR)(implicit serde: KeySerde[KR]): S[KR, V]

  /**
   * Returns a new stream with a new value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new value for each record
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#mapValues]]
   * @see [[org.apache.kafka.streams.kstream.KTable#mapValues]]
   */
  def mapValues[VR](mapper: V => VR)(implicit serde: ValueSerde[VR]): S[K, VR]

  /**
   * Returns a new stream with a new key and value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new key and value for each record
   * @param keySerde  a serialization format for the output record key
   * @param valueSerde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#map]]
   */
  def map[KR, VR](mapper: (K, V) => (KR, VR))(implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): S[KR, VR]
}
