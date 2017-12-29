package org.eu.fuzzy.kafka.streams.functions.kstream

import org.eu.fuzzy.kafka.streams.KStream
import org.eu.fuzzy.kafka.streams.serialization.{KeySerde, ValueSerde}

/**
 * Represents a set of functions to transform a record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait TransformFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.TransformFunctions[K, V, KStream] {

  /**
   * Returns a new stream with a zero or more records for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute the new output records
   * @param keySerde  a serialization format for the output record key
   * @param valueSerde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#flatMap]]
   */
  // format: off
  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)])
                     (implicit keySerde: KeySerde[KR], valueSerde: ValueSerde[VR]): KStream[KR, VR]
  // format: on

  /**
   * Returns a new stream with a zero or more records with unmodified keys and new values for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute the new output values
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#flatMapValues]]
   */
  def flatMapValues[VR](mapper: V => Iterable[VR])(implicit serde: ValueSerde[VR]): KStream[K, VR]
}
