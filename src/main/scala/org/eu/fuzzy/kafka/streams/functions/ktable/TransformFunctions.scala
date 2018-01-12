package org.eu.fuzzy.kafka.streams.functions.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.Options

trait TransformFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.TransformFunctions[K, V, KTable] {

  /**
   * Returns a new stream with a new key for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   *
   * @param mapper  a function to compute a new value for each record
   * @param options  a set of options to use when materializing to the local state store
   */
  def mapKeys[KR](mapper: (K, V) => KR, options: Options[KR, V]): KTable[KR, V]

  /**
   * Returns a new stream with a new value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new value for each record
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#mapValues]]
   */
  def mapValues[VR](mapper: V => VR, options: Options[K, VR]): KTable[K, VR]

  /**
   * Returns a new stream with a new key and value for each input record.
   *
   * This is a stateless record-by-record operation.
   *
   * @tparam KR  a new type of record key
   * @tparam VR  a new type of record value
   *
   * @param mapper  a function to compute a new key and value for each
   * @param options  a set of options to use when materializing to the local state store
   */
  def map[KR, VR](mapper: (K, V) => (KR, VR), options: Options[KR, VR]): KTable[KR, VR]
}
