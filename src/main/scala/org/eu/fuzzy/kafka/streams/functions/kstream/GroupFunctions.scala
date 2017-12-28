package org.eu.fuzzy.kafka.streams.functions.kstream

import org.apache.kafka.streams.kstream.{Window, Windows, SessionWindows}
import org.eu.fuzzy.kafka.streams.serialization.KeySerde

/**
 * Represents a set of functions to group the records of record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait GroupFunctions[K, V]
    extends org.eu.fuzzy.kafka.streams.functions.GroupFunctions[K, V, AggFunctions] {

  /**
   * Groups the records of this stream by their current key according to a time windows.
   *
   * @param windows  a window specification with time boundaries
   */
  def windowedByKey[W <: Window](windows: Windows[W]): TimeWindowedFunctions[K, V]

  /**
   * Groups the records of this stream using the given function and time windows.
   *
   * @param mapper  a function to compute a new grouping key
   * @param windows  a window specification with time boundaries
   * @param serde  a serialization format for the output record key
   */
  // format: off
  def windowedBy[KR, W <: Window](mapper: (K, V) => KR, windows: Windows[W])
                                 (implicit serde: KeySerde[KR]): TimeWindowedFunctions[KR, V]
  // format: on

  /**
   * Groups the records of this stream by their current key according to a session windows.
   *
   * @param windows  a window specification without fixed time boundaries
   */
  def windowedByKey(windows: SessionWindows): SessionWindowedFunctions[K, V]

  /**
   * Groups the records of this stream using the given function and session windows.
   *
   * @param mapper  a function to compute a new grouping key
   * @param windows  a window specification without fixed time boundaries
   * @param serde  a serialization format for the output record key
   */
  // format: off
  def windowedBy[KR](mapper: (K, V) => KR, windows: SessionWindows)
                    (implicit serde: KeySerde[KR]): SessionWindowedFunctions[KR, V]
  // format: on
}
