package org.eu.fuzzy.kafka.streams.functions.kstream

import org.apache.kafka.streams.kstream.JoinWindows
import org.eu.fuzzy.kafka.streams.{KGlobalTable, KTable, KStream}
import org.eu.fuzzy.kafka.streams.serialization.Serde

/**
 * Represents a set of functions to merge a record stream with another stream/table.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam JK a type of record key in the stream to be joined
 */
trait JoinFunctions[K, V, JK] extends TableJoinFunctions[K, V, JK] {

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using
   * windowed inner join.
   *
   * The key-based join is done according to following conditions:
   *  - Input records with a null key or a null value are ignored and do not trigger the join.
   *  - Two input records are joined if their timestamps are '''close''' to each other as defined
   *    by the time window.
   *
   * <table border='1'>
   *  <caption>Example (assuming all input records belong to the correct windows)</caption>
   *  <tr>
   *    <th>This</th>
   *    <th>Other</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K2:B&gt;</td>
   *    <td>&lt;K2:b&gt;</td>
   *    <td>&lt;K2:joiner(B, b)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K3:c&gt;</td>
   *    <td></td>
   *  </tr>
   * </table>
   *
   * @tparam VO  a value type of the other stream
   * @tparam VR  a value type of the result stream
   *
   * @param otherStream  a stream to be joined with this stream
   * @param windows  a sliding window specification
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  // format: off
  def innerJoin[VO, VR](otherStream: KStream[JK, VO], windows: JoinWindows)
                       (joiner: (V, VO) => VR)
                       (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using
   * windowed left join.
   *
   * The key-based join is done according to following conditions:
   *  - Input records with a null key or a null value are ignored and do not trigger the join.
   *  - Two input records are joined if their timestamps are '''close''' to each other as defined
   *    by the time window.
   *  - For each input record on the left side that does not have any match on the right side,
   *    the `joiner` function will be called with a `null` value.
   *
   * <table border='1'>
   *  <caption>Example (assuming all input records belong to the correct windows)</caption>
   *  <tr>
   *    <th>This</th>
   *    <th>Other</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K2:B&gt;</td>
   *    <td>&lt;K2:b&gt;</td>
   *    <td>&lt;K2:joiner(B, b)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K3:c&gt;</td>
   *    <td></td>
   *  </tr>
   * </table>
   *
   * @tparam VO  a value type of the other stream
   * @tparam VR  a value type of the result stream
   *
   * @param otherStream  a stream to be joined with this stream
   * @param windows  a sliding window specification
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  // format: off
  def lefJoin[VO, VR](otherStream: KStream[JK, VO], windows: JoinWindows)
                     (joiner: (V, Option[VO]) => VR)
                     (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using
   * windowed outer join.
   *
   * The key-based join is done according to following conditions:
   *  - Input records with a null key or a null value are ignored and do not trigger the join.
   *  - Two input records are joined if their timestamps are '''close''' to each other as defined
   *    by the time window.
   *  - For each input record on one side that does not have any match on the other side,
   *    the `joiner` function will be called with a `null` value.
   *
   * <table border='1'>
   *  <caption>Example (assuming all input records belong to the correct windows)</caption>
   *  <tr>
   *    <th>This</th>
   *    <th>Other</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K2:B&gt;</td>
   *    <td>&lt;K2:b&gt;</td>
   *    <td>&lt;K2:joiner(null, b)&gt;<br />&lt;K2:joiner(B, b)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K3:c&gt;</td>
   *    <td>&lt;K3:joiner(null, c)&gt;</td>
   *  </tr>
   * </table>
   *
   * @tparam VO  a value type of the other stream
   * @tparam VR  a value type of the result stream
   *
   * @param otherStream  a stream to be joined with this stream
   * @param windows  a sliding window specification
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   */
  // format: off
  def fullJoin[VO, VR](otherStream: KStream[JK, VO], windows: JoinWindows)
                      (joiner: (Option[V], Option[VO]) => VR)
                      (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on
}
