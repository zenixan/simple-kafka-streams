package org.eu.fuzzy.kafka.streams.functions.kstream

import org.eu.fuzzy.kafka.streams.serialization.Serde
import org.eu.fuzzy.kafka.streams.{KStream, KGlobalTable, KTable}

/**
 * Represents a set of functions to merge a record stream with tables.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam JK a type of key in the table to be joined
 */
trait TableJoinFunctions[K, V, JK] {

  /**
   * Returns a new stream by joining records of this stream with records from the global table using
   * non-windowed inner join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and
   *    do not trigger the join.
   *
   * <table border='1'>
   *  <caption>Example</caption>
   *  <tr>
   *    <th>Stream</th>
   *    <th>Table</th>
   *    <th>State</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:C&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:joiner(C, b)&gt;</td>
   *  </tr>
   * </table>
   *
   * @tparam GV  a type of record value in the global table
   * @tparam VR  a value type of the result stream
   *
   * @param globalTable  a global table to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  // format: off
  def innerJoin[GV, VR](globalTable: KGlobalTable[JK, GV])
                       (joiner: (V, GV) => VR)
                       (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on

  /**
   * Returns a new stream by joining records of this stream with records from the table using
   * non-windowed inner join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and
   *    do not trigger the join.
   *
   * <table border='1'>
   *  <caption>Example</caption>
   *  <tr>
   *    <th>Stream</th>
   *    <th>Table</th>
   *    <th>State</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td></td>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:C&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:joiner(C, b)&gt;</td>
   *  </tr>
   * </table>
   *
   * @tparam VT  a value type of the table
   * @tparam VR  a value type of the result stream
   *
   * @param table  a table to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   */
  // format: off
  def innerJoin[VT, VR](table: KTable[JK, VT])
                       (joiner: (V, VT) => VR)
                       (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on

  /**
   * Returns a new stream by joining records of this stream with records from the global table using
   * non-windowed left join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and
   *    do not trigger the join.
   *  - For each input record on the left side that does not have any match on the right side,
   *    the `joiner` function will be called with a `null` value.
   *
   * <table border='1'>
   *  <caption>Example</caption>
   *  <tr>
   *    <th>Stream</th>
   *    <th>Global Table</th>
   *    <th>State</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td></td>
   *   </tr>
   *  <tr>
   *    <td>&lt;K1:C&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:joiner(C, b)&gt;</td>
   *  </tr>
   * </table>
   *
   * @tparam GV  a type of record value in the global table
   * @tparam VR  a value type of the result stream
   *
   * @param globalTable  a global table to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#leftJoin]]
   */
  // format: off
  def leftJoin[GV, VR](globalTable: KGlobalTable[JK, GV])
                      (joiner: (V, Option[GV]) => VR)
                      (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on

  /**
   * Returns a new stream by joining records of this stream with records from the table using
   * non-windowed left join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and
   *    do not trigger the join.
   *  - For each input record on the left side that does not have any match on the right side,
   *    the `joiner` function will be called with a `null` value.
   *
   * <table border='1'>
   *  <caption>Example</caption>
   *  <tr>
   *    <th>Stream</th>
   *    <th>Table</th>
   *    <th>State</th>
   *    <th>Result</th>
   *  </tr>
   *  <tr>
   *    <td>&lt;K1:A&gt;</td>
   *    <td></td>
   *    <td></td>
   *    <td>&lt;K1:joiner(A, null)&gt;</td>
   *  </tr>
   *  <tr>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td></td>
   *   </tr>
   *  <tr>
   *    <td>&lt;K1:C&gt;</td>
   *    <td></td>
   *    <td>&lt;K1:b&gt;</td>
   *    <td>&lt;K1:joiner(C, b)&gt;</td>
   *  </tr>
   * </table>
   *
   * @tparam VT  a value type of the table
   * @tparam VR  a value type of the result stream
   *
   * @param table  a table to be joined with this stream
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#leftJoin]]
   */
  // format: off
  def leftJoin[VT, VR](table: KTable[JK, VT])
                      (joiner: (V, Option[VT]) => VR)
                      (implicit serde: Serde[VR]): KStream[K, VR]
  // format: on
}
