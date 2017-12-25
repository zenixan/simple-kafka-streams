package org.eu.fuzzy.kafka.streams.functions.kstream

import org.apache.kafka.streams.kstream.JoinWindows
import org.eu.fuzzy.kafka.streams.{KGlobalTable, KStream, KTable}
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde

/**
 * Represents a set of joining functions for a record stream.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 */
trait JoinFunctions[K, V] {
  /**
   * Returns a new stream by joining records of this stream with records from the global table using non-windowed inner join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and do not trigger the join.
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
   * @tparam GK  a type of record key in the global table
   * @tparam GV  a type of record value in the global table
   * @tparam VR  a value type of the result stream
   *
   * @param globalTable  a global table to be joined with this stream
   * @param mapper  a function to map the record of this stream to the key of the global table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  def innerJoin[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                           (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the table using non-windowed inner join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and do not trigger the join.
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
  def innerJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using windowed inner join.
   *
   * The key-based join is done according to following conditions:
   *  - Input records with a null key or a null value are ignored and do not trigger the join.
   *  - Two input records are joined if their timestamps are 'close' to each other as defined by the time window.
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
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param windows  a time window
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  def innerJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                       (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the global table using non-windowed left join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and do not trigger the join.
   *  - For each input record on the left side that does not have any match on the right side, the `joiner` function
   *    will be called with a `null` value.
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
   * @tparam GK  a type of record key in the global table
   * @tparam GV  a type of record value in the global table
   * @tparam VR  a value type of the result stream
   *
   * @param globalTable  a global table to be joined with this stream
   * @param mapper  a function to map the record of this stream to the key of the global table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#leftJoin]]
   */
  def leftJoin[GK, GV, VR](globalTable: KGlobalTable[GK, GV], mapper: (K, V) => GK, joiner: (V, GV) => VR)
                          (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the table using non-windowed left join.
   *
   * The key-based join is done according to following conditions:
   *  - Only input records for the stream triggers the join.
   *  - Input records for the stream with a `null` key or a `null` value are ignored and do not trigger the join.
   *  - For each input record on the left side that does not have any match on the right side, the `joiner` function
   *    will be called with a `null` value.
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
  def leftJoin[VT, VR](table: KTable[K, VT], joiner: (V, VT) => VR)(implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using windowed left join.
   *
   * The key-based join is done according to following conditions:
   *  - Input records with a null key or a null value are ignored and do not trigger the join.
   *  - Two input records are joined if their timestamps are 'close' to each other as defined by the time window.
   *  - For each input record on the left side that does not have any match on the right side, the `joiner` function
   *    will be called with a `null` value.
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
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param windows  a time window
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KStream#join]]
   */
  def lefJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                     (implicit serde: ValueSerde[VR]): KStream[K, VR]

  /**
   * Returns a new stream by joining records of this stream with records from the given stream using windowed outer join.
   *
   * The key-based join is done according to following conditions:
   *  - Input records with a null key or a null value are ignored and do not trigger the join.
   *  - Two input records are joined if their timestamps are 'close' to each other as defined by the time window.
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
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param windows  a time window
   * @param serde  a serialization format for the output record value
   */
  def outerJoin[VO, VR](otherStream: KStream[K, VO], joiner: (V, VO) => VR, windows: JoinWindows)
                       (implicit serde: ValueSerde[VR]): KStream[K, VR]
}
