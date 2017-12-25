package org.eu.fuzzy.kafka.streams.functions.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde

/**
 * Represents a set of joining functions for a changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 *
 * @define innerJoinDesc
 * Returns a new table by joining records of this table with records from the given table using non-windowed inner join.
 *
 * The key-based join is done according to following conditions:
 *  - Input records with a `null` key are ignored and do not trigger the join.
 *  - Input records with a `null` value are interpreted as tombstones for the corresponding key, which indicate
 *    the deletion of the key from the table. Tombstones do not trigger the join.
 *
 * <table border='1'>
 *  <caption>Example</caption>
 *  <tr>
 *    <th>This Table</th>
 *    <th>This State</th>
 *    <th>Other Table</th>
 *    <th>Other State</th>
 *    <th>Result</th>
 *  </tr>
 *  <tr>
 *    <td>&lt;K1:A&gt;</td>
 *    <td>&lt;K1:A&gt;</td>
 *    <td></td>
 *    <td></td>
 *    <td></td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td>&lt;K1:A&gt;</td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:joiner(A, b)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td>&lt;K1:C&gt;</td>
 *    <td>&lt;K1:C&gt;</td>
 *    <td></td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:joiner(C, b)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td>&lt;K1:C&gt;</td>
 *    <td>&lt;K1:null&gt;</td>
 *    <td></td>
 *    <td>&lt;K1:null&gt;</td>
 *  </tr>
 * </table>
 *
 * @define leftJoinDesc
 * Returns a new table by joining records of this table with records from the given table using non-windowed left join.
 *
 * The key-based join is done according to following conditions:
 *  - Input records with a `null` key are ignored.
 *  - Input records with a `null` value are interpreted as tombstones for the corresponding key, which indicate
 *    the deletion of the key from the table. Tombstones do not trigger the join.
 *  - For each input record on the left side that does not have any match on the right side, the `joiner` function
 *    will be called with a `null` value.
 *
 * <table border='1'>
 *  <caption>Example</caption>
 *  <tr>
 *    <th>This Table</th>
 *    <th>This State</th>
 *    <th>Other Table</th>
 *    <th>Other State</th>
 *    <th>Result</th>
 *  </tr>
 *  <tr>
 *    <td>&lt;K1:A&gt;</td>
 *    <td>&lt;K1:A&gt;</td>
 *    <td></td>
 *    <td></td>
 *    <td>&lt;K1:joiner(A, null)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td>&lt;K1:A&gt;</td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:joiner(A, b)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td>&lt;K1:null&gt;</td>
 *    <td></td>
 *    <td></td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:null&gt;</td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td></td>
 *    <td>&lt;K1:null&gt;</td>
 *    <td></td>
 *    <td></td>
 *  </tr>
 * </table>
 *
 * @define outerJoinDesc
 * Returns a new table by joining records of this table with records from the given table using non-windowed outer join.
 *
 * The key-based join is done according to following conditions:
 *  - Input records with a `null` key are ignored.
 *  - Input records with a `null` value are interpreted as tombstones for the corresponding key, which indicate
 *    the deletion of the key from the table. Tombstones do not trigger the join.
 *  - For each input record on one side that does not have any match on the other side,
 *    the `joiner` function will be called with a `null` value.
 *
 * <table border='1'>
 *  <caption>Example</caption>
 *  <tr>
 *    <th>This Table</th>
 *    <th>This State</th>
 *    <th>Other Table</th>
 *    <th>Other State</th>
 *    <th>Result</th>
 *  </tr>
 *  <tr>
 *    <td>&lt;K1:A&gt;</td>
 *    <td>&lt;K1:A&gt;</td>
 *    <td></td>
 *    <td></td>
 *    <td>&lt;K1:joiner(A, null)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td>&lt;K1:A&gt;</td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:joiner(A, b)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td>&lt;K1:null&gt;</td>
 *    <td></td>
 *    <td></td>
 *    <td>&lt;K1:b&gt;</td>
 *    <td>&lt;K1:joiner(null, b)&gt;</td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td></td>
 *    <td>&lt;K1:null&gt;</td>
 *    <td></td>
 *    <td>&lt;K1:null&gt;</td>
 *  </tr>
 * </table>
 */
trait JoinFunctions[K, V] {
  /**
   * $innerJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#innerJoin]]
   */
  def innerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR)(implicit serde: ValueSerde[VR]): KTable[K, VR]

  /**
   * $innerJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#innerJoin]]
   */
  def innerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR, options: Options[K, VR]): KTable[K, VR]

  /**
   * $leftJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#leftJoin]]
   */
  def leftJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR)(implicit serde: ValueSerde[VR]): KTable[K, VR]

  /**
   * $leftJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#leftJoin]]
   */
  def leftJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR, options: Options[K, VR]): KTable[K, VR]

  /**
   * $outerJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#outerJoin]]
   */
  def outerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR)(implicit serde: ValueSerde[VR]): KTable[K, VR]

  /**
   * $outerJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param options  a set of options to use when materializing to the local state store
   *
   * @see [[org.apache.kafka.streams.kstream.KTable#outerJoin]]
   */
  def outerJoin[VO, VR](other: KTable[K, VO], joiner: (V, VO) => VR, options: Options[K, VR]): KTable[K, VR]
}
