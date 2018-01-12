package org.eu.fuzzy.kafka.streams.functions.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.serialization.Serde

/**
 * Represents a common set of functions to merge a changelog stream with another changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 * @tparam JK a type of primary key in the table to be joined
 *
 * @define innerJoinDesc
 * Returns a new table by joining records of this table with records from the given table using non-windowed inner join.
 *
 * The key-based join is done according to following conditions:
 *  - Input records with a `null` key are ignored and do not trigger the join.
 *  - Input records with a `null` value are interpreted as tombstones for the corresponding key,
 *    which indicate the deletion of the key from the table.
 *    Tombstones do not trigger the join.
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
 *  - Input records with a `null` value are interpreted as tombstones for the corresponding key,
 *    which indicate the deletion of the key from the table.
 *    Tombstones do not trigger the join.
 *  - For each input record on the left side that does not have any match on the right side,
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
 */
trait BasicJoinFunctions[K, V, JK] {

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
   * @see [[[https://en.wikipedia.org/wiki/Join_(SQL)#Inner_join Inner Join]]
   * @see [[org.apache.kafka.streams.kstream.KTable#innerJoin]]
   */
  // format: off
  def innerJoin[VO, VR](other: KTable[JK, VO])
                       (joiner: (V, VO) => VR)
                       (implicit serde: Serde[VR]): KTable[K, VR]
  // format: on

  /**
   * $innerJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param options  a set of options to use when materializing to the local state store
   * @param joiner  a function to compute the join result for a pair of matching records
   *
   * @see [[[https://en.wikipedia.org/wiki/Join_(SQL)#Inner_join Inner Join]]
   * @see [[org.apache.kafka.streams.kstream.KTable#innerJoin]]
   */
  // format: off
  def innerJoin[VO, VR](other: KTable[JK, VO], options: Options[K, VR])
                       (joiner: (V, VO) => VR): KTable[K, VR]
  // format: on

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
   * @see [[https://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join Left Outer Join]]
   * @see [[org.apache.kafka.streams.kstream.KTable#leftJoin]]
   */
  // format: off
  def leftJoin[VO, VR](other: KTable[JK, VO])
                      (joiner: (V, Option[VO]) => VR)
                      (implicit serde: Serde[VR]): KTable[K, VR]
  // format: on

  /**
   * $leftJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param options  a set of options to use when materializing to the local state store
   * @param joiner  a function to compute the join result for a pair of matching records
   *
   * @see [[https://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join Left Outer Join]]
   * @see [[org.apache.kafka.streams.kstream.KTable#leftJoin]]
   */
  // format: off
  def leftJoin[VO, VR](other: KTable[JK, VO], options: Options[K, VR])
                      (joiner: (V, Option[VO]) => VR): KTable[K, VR]
  // format: on
}
