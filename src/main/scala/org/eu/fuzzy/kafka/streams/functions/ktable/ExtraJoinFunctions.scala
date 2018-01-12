package org.eu.fuzzy.kafka.streams.functions.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.KTable.Options
import org.eu.fuzzy.kafka.streams.serialization.Serde

/**
 * Represents an extended set of functions to merge a changelog stream with another changelog stream.
 *
 * @note Full join by 'foreign key' isn't supported.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 *
 * @define fullJoinDesc
 * Returns a new table by joining records of this table with records from the given table using non-windowed outer join.
 *
 * The key-based join is done according to following conditions:
 *  - Input records with a `null` key are ignored.
 *  - Input records with a `null` value are interpreted as tombstones for the corresponding key,
 *    which indicate the deletion of the key from the table. Tombstones do not trigger the join.
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
trait ExtraJoinFunctions[K, V] {

  /**
   * $fullJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param joiner  a function to compute the join result for a pair of matching records
   * @param serde  a serialization format for the output record value
   *
   * @see [[https://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join Full Outer Join]]
   * @see [[org.apache.kafka.streams.kstream.KTable#outerJoin]]
   */
  // format: off
  def fullJoin[VO, VR](other: KTable[K, VO])
                      (joiner: (Option[V], Option[VO]) => VR)
                      (implicit serde: Serde[VR]): KTable[K, VR]
  // format: on

  /**
   * $fullJoinDesc
   *
   * @tparam VO  a value type of the other table
   * @tparam VR  a value type of the result table
   *
   * @param other  the other table to be joined with this table
   * @param options  a set of options to use when materializing to the local state store
   * @param joiner  a function to compute the join result for a pair of matching records
   *
   * @see [[https://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join Full Outer Join]]
   * @see [[org.apache.kafka.streams.kstream.KTable#outerJoin]]
   */
  // format: off
  def fullJoin[VO, VR](other: KTable[K, VO], options: Options[K, VR])
                      (joiner: (Option[V], Option[VO]) => VR): KTable[K, VR]
  // format: on
}
