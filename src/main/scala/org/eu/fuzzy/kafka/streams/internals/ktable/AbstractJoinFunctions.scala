package org.eu.fuzzy.kafka.streams.internals.ktable

import org.eu.fuzzy.kafka.streams.KTable
import org.eu.fuzzy.kafka.streams.functions.ktable.JoinFunctions
import org.eu.fuzzy.kafka.streams.serialization.Serde
import org.eu.fuzzy.kafka.streams.state.recordStoreOptions
import org.eu.fuzzy.kafka.streams.internals.{TableWrapper, getStateStoreName}

/**
 * Provides a join builder for a changelog stream.
 *
 * @tparam K  a type of primary key
 * @tparam V  a type of record value
 * @tparam JK a type of primary key in the table to be joined
 *
 * @param table  an underlying changelog stream
 */
private[streams] abstract class AbstractJoinFunctions[K, V, JK](table: TableWrapper[K, V])
    extends JoinFunctions[K, V, JK] {
  // format: off
  override def innerJoin[VO, VR](other: KTable[JK, VO])
                                (joiner: (V, VO) => VR)
                                (implicit serde: Serde[VR]): KTable[K, VR] =
    innerJoin(other, recordStoreOptions(getStateStoreName, table.topic.keySerde, serde))(joiner)

  override def leftJoin[VO, VR](other: KTable[JK, VO])
                               (joiner: (V, Option[VO]) => VR)
                               (implicit serde: Serde[VR]): KTable[K, VR] =
    leftJoin(other, recordStoreOptions(getStateStoreName, table.topic.keySerde, serde))(joiner)

  override def fullJoin[VO, VR](other: KTable[K, VO])
                               (joiner: (Option[V], Option[VO]) => VR)
                               (implicit serde: Serde[VR]): KTable[K, VR] =
    fullJoin(other, recordStoreOptions(getStateStoreName, table.topic.keySerde, serde))(joiner)
  // format: on
}
