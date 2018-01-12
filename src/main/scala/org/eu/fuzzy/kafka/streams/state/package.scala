package org.eu.fuzzy.kafka.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Windows, SessionWindows}
import org.apache.kafka.streams.state.{Stores, KeyValueStore, WindowStore, SessionStore}
import org.eu.fuzzy.kafka.streams.serialization.Serde

package object state {

  /**
   * Returns a new provider of persistent store for the key-value records.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param name  a name of state store
   * @param keySerde  a serialization format for the record key
   * @param valSerde  a serialization format for the record value
   */
  def recordStoreOptions[K, V](
      name: String,
      keySerde: Serde[K],
      valSerde: Serde[V]): StoreOptions[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    StoreOptions(keySerde,
                 valSerde,
                 cachingEnabled = true,
                 loggingEnabled = true,
                 Map(),
                 Stores.persistentKeyValueStore(name))

  /**
   * Returns a new provider of persistent store for the windowed values.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param name  a name of state store
   * @param keySerde  a serialization format for the record key
   * @param valSerde  a serialization format for the record value
   * @param windows  a window specification with time boundaries
   */
  def windowStoreOptions[K, V](
      name: String,
      keySerde: Serde[K],
      valSerde: Serde[V],
      windows: Windows[_]): StoreOptions[K, V, WindowStore[Bytes, Array[Byte]]] = {
    val storeSupplier =
      Stores.persistentWindowStore(name, windows.maintainMs, windows.segments, windows.size, false)
    StoreOptions(keySerde,
                 valSerde,
                 cachingEnabled = true,
                 loggingEnabled = true,
                 Map(),
                 storeSupplier)
  }

  /**
   * Returns a new provider of persistent store for the aggregated values of sessions.
   *
   * @tparam K  a type of record key
   * @tparam V  a type of record value
   *
   * @param name  a name of state store
   * @param keySerde  a serialization format for the record key
   * @param valSerde  a serialization format for the record value
   * @param windows  a window specification without fixed time boundaries
   */
  def sessionStoreOptions[K, V](
      name: String,
      keySerde: Serde[K],
      valSerde: Serde[V],
      windows: SessionWindows): StoreOptions[K, V, SessionStore[Bytes, Array[Byte]]] = {
    val storeSupplier = Stores.persistentSessionStore(name, windows.maintainMs)
    StoreOptions(keySerde,
                 valSerde,
                 cachingEnabled = true,
                 loggingEnabled = true,
                 Map(),
                 storeSupplier)
  }
}
