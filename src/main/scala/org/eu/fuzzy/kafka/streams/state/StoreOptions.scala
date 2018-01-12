package org.eu.fuzzy.kafka.streams.state

import scala.collection.JavaConverters.mapAsJavaMap

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.StoreSupplier
import org.apache.kafka.streams.state.{
  KeyValueBytesStoreSupplier,
  SessionBytesStoreSupplier,
  WindowBytesStoreSupplier
}

import org.eu.fuzzy.kafka.streams.KTopic
import org.eu.fuzzy.kafka.streams.serialization.Serde

/**
 * Represents a set of options for data materializing to the local state store.
 *
 * @tparam K  a type of record key
 * @tparam V  a type of record value
 * @tparam S  a type of state store
 *
 * @param keySerde  a serialization format for the record key
 * @param valSerde  a serialization format for the record value
 * @param cachingEnabled  a flag to indicate whether or not to cache records
 * @param loggingEnabled  a flag to indicate whether or not to create a changelog topic for the store
 * @param topicConfig  a set of settings that should be applied to the changelog topic
 * @param storeSupplier  a provider of state store
 */
final case class StoreOptions[K, V, S <: StateStore](keySerde: Serde[K],
                                                     valSerde: Serde[V],
                                                     cachingEnabled: Boolean,
                                                     loggingEnabled: Boolean,
                                                     topicConfig: Map[String, String],
                                                     storeSupplier: StoreSupplier[S]) {

  /** Converts this set of options to the anonymous topic. */
  @inline def toTopic: KTopic[K, V] = KTopic(keySerde, valSerde)

  /**
   * Converts this set of options to the [[org.apache.kafka.streams.kstream.Materialized]] instance.
   */
  def toMaterialized: Materialized[K, V, S] = storeSupplier match {
    case supplier: KeyValueBytesStoreSupplier =>
      applySettings(Materialized.as[K, V](supplier).asInstanceOf[Materialized[K, V, S]])

    case supplier: WindowBytesStoreSupplier =>
      applySettings(Materialized.as[K, V](supplier).asInstanceOf[Materialized[K, V, S]])

    case supplier: SessionBytesStoreSupplier =>
      applySettings(Materialized.as[K, V](supplier).asInstanceOf[Materialized[K, V, S]])

    case _ => applySettings(Materialized.`with`(keySerde, valSerde))
  }

  private def applySettings(materialized: Materialized[K, V, S]): Materialized[K, V, S] = {
    materialized.withKeySerde(keySerde).withValueSerde(valSerde)
    if (cachingEnabled) materialized.withCachingEnabled() else materialized.withCachingDisabled()
    if (loggingEnabled) {
      materialized.withLoggingEnabled(mapAsJavaMap(topicConfig))
    } else {
      materialized.withLoggingDisabled()
    }
  }
}
