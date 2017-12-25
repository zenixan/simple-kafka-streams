package org.eu.fuzzy.kafka.streams.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import org.eu.fuzzy.kafka.streams.serialization.KeySerde;
import org.eu.fuzzy.kafka.streams.serialization.KeySerde$;
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde;
import org.eu.fuzzy.kafka.streams.serialization.ValueSerde$;

/**
 * This utility class is used to extract the serialization format.
 */
class InternalMaterialized<K, V> extends Materialized<K, V, KeyValueStore<Bytes, byte[]>> {

    InternalMaterialized(final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        super(materialized);
    }

    /**
     * Returns a serialization format for the record key.
     */
    KeySerde<K> keySerde() {
        return KeySerde$.MODULE$.apply(keySerde);
    }

    /**
     * Returns a serialization format for the record value.
     */
    ValueSerde<V> valueSerde() {
        return ValueSerde$.MODULE$.apply(valueSerde);
    }

}
