package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>State is not stored in this class but in the ChronicleMap instance that the {@link
 * MemoryMappedKeyedStateBackend} manages and checkpoints.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of values kept internally in state.
 */
public abstract class AbstractMemoryMappedState<K, N, V>
        implements InternalKvState<K, N, V>, State {

    /** Serializer for the namespace. */
    final TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    final TypeSerializer<V> valueSerializer;

    final TypeSerializer<K> keySerializer;
    /** The current namespace, which the next value methods will refer to. */
    private N currentNamespace;

    /** Backend that holds the actual RocksDB instance where we store state. */
    protected MemoryMappedKeyedStateBackend<K> backend;

    //    /** The column family of this particular instance of state. */
    //    protected ColumnFamilyHandle columnFamily;

    protected final V defaultValue;

    //    protected final WriteOptions writeOptions;

    protected final DataOutputSerializer dataOutputView;

    protected final DataInputDeserializer dataInputView;

    private final SerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

    /**
     * Creates a new Memory Map backed state.
     *
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    protected AbstractMemoryMappedState(
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            TypeSerializer<K> keySerializer,
            V defaultValue,
            MemoryMappedKeyedStateBackend<K> backend) {

        this.namespaceSerializer = namespaceSerializer;
        this.backend = backend;

        this.valueSerializer =
                Preconditions.checkNotNull(valueSerializer, "State value serializer");
        this.defaultValue = defaultValue;

        this.dataOutputView = new DataOutputSerializer(128);
        this.dataInputView = new DataInputDeserializer();
        this.sharedKeyNamespaceSerializer = backend.getSharedKeyBuilder();
        this.keySerializer = keySerializer;
    }

    // ------------------------------------------------------------------------
    @Override
    public void clear() {
        try {
            // Get the current Key
            byte[] serializedKeyAndNamespace =
                    sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(
                            currentNamespace, namespaceSerializer);
            Tuple2<K, N> keyAndNamespace =
                    KvStateSerializer.deserializeKeyAndNamespace(
                            serializedKeyAndNamespace, keySerializer, namespaceSerializer);
            K key = keyAndNamespace.f0;
            String stateName = backend.stateToStateName.get(this);

            // Update Data Structures
            backend.namespaceAndStateNameToKeys
                    .get(new Tuple2<N, String>(currentNamespace, stateName))
                    .remove(key);
            backend.namespaceKeyStateNameToState.remove(
                    new Tuple3<N, String, K>(currentNamespace, stateName, key));
            backend.stateNamesToKeysAndNamespaces
                    .get(stateName)
                    .remove(new Tuple2<K, N>(key, currentNamespace));
        } catch (IOException e) {
            throw new NotImplementedException("TODO", e);
        }
    }

    public Tuple2<K, N> deserializeKeyAndNamespace(byte[] serializedKeyAndNamespace)
            throws Exception {
        return KvStateSerializer.deserializeKeyAndNamespace(
                serializedKeyAndNamespace, keySerializer, namespaceSerializer);
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
    }

    public N getCurrentNamespace() {
        return currentNamespace;
    }

    public SerializedCompositeKeyBuilder<K> getSharedKeyNamespaceSerializer() {
        return sharedKeyNamespaceSerializer;
    }

    public V getDefaultValue() {
        return defaultValue;
    }

    byte[] serializeCurrentKeyWithGroupAndNamespace() {
        return sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(
                currentNamespace, namespaceSerializer);
    }

    public byte[] serializeValue(V value) throws Exception {
        return serializeValue(value, valueSerializer);
    }

    public byte[] serializeValue(V value, TypeSerializer<V> safeValueSerializer) throws Exception {
        dataOutputView.clear();
        safeValueSerializer.serialize(value, dataOutputView);
        return dataOutputView.getCopyOfBuffer();
    }

    byte[] serializeCurrentNamespace() throws Exception {
        return serializeNamespace(currentNamespace, namespaceSerializer);
    }

    public byte[] serializeNamespace(N namespace, TypeSerializer<N> safeValueSerializer)
            throws Exception {
        dataOutputView.clear();
        safeValueSerializer.serialize(namespace, dataOutputView);
        return dataOutputView.getCopyOfBuffer();
    }

    public Tuple2<byte[], String> getNamespaceKeyStateNameTuple() throws Exception {
        byte[] serializedKeyAndNamespace =
                getSharedKeyNamespaceSerializer()
                        .buildCompositeKeyNamespace(getCurrentNamespace(), namespaceSerializer);
        return new Tuple2<byte[], String>(serializedKeyAndNamespace, getStateName());
    }

    public String getStateName() {
        return backend.stateToStateName.get(this);
    }

    byte[] getKeyBytes() {
        return serializeCurrentKeyWithGroupAndNamespace();
    }
}
