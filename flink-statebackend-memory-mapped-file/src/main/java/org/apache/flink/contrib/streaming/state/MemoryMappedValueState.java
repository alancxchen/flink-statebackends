/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalValueState;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

/**
 * {@link ValueState} implementation that stores state in a Memory Mapped File.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class MemoryMappedValueState<K, N, V> extends AbstractMemoryMappedState<K, N, V>
        implements InternalValueState<K, N, V> {
    private static Logger log = Logger.getLogger("mmf value state");
    private ChronicleMap<K, V> kvStore;
    private static int numKeyedStatesBuilt = 0;
    private boolean chronicleMapInitialized = false;
    private String className = "MemoryMappedValueState";
    /**
     * Creates a new {@code MemoryMappedValueState}.
     *
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private MemoryMappedValueState(
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            TypeSerializer<K> keySerializer,
            V defaultValue,
            MemoryMappedKeyedStateBackend<K> backend)
            throws IOException {

        super(namespaceSerializer, valueSerializer, keySerializer, defaultValue, backend);
    }

    public void setUpChronicleMap() throws IOException {
        this.kvStore = createChronicleMap();
    }

    private ChronicleMap<K, V> createChronicleMap() throws IOException {
        String[] filePrefixes = {
            "namespaceKeyStateNameToValue",
        };
        File[] files = createPersistedFiles(filePrefixes);

        numKeyedStatesBuilt += 1;
        N averageNamespace = (N) VoidNamespace.INSTANCE;
        ChronicleMapBuilder<K, V> cmapBuilder =
                ChronicleMapBuilder.of(
                                (Class<K>) backend.getCurrentKey().getClass(),
                                (Class<V>) valueSerializer.createInstance().getClass())
                        .name("key-and-namespace-to-values")
                        .entries(1_000_000);
        if (backend.getCurrentKey() instanceof Integer || backend.getCurrentKey() instanceof Long) {
            log.info("Key is an Int or Long");
            //            return ChronicleMapBuilder.of(
            //                            (Class<K>) backend.getCurrentKey().getClass(),
            //                            (Class<V>) valueSerializer.createInstance().getClass())
            //                    .name("key-and-namespace-to-values")
            //                    .entries(1_000_000)
            //                    .createPersistedTo(files[0]);
        } else {
            cmapBuilder.averageKeySize(64);
        }

        if (valueSerializer.createInstance() instanceof Integer
                || valueSerializer.createInstance() instanceof Long) {
            log.info("Value is an Int or Long");
        } else {
            cmapBuilder.averageValue(valueSerializer.createInstance());
        }
        return cmapBuilder.createPersistedTo(files[0]);
        //        return ChronicleMapBuilder.of(
        //                        (Class<K>) backend.getCurrentKey().getClass(),
        //                        (Class<V>) valueSerializer.createInstance().getClass())
        //                .name("key-and-namespace-to-values")
        //                .averageKeySize(64)
        //                .averageValue(valueSerializer.createInstance())
        //                .entries(1_000_000)
        //                .createPersistedTo(files[0]);
    }

    private File[] createPersistedFiles(String[] filePrefixes) throws IOException {
        File[] files = new File[filePrefixes.length];
        for (int i = 0; i < filePrefixes.length; i++) {
            files[i] =
                    new File(
                            OS.getTarget()
                                    + "/BackendChronicleMaps/"
                                    + this.className
                                    + "/"
                                    + filePrefixes[i]
                                    + "_"
                                    + Integer.toString(this.numKeyedStatesBuilt)
                                    + ".dat");

            files[i].getParentFile().mkdirs();
            files[i].delete();
            files[i].createNewFile();
        }
        return files;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    @Override
    public Set<K> getKeys(N n) {
        return kvStore.keySet();
    }

    @Override
    public V value() throws IOException {
        if (!this.chronicleMapInitialized) {
            setUpChronicleMap();
            this.chronicleMapInitialized = true;
        }
        K backendKey = backend.getCurrentKey();
        if (!kvStore.containsKey(backendKey)) {
            return defaultValue;
        }
        return this.kvStore.get(backendKey);
        //        try {
        //            byte[] valueBytes =
        //
        // backend.namespaceKeyStatenameToValue.get(getNamespaceKeyStateNameTuple());
        //            if (valueBytes == null) {
        //                return getDefaultValue();
        //            }
        //            dataInputView.setBuffer(valueBytes);
        //            return valueSerializer.deserialize(dataInputView);
        //        } catch (java.lang.Exception e) {
        //            throw new FlinkRuntimeException(
        //                    "Error while retrieving data from Memory Mapped File.", e);
        //        }
    }

    Tuple2<K, N> getBackendKey() {
        return new Tuple2<K, N>(backend.getCurrentKey(), getCurrentNamespace());
    }

    @Override
    public void update(V value) throws IOException {
        if (!this.chronicleMapInitialized) {
            setUpChronicleMap();
            this.chronicleMapInitialized = true;
        }
        if (value == null) {
            kvStore.remove(backend.getCurrentKey());
            return;
        }

        this.kvStore.put(backend.getCurrentKey(), value);

        //        try {
        //            byte[] serializedValue = serializeValue(value, valueSerializer);
        //            Tuple2<byte[], String> namespaceKeyStateNameTuple =
        // getNamespaceKeyStateNameTuple();
        //            backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple,
        // serializedValue);
        //
        //            //            Fixed bug where we were using the wrong tuple to update the keys
        //            byte[] currentNamespace = serializeCurrentNamespace();
        //
        //            Tuple2<ByteBuffer, String> tupleForKeys =
        //                    new Tuple2(ByteBuffer.wrap(currentNamespace), getStateName());
        //            HashSet<K> keyHash =
        //                    backend.namespaceAndStateNameToKeys.getOrDefault(
        //                            tupleForKeys, new HashSet<K>());
        //            keyHash.add(backend.getCurrentKey());
        //
        //            backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);
        //
        //            backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
        //            backend.stateNamesToKeysAndNamespaces
        //                    .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
        //                    .add(namespaceKeyStateNameTuple.f0);
        //        } catch (java.lang.Exception e) {
        //            throw new FlinkRuntimeException("Error while adding data to Memory Mapped
        // File", e);
        //        }
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception {

        String stateName = backend.stateToStateName.get(this);
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyAndNamespace.f0, backend.getNumberOfKeyGroups());
        SerializedCompositeKeyBuilder<K> keyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);
        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
        byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
        if (kvStore.containsKey(keyAndNamespace.f0)) {
            V value = kvStore.get(keyAndNamespace.f0);
            dataOutputView.clear();
            safeValueSerializer.serialize(value, dataOutputView);
            return dataOutputView.getCopyOfBuffer();
        }

        dataOutputView.clear();
        safeValueSerializer.serialize(getDefaultValue(), dataOutputView);
        byte[] defaultValue = dataOutputView.getCopyOfBuffer();
        return defaultValue;
        //
        //        byte[] value =
        //                backend.namespaceKeyStatenameToValue.getOrDefault(
        //                        new Tuple2<byte[], String>(key, stateName), defaultValue);

    }

    @SuppressWarnings("unchecked")
    public static <K, N, NS, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            RegisteredKeyValueStateBackendMetaInfo<NS, SV> registerResult,
            TypeSerializer<K> keySerializer,
            MemoryMappedKeyedStateBackend<K> backend)
            throws IOException {
        return (IS)
                new MemoryMappedValueState<>(
                        registerResult.getNamespaceSerializer(),
                        registerResult.getStateSerializer(),
                        keySerializer,
                        stateDesc.getDefaultValue(),
                        backend);
    }
}
