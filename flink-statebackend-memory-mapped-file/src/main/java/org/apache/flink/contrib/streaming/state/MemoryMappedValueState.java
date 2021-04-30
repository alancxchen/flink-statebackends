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
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashSet;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class MemoryMappedValueState<K, N, V> extends AbstractMemoryMappedState<K, N, V>
        implements InternalValueState<K, N, V> {

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
            MemoryMappedKeyedStateBackend<K> backend) {

        super(namespaceSerializer, valueSerializer, keySerializer, defaultValue, backend);
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
    public V value() {
        try {
            byte[] valueBytes =
                    backend.namespaceKeyStateNameToValue.get(getNamespaceKeyStateNameTuple());
            if (valueBytes == null) {
                return getDefaultValue();
            }
            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);
        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException(
                    "Error while retrieving data from Memory Mapped File.", e);
        }
    }

    @Override
    public void update(V value) {
        if (value == null) {
            clear();
            return;
        }
        try {
            byte[] serializedValue = serializeValue(value, valueSerializer);
            Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
            backend.namespaceKeyStateNameToValue.put(namespaceKeyStateNameTuple, serializedValue);

            //            backend.namespaceAndStateNameToKeys
            //                    .getOrDefault(namespaceKeyStateNameTuple, new HashSet<K>())
            //                    .add(getCurrentKey());
            backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
            backend.stateNamesToKeysAndNamespaces
                    .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
                    .add(namespaceKeyStateNameTuple.f0);
        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception {

        String stateName = backend.stateToStateName.get(this);
        V value = this.value();
        byte[] serializedValue = serializeValue(value, safeValueSerializer);
        return serializedValue;
    }

    //    public K getCurrentKey() throws Exception {
    //        //        See getSerializedValue for inspiration
    //        byte[] keyBytes = getSharedKeyNamespaceSerializer().getSerializedKeyBytes();
    //        dataInputView.setBuffer(keyBytes);
    //        return getKeySerializer().deserialize(dataInputView);
    //    }

    public String getStateName() {
        return backend.stateToStateName.get(this);
    }

    public Tuple2<byte[], String> getNamespaceKeyStateNameTuple() throws Exception {
        byte[] serializedKeyAndNamespace =
                getSharedKeyNamespaceSerializer()
                        .buildCompositeKeyNamespace(getCurrentNamespace(), namespaceSerializer);
        return new Tuple2<byte[], String>(serializedKeyAndNamespace, getStateName());
    }

    @SuppressWarnings("unchecked")
    public static <K, N, NS, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            RegisteredKeyValueStateBackendMetaInfo<NS, SV> registerResult,
            TypeSerializer<K> keySerializer,
            MemoryMappedKeyedStateBackend<K> backend) {
        return (IS)
                new MemoryMappedValueState<>(
                        registerResult.getNamespaceSerializer(),
                        registerResult.getStateSerializer(),
                        keySerializer,
                        stateDesc.getDefaultValue(),
                        backend);
    }
}
