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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link MapState} implementation that stores state in a MemoryMappedFile.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the map state.
 * @param <UV> The type of the values in the map state.
 */
class MemoryMappedMapState<K, N, UK, UV> extends AbstractMemoryMappedState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {
    /** Serializer for the keys and values. */
    private final TypeSerializer<UK> userKeySerializer;

    private final TypeSerializer<UV> userValueSerializer;

    private LinkedHashMap<K, HashSet<UK>> keyToUserKeys;
    /**
     * Creates a new {@code MemoryMappedValueState}.
     *
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private MemoryMappedMapState(
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<Map<UK, UV>> valueSerializer,
            TypeSerializer<K> keySerializer,
            Map<UK, UV> defaultValue,
            MemoryMappedKeyedStateBackend<K> backend) {
        super(namespaceSerializer, valueSerializer, keySerializer, defaultValue, backend);
        Preconditions.checkState(
                valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

        MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
        this.userKeySerializer = castedMapSerializer.getKeySerializer();
        this.userValueSerializer = castedMapSerializer.getValueSerializer();
        this.keyToUserKeys = new LinkedHashMap<>();
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        throw new FlinkRuntimeException("Not needed for Benchmarking");
    }


    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            RegisteredKeyValueStateBackendMetaInfo<N, SV> registerResult,
            TypeSerializer<K> keySerializer,
            MemoryMappedKeyedStateBackend<K> backend) {
        return (IS)
                new MemoryMappedMapState<>(
                        registerResult.getNamespaceSerializer(),
                        (TypeSerializer<Map<UK, UV>>) registerResult.getStateSerializer(),
                        keySerializer,
                        (Map<UK, UV>) stateDesc.getDefaultValue(),
                        backend);
    }

    @Override
    public UV get(UK userkey) throws Exception {
        byte[] keyBytes = statekeyNamespaceUserkeyBytes(userkey);
        byte[] valueBytes = backend.namespaceKeyStatenameToValue.get(new Tuple2<byte[], String> (keyBytes, getStateName()));

        dataInputView.setBuffer(valueBytes);

        return userValueSerializer.deserialize(dataInputView);
    }

    @Override
    public void put(UK userkey, UV uservalue) throws Exception {

        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();

//        if (!backend.namespaceKeyStatenameToValue.containsKey(namespaceKeyStateNameTuple)) {
//            HashMap<UK, UV> entryMap = new HashMap<>();
//            entryMap.put(userkey, uservalue);
//            putValueInBackend(entryMap, namespaceKeyStateNameTuple);
//        } else {
//            byte[] serializedValue = backend.namespaceKeyStatenameToValue.get(namespaceKeyStateNameTuple);
//            dataInputView.setBuffer(serializedValue);
//            HashMap<UK, UV> entryMap = (HashMap<UK, UV>) valueSerializer.deserialize(dataInputView);
//
//            entryMap.put(userkey, uservalue);
//            putValueInBackend(entryMap, namespaceKeyStateNameTuple);
//        }

//        Put uservalue and userkey into the memory mapped file

        byte[] keyBytes = statekeyNamespaceUserkeyBytes(userkey);
        dataOutputView.clear();
        userValueSerializer.serialize(uservalue, dataOutputView);
        byte[] valueBytes = dataOutputView.getCopyOfBuffer();
        backend.namespaceKeyStatenameToValue.put(new Tuple2<byte[], String> (keyBytes, getStateName()), valueBytes);

//        Handle backend logic
        putCurrentStateKeyInBackend();

        backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
        backend.stateNamesToKeysAndNamespaces
                .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
                .add(namespaceKeyStateNameTuple.f0);

//       Keeping track of userkeys for each State Key
        K currentStateKey = backend.getCurrentKey();
        if (keyToUserKeys.containsKey(currentStateKey)) {
            keyToUserKeys.get(currentStateKey).add(userkey);
        } else {
            HashSet<UK> userkeySet = new HashSet<>();
            userkeySet.add(userkey);
            keyToUserKeys.put(currentStateKey, userkeySet);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        for (Map.Entry<UK, UV> entry: map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
//        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
//        putValueInBackend((HashMap<UK, UV>) map, namespaceKeyStateNameTuple);

    // Backend Logic for State Key and Namespace
//        putCurrentStateKeyInBackend();

//        backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
//        backend.stateNamesToKeysAndNamespaces
//                .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
//                .add(namespaceKeyStateNameTuple.f0);
////
////       Keeping track of userkeys for each State Key
//        K currentStateKey = backend.getCurrentKey();
//        if (keyToUserKeys.containsKey(currentStateKey)) {
//            for (Map.Entry<UK, UV> entry : map.entrySet()) {
//                keyToUserKeys.get(currentStateKey).add(entry.getKey());
//            }
//
//        } else {
//            HashSet<UK> userkeySet = new HashSet<>();
//            for (Map.Entry<UK, UV> entry : map.entrySet()) {
//                userkeySet.add(entry.getKey());
//            }
//            keyToUserKeys.put(currentStateKey, userkeySet);
//        }

    }

    @Override
    public void remove(UK userkey) throws Exception {
        if (!keyToUserKeys.containsKey(userkey)) {
            throw new RuntimeException("User key not found");
        }

        byte[] serializedBackendKey = statekeyNamespaceUserkeyBytes(userkey);
        backend.namespaceKeyStatenameToValue.remove(new Tuple2<> (serializedBackendKey, getStateName()));
    }

    @Override
    public boolean contains(UK userkey) throws Exception {
        K currentKey = backend.getCurrentKey();
        if (!keyToUserKeys.containsKey(currentKey)) {
           return false;
        }
        return keyToUserKeys.get(currentKey).contains(userkey);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        K currentKey = backend.getCurrentKey();
        if (keyToUserKeys.containsKey(currentKey)) {
            throw new FlinkRuntimeException("No entries for this key");
        }
        HashMap<UK, UV> iterable = new HashMap<>();
        for (UK userkey: keyToUserKeys.get(currentKey)) {
            UV uservalue = get(userkey);
            iterable.put(userkey, uservalue);
        }
        return iterable.entrySet();
    }
    public HashMap<UK, UV> getHashMap() throws Exception {
        K currentKey = backend.getCurrentKey();
        if (keyToUserKeys.containsKey(currentKey)) {
            throw new FlinkRuntimeException("No entries for this key");
        }
        HashMap<UK, UV> map = new HashMap<>();
        for (UK userkey: keyToUserKeys.get(currentKey)) {
            UV uservalue = get(userkey);
            map.put(userkey, uservalue);
        }
        return map;
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        K currentKey = backend.getCurrentKey();
        return keyToUserKeys.getOrDefault(currentKey, new HashSet<UK>());
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return getHashMap().values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return null;
    }

    @Override
    public boolean isEmpty() throws Exception {
        return false;
    }

    public byte[] statekeyNamespaceUserkeyBytes(UK userkey) throws IOException {
        return getSharedKeyNamespaceSerializer().buildCompositeKeyNamesSpaceUserKey(getCurrentNamespace(), namespaceSerializer, userkey, userKeySerializer);
    }

    public void putCurrentStateKeyInBackend() throws Exception {
        byte[] currentNamespace = serializeCurrentNamespace();
        Tuple2<ByteBuffer, String> tupleForKeys =
                new Tuple2(ByteBuffer.wrap(currentNamespace), getStateName());
        HashSet<K> keyHash =
                backend.namespaceAndStateNameToKeys.getOrDefault(
                        tupleForKeys, new HashSet<K>());
        keyHash.add(backend.getCurrentKey());
        backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);
    }

    public void putValueInBackend(HashMap<UK, UV> value, Tuple2<byte[], String> namespaceKeyStateNameTuple) throws Exception {
        dataOutputView.clear();
        valueSerializer.serialize(value, dataOutputView);
        byte[] serializedValue = dataOutputView.getCopyOfBuffer();

        backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple, serializedValue);
    }



}
