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
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

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
    private static Logger log = Logger.getLogger("memory mapped map state");
    /** Serializer for the keys and values. */
    private final TypeSerializer<UK> userKeySerializer;

    private final TypeSerializer<UV> userValueSerializer;

    //    private LinkedHashMap<K, HashSet<UK>> keyToUserKeys;
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
        //        this.keyToUserKeys = new LinkedHashMap<>();
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
            TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {
        // Boilerplate
        String stateName = backend.stateToStateName.get(this);
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        SerializedCompositeKeyBuilder<K> keyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);

        try {
            Map<UK, UV> value = getHashMap(keyAndNamespace.f0, keyAndNamespace.f1, keyBuilder);
            final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

            final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
            final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();

            return KvStateSerializer.serializeMap(
                    value.entrySet(), dupUserKeySerializer, dupUserValueSerializer);

        } catch (Exception e) {
            log.info(e.getMessage());
            if (getDefaultValue() == null) {
                return null;
            }
            dataOutputView.clear();
            safeValueSerializer.serialize(getDefaultValue(), dataOutputView);
            byte[] dv = dataOutputView.getCopyOfBuffer();
            return dv;
        }

        //
        //        throw new FlinkRuntimeException("Not needed for Benchmarking");
    }

    private Map<UK, UV> getHashMap(K key, N namespace, SerializedCompositeKeyBuilder<K> keyBuilder)
            throws Exception {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(key, backend.getNumberOfKeyGroups());
        keyBuilder.setKeyAndKeyGroup(key, keyGroup);

        //        if (!keyToUserKeys.containsKey(key)) {
        //            throw new FlinkRuntimeException("No entries for this key");
        //        }
        //        HashMap<UK, UV> map = new HashMap<>();
        //        for (UK userkey : keyToUserKeys.get(key)) {

        byte[] keyBytes = keyBuilder.buildCompositeKeyNamespace(namespace, namespaceSerializer);
        Tuple2<byte[], String> backendKey = new Tuple2<byte[], String>(keyBytes, getStateName());
        return getMapFromBackend(backendKey);
        //        if (!backend.namespaceKeyStatenameToValue.containsKey(backendKey)) {
        //            throw new FlinkRuntimeException("no consistency between key and key set");
        //        }
        //            byte[] valueBytes = backend.namespaceKeyStatenameToValue.get(backendKey);
        //
        //            dataInputView.setBuffer(valueBytes);
        //
        //            UV uservalue = userValueSerializer.deserialize(dataInputView);
        //            map.put(userkey, uservalue);
        //            log.info("key: " + userkey.toString() + " value: " + uservalue.toString());
        //        }
        //        log.info(map.toString());
        //        return map;
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
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        if (!backend.namespaceKeyStatenameToValue.containsKey(namespaceKeyStateNameTuple)) {
            return null;
        }
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);
        return map.get(userkey);

        //        byte[] keyBytes = statekeyNamespaceUserkeyBytes(userkey);
        //        Tuple2<byte[], String> backendKey = new Tuple2<byte[], String>(keyBytes,
        // getStateName());
        //        if (!backend.namespaceKeyStatenameToValue.containsKey(backendKey)) {
        //            return null;
        //        }
        //        byte[] valueBytes = backend.namespaceKeyStatenameToValue.get(backendKey);
        //
        //        dataInputView.setBuffer(valueBytes);
        //
        //        return userValueSerializer.deserialize(dataInputView);
    }

    @Override
    public void put(UK userkey, UV uservalue) throws Exception {

        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> entryMap;
        if (!backend.namespaceKeyStatenameToValue.containsKey(namespaceKeyStateNameTuple)) {
            entryMap = new HashMap<>();

        } else {
            entryMap = getMapFromBackend(namespaceKeyStateNameTuple);
        }
        entryMap.put(userkey, uservalue);
        putValueInBackend(entryMap, namespaceKeyStateNameTuple);

        //        Put uservalue and userkey into the memory mapped file

        //        byte[] keyBytes = statekeyNamespaceUserkeyBytes(userkey);
        //        dataOutputView.clear();
        //        userValueSerializer.serialize(uservalue, dataOutputView);
        //        byte[] valueBytes = dataOutputView.getCopyOfBuffer();
        //        backend.namespaceKeyStatenameToValue.put(
        //                new Tuple2<byte[], String>(keyBytes, getStateName()), valueBytes);

        //        Handle backend logic
        putCurrentStateKeyInBackend();

        backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);

        if (!backend.stateNamesToKeysAndNamespaces.containsKey(namespaceKeyStateNameTuple.f1)) {
            HashSet<byte[]> map = new HashSet<>();
            map.add(namespaceKeyStateNameTuple.f0);
            backend.stateNamesToKeysAndNamespaces.put(getStateName(), map);
        } else {
            backend.stateNamesToKeysAndNamespaces
                    .get(namespaceKeyStateNameTuple.f1)
                    .add(namespaceKeyStateNameTuple.f0);
        }

        //       Keeping track of userkeys for each State Key
        //        K currentStateKey = backend.getCurrentKey();
        //        if (keyToUserKeys.containsKey(currentStateKey)) {
        //            keyToUserKeys.get(currentStateKey).add(userkey);
        //        } else {
        //            HashSet<UK> userkeySet = new HashSet<>();
        //            userkeySet.add(userkey);
        //            keyToUserKeys.put(currentStateKey, userkeySet);
        //        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        //        for (Map.Entry<UK, UV> entry : map.entrySet()) {
        //            put(entry.getKey(), entry.getValue());
        //        }
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        try {
            Map<UK, UV> currentMap = getMapFromBackend(namespaceKeyStateNameTuple);

            currentMap.putAll(map);
            putValueInBackend(currentMap, namespaceKeyStateNameTuple);
        } catch (FlinkRuntimeException e) {
            putValueInBackend(map, namespaceKeyStateNameTuple);
        }

        //         Backend Logic for State Key and Namespace
        putCurrentStateKeyInBackend();

        backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
        if (!backend.stateNamesToKeysAndNamespaces.containsKey(namespaceKeyStateNameTuple.f1)) {
            HashSet<byte[]> m = new HashSet<>();
            m.add(namespaceKeyStateNameTuple.f0);
            backend.stateNamesToKeysAndNamespaces.put(getStateName(), m);
        } else {
            backend.stateNamesToKeysAndNamespaces
                    .get(namespaceKeyStateNameTuple.f1)
                    .add(namespaceKeyStateNameTuple.f0);
        }
        //
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
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);

        map.remove(userkey);

        putValueInBackend(map, namespaceKeyStateNameTuple);

        //        if (!keyToUserKeys.containsKey(userkey)) {
        //            throw new RuntimeException("User key not found");
        //        }
        //
        //        byte[] serializedBackendKey = statekeyNamespaceUserkeyBytes(userkey);
        //        backend.namespaceKeyStatenameToValue.remove(
        //                new Tuple2<>(serializedBackendKey, getStateName()));
    }

    @Override
    public boolean contains(UK userkey) throws Exception {
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);
        return map.containsKey(userkey);
        //        K currentKey = backend.getCurrentKey();
        //        if (!keyToUserKeys.containsKey(currentKey)) {
        //            return false;
        //        }
        //        return keyToUserKeys.get(currentKey).contains(userkey);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);
        return map.entrySet();
        //        K currentKey = backend.getCurrentKey();
        //        if (!keyToUserKeys.containsKey(currentKey)) {
        //            throw new FlinkRuntimeException("No entries for this key");
        //        }
        //        HashMap<UK, UV> iterable = new HashMap<>();
        //        for (UK userkey : keyToUserKeys.get(currentKey)) {
        //            UV uservalue = get(userkey);
        //            iterable.put(userkey, uservalue);
        //        }
        //        return iterable.entrySet();
    }

    //    public HashMap<UK, UV> getHashMap() throws Exception {
    //        K currentKey = backend.getCurrentKey();
    //        if (!keyToUserKeys.containsKey(currentKey)) {
    //            throw new FlinkRuntimeException("No entries for this key");
    //        }
    //        HashMap<UK, UV> map = new HashMap<>();
    //        for (UK userkey : keyToUserKeys.get(currentKey)) {
    //            UV uservalue = get(userkey);
    //            map.put(userkey, uservalue);
    //        }
    //        return map;
    //    }

    @Override
    public Iterable<UK> keys() throws Exception {
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);
        return map.keySet();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);
        return map.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        Map<UK, UV> map = getMapFromBackend(namespaceKeyStateNameTuple);
        return map.entrySet().iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        return false;
    }

    //    public byte[] statekeyNamespaceUserkeyBytes(UK userkey) throws IOException {
    //        return getSharedKeyNamespaceSerializer()
    //                .buildCompositeKeyNamesSpaceUserKey(
    //                        getCurrentNamespace(), namespaceSerializer, userkey,
    // userKeySerializer);
    //    }

    public void putCurrentStateKeyInBackend() throws Exception {
        byte[] currentNamespace = serializeCurrentNamespace();
        Tuple2<ByteBuffer, String> tupleForKeys =
                new Tuple2(ByteBuffer.wrap(currentNamespace), getStateName());
        HashSet<K> keyHash =
                backend.namespaceAndStateNameToKeys.getOrDefault(tupleForKeys, new HashSet<K>());
        keyHash.add(backend.getCurrentKey());
        backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);
    }

    public void putValueInBackend(
            Map<UK, UV> value, Tuple2<byte[], String> namespaceKeyStateNameTuple) throws Exception {
        dataOutputView.clear();
        valueSerializer.serialize(value, dataOutputView);
        byte[] serializedValue = dataOutputView.getCopyOfBuffer();

        backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple, serializedValue);
    }

    public Map<UK, UV> getMapFromBackend() throws Exception {
        Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
        return getMapFromBackend(namespaceKeyStateNameTuple);
    }

    public Map<UK, UV> getMapFromBackend(Tuple2<byte[], String> namespaceKeyStateNameTuple)
            throws Exception {
        if (!backend.namespaceKeyStatenameToValue.containsKey(namespaceKeyStateNameTuple)) {
            throw new FlinkRuntimeException("Backend doesn't contain key");
        }
        byte[] value = backend.namespaceKeyStatenameToValue.get(namespaceKeyStateNameTuple);
        dataInputView.setBuffer(value);
        return (HashMap<UK, UV>) valueSerializer.deserialize(dataInputView);
    }
}
