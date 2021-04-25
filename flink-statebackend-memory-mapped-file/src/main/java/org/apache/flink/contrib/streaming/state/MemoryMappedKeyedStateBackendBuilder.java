package org.apache.flink.contrib.streaming.state;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.*;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Builder class for {@link MemoryMappedKeyedStateBackend} which handles all necessary initializations
 * and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class MemoryMappedKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
    /** String that identifies the operator that owns this backend. */
    private final String operatorIdentifier;

//    private final EmbeddedRocksDBStateBackend.PriorityQueueStateType priorityQueueStateType;


//    /** Path where this configured instance stores its data directory. */
//    private final File instanceBasePath;

//    /** Path where this configured instance stores its RocksDB database. */
//    private final File instanceRocksDBPath;

    private final MetricGroup metricGroup;

    /** True if incremental checkpointing is enabled. */
    private boolean enableIncrementalCheckpointing;


    public MemoryMappedKeyedStateBackendBuilder(
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
//            File instanceBasePath,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            LocalRecoveryConfig localRecoveryConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            CloseableRegistry cancelStreamRegistry) {

        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);

        this.operatorIdentifier = operatorIdentifier;
//        this.instanceBasePath = instanceBasePath;
//        this.instanceRocksDBPath = new File(instanceBasePath, DB_INSTANCE_DIR_STRING);
        this.metricGroup = metricGroup;
        this.enableIncrementalCheckpointing = false;
    }


    @Override
    public MemoryMappedKeyedStateBackend<K> build() throws BackendBuildingException {
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo> kvStateInformation =
                new LinkedHashMap<>();
        LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new LinkedHashMap<>();

        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        PriorityQueueSetFactory priorityQueueFactory;
        SerializedCompositeKeyBuilder<K> sharedKeyBuilder;
        // Number of bytes required to prefix the key groups.
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);



        // ChronicleMaps
        ChronicleMap<Tuple2<byte[], String>, HashSet<K>> namespaceAndStateNameToKeys;

        ChronicleMap<Tuple2<byte[], String>, State> namespaceStateNameKeyToState;
        ChronicleMap<String, HashSet<byte[]>> stateNamesToKeysAndNamespaces;
        LinkedHashMap<State, String> stateToStateName;
        ChronicleMap<Tuple2<byte[], String>, byte[]> namespaceKeyStateNameToValue;
        ChronicleMap<String, State> stateNameToState;
        try {
            sharedKeyBuilder =
                    new SerializedCompositeKeyBuilder<>(
                            keySerializerProvider.currentSchemaSerializer(),
                            keyGroupPrefixBytes,
                            32);


            /*
             * Creating all the Chronicle Maps
             * */
            Tuple2<byte[], String> byteArrayStringTuple = new Tuple2<byte[], String>("Any String you want".getBytes(), "123");
            HashSet<K> keyHashSet = new HashSet<K>();
            byte[] byteArray = "Any String you want".getBytes();
            HashSet<byte[]> byteHashSet = new HashSet<byte[]>();


            // !!! Does this way of opening files work
             namespaceAndStateNameToKeys =
                    ChronicleMapBuilder.of((Class<Tuple2<byte[], String>>)byteArrayStringTuple.getClass(), (Class<HashSet<K>>)keyHashSet.getClass())
                            .name("name-and-state-name-to-keys-map")
                            .averageKey(byteArrayStringTuple)
                            .entries(50_000).createPersistedTo(new File(OS.getTarget() + "/namespaceAndStateNameToKeys.dat"));


            namespaceStateNameKeyToState =
                    ChronicleMapBuilder.of((Class<Tuple2<byte[], String>>)byteArrayStringTuple.getClass(), State.class)
                            .name("name-state_name-key-to-state-map")
                            .averageKey(byteArrayStringTuple)
                            .entries(50_000).createPersistedTo(new File(OS.getTarget() + "/namespaceStateNameKeyToState.dat"));

            stateNamesToKeysAndNamespaces =
                    ChronicleMapBuilder.of(String.class, (Class<HashSet<byte[]>>)byteHashSet.getClass())
                            .name("state_name-to-keys-and-namespaces-map")
                            .averageKey("Any String you want")
                            .entries(50_000).createPersistedTo(new File(OS.getTarget() + "/stateNamesToKeysAndNamespaces.dat"));


            stateToStateName = new LinkedHashMap<State, String>();

            namespaceKeyStateNameToValue = ChronicleMapBuilder.of((Class<Tuple2<byte[], String>>)byteArrayStringTuple.getClass(), byte[].class)
                    .name("name-and-state-to-keys-map")
                    .averageKey(byteArrayStringTuple)
                    .entries(50_000).createPersistedTo(new File(OS.getTarget() + "/namespaceKeyStateNameToValue.dat"));

            stateNameToState = ChronicleMapBuilder.of(String.class, State.class)
                    .name("state_name-to-state-map")
                    .averageKey("Any String you want")
                    .entries(50_000).createPersistedTo(new File(OS.getTarget() + "/stateNameToState.dat"));



        } catch (Throwable e) {
            // Do clean up
            IOUtils.closeQuietly(cancelStreamRegistryForBackend);
            kvStateInformation.clear();
//            try {
//                FileUtils.deleteDirectory(instanceBasePath);
//            } catch (Exception ex) {
//                logger.warn("Failed to delete base path for MemoryMappedFile: " + instanceBasePath, ex);
//            }
            // Log and rethrow
            if (e instanceof BackendBuildingException) {
                throw (BackendBuildingException) e;
            } else {
                String errMsg = "Caught unexpected exception.";
                logger.error(errMsg, e);
                throw new BackendBuildingException(errMsg, e);
            }
        }
        InternalKeyContext<K> keyContext =
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);
        logger.info("Finished building Memory Mapped keyed state-backend at {}.", OS.getTarget());
//        this.instanceBasePath,


        return new MemoryMappedKeyedStateBackend<>(
                this.userCodeClassLoader,
                this.kvStateRegistry,
                this.keySerializerProvider.currentSchemaSerializer(),
                this.executionConfig,
                this.ttlTimeProvider,
                kvStateInformation,
                latencyTrackingStateConfig,
//                keyGroupPrefixBytes,
                cancelStreamRegistryForBackend,
                this.keyGroupCompressionDecorator,
                sharedKeyBuilder,
//                priorityQueueFactory,
                keyContext,
                namespaceAndStateNameToKeys,
                namespaceStateNameKeyToState,
                stateNamesToKeysAndNamespaces,
                stateToStateName,
                namespaceKeyStateNameToValue,
                stateNameToState
                );
    }
}




