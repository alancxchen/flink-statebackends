package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;

import net.openhft.chronicle.map.ChronicleMap;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nonnull;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.RunnableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** An {@link AbstractKeyedStateBackend} that stores its state in a Memory Mapped File. */
public class MemoryMappedKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
    private static Logger log = Logger.getLogger("memory mapped file");

    // Serialized (namespace) + String (StateName) -> HashSet of Keys
    public final LinkedHashMap<Tuple2<ByteBuffer, String>, HashSet<K>> namespaceAndStateNameToKeys;
    // Serialized (namespace+Key) + String(StateName) -> State
    public final LinkedHashMap<Tuple2<byte[], String>, State> namespaceKeyStateNameToState;

    // String(StateName) -> HashSet of Serialized (keys + namespaces)
    public final LinkedHashMap<String, HashSet<byte[]>> stateNamesToKeysAndNamespaces;

    // Serialized (namespace + Key) + StateName -> Serialized (Value)
    public final ChronicleMap<Tuple2<byte[], String>, byte[]> namespaceKeyStatenameToValue;

    // State -> StateName
    public LinkedHashMap<State, String> stateToStateName;

    // stateName + namespaceString -> State
    public LinkedHashMap<String, State> stateNameToState;

    //    RegisteredKeyValueStateBackendMetaInfo
    private final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo> kvStateInformation;

    private K currentKey;
    private int keyGroupPrefixBytes;
    private boolean disposed = false;
    /** The key serializer. */
    protected final TypeSerializer<K> keySerializer;

    SerializedCompositeKeyBuilder<K> sharedKeyBuilder;
    private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    ValueStateDescriptor.class,
                                    (StateFactory) MemoryMappedValueState::create),
                            Tuple2.of(
                                    MapStateDescriptor.class,
                                    (StateFactory) MemoryMappedMapState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private interface StateFactory {
        <K, N, NS, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                RegisteredKeyValueStateBackendMetaInfo<NS, SV> registerResult,
                TypeSerializer<K> keySerializer,
                MemoryMappedKeyedStateBackend<K> backend)
                throws Exception;
    }

    public MemoryMappedKeyedStateBackend(
            ClassLoader userCodeClassLoader,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo> kvStateInformation,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            InternalKeyContext<K> keyContext,
            LinkedHashMap<Tuple2<ByteBuffer, String>, HashSet<K>> namespaceAndStateNameToKeys,
            LinkedHashMap<Tuple2<byte[], String>, State> namespaceKeyStateNameToState,
            LinkedHashMap<String, HashSet<byte[]>> stateNamesToKeysAndNamespaces,
            LinkedHashMap<State, String> stateToStateName,
            ChronicleMap<Tuple2<byte[], String>, byte[]> namespaceKeyStatenameToValue,
            LinkedHashMap<String, State> stateNameToState) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.keySerializer = keySerializer;
        this.namespaceAndStateNameToKeys = namespaceAndStateNameToKeys;
        this.namespaceKeyStateNameToState = namespaceKeyStateNameToState;
        this.stateNamesToKeysAndNamespaces = stateNamesToKeysAndNamespaces;
        this.kvStateInformation = kvStateInformation;
        this.sharedKeyBuilder = sharedKeyBuilder;
        this.stateToStateName = stateToStateName;
        this.namespaceKeyStatenameToValue = namespaceKeyStatenameToValue;
        this.stateNameToState = stateNameToState;
    }

    /** @see KeyedStateBackend */
    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    //    public <N> getSerializedNamespaceFromState(String state, N namespace) {
    //        namespace.getClass().getTypeName();
    //    }
    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String stateName, N namespace) {
        //        throw new NotImplementedException("getKeys");
        State s = stateNameToState.get(stateName);
        //        log.info("I'm starting");
        try {
            //            log.info("trying");
            if (s instanceof AbstractMemoryMappedState) {
                AbstractMemoryMappedState<?, N, ?> astate =
                        ((AbstractMemoryMappedState<?, N, ?>) s);
                //                log.info("it is an abstract memory mapped state");
                //                fixed bug where it was serializing the wrong namespace (not the

                // argument given)
                byte[] serializedNamespace =
                        astate.serializeNamespace(namespace, astate.getNamespaceSerializer());
                //                Tuple2<byte[], String> dd= new Tuple2<byte[],
                // String>(serializedNamespace, stateName);
                //                dd.get
                //                log.info("got the namespace, " + namespace);
                HashSet<K> keys =
                        namespaceAndStateNameToKeys.get(
                                new Tuple2<ByteBuffer, String>(
                                        ByteBuffer.wrap(serializedNamespace), stateName));
                //                log.info("WHAT");
                //                if (keys == null) {
                //                    DataOutputSerializer dataOutputView = new
                // DataOutputSerializer(128);
                //                    DataInputDeserializer dataInputView = new
                // DataInputDeserializer();
                //                    for (Tuple2<byte[], String> k :
                // namespaceAndStateNameToKeys.keySet()) {
                //                        log.info("key");
                //                        log.info(k.f1);
                ////                        dataInputView.setBuffer();
                //                        dataInputView.setBuffer(k.f0);
                //                        N ns =
                // astate.getNamespaceSerializer().deserialize(dataInputView);
                //                        log.info(ns.toString());
                //                        log.info("end");
                //                    }
                //                }

                //                String d = "got the keys, length=" + keys.size();
                //                log.info(d);
                Spliterator<K> keySpliterator = keys.spliterator();
                //                log.info("got the spliterator");
                //                log.info(
                //                        "Key Hashset: for namespace "
                //                                + astate.getCurrentNamespace().toString()
                //                                + keys.toString());
                //                for (K k : keys) {
                //                    log.info(k.toString());
                //                }

                Stream<K> targetStream = StreamSupport.stream(keySpliterator, false);
                return targetStream;
            } else {
                throw new Exception("Shouldn't happen");
            }
        } catch (java.lang.Exception e) {
            log.info("Caught an exception in getKeys()");
            log.info(e.toString());
            HashSet<K> keys = new HashSet<>();
            return StreamSupport.stream(keys.spliterator(), false);
        }
    }

    //    Each State maps to one namespace
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String stateName) {
        throw new NotImplementedException("GetKeysAndNamespaces");
        //        HashSet<byte[]> serializedNamespaceAndKeyTuples =
        //                stateNamesToKeysAndNamespaces.get(stateName);
        //        State s = stateNameToState.get(stateName);
        //
        //        try {
        //            if (s instanceof AbstractMemoryMappedState) {
        //                AbstractMemoryMappedState<?, ?, ?> state = (AbstractMemoryMappedState<?,
        // ?, ?>) s;
        //                HashSet<Tuple2<K, N>> namespaceAndKeyTuples = new HashSet<>();
        //                for (byte[] b : serializedNamespaceAndKeyTuples) {
        //                    Tuple2<K, N> tup = (Tuple2<K, N>) state.deserializeKeyAndNamespace(b);
        //                    namespaceAndKeyTuples.add(tup);
        //                }
        //                Spliterator<Tuple2<K, N>> keySpliterator =
        // namespaceAndKeyTuples.spliterator();
        //
        //                Stream<Tuple2<K, N>> targetStream = StreamSupport.stream(keySpliterator,
        // false);
        //                return targetStream;
        //            } else {
        //                throw new Exception("Should not happen");
        //            }
        //        } catch (java.lang.Exception e) {
        //            HashSet<Tuple2<K, N>> namespaceAndKeyTuples = new HashSet<>();
        //            return StreamSupport.stream(namespaceAndKeyTuples.spliterator(), false);
        //        }
    }

    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        sharedKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
        //        namespaceAndStateNameToKeys.put(getCurrentKey(), )
    }

    //    TODO low priority
    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }
        super.dispose();
        //        namespaceAndStateNameToKeys.close();
        //        namespaceKeyStateNameToState.close();
        //        stateNamesToKeysAndNamespaces.close();
        this.disposed = true;
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        throw new NotImplementedException("TODO");
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        throw new NotImplementedException("TODO");
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new NotImplementedException("TODO");
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDesc.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }

        TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();
        RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        stateDesc.getType(),
                        stateDesc.getName(),
                        namespaceSerializer,
                        stateSerializer,
                        StateSnapshotTransformFactory.noTransform());
        kvStateInformation.put(stateDesc.getName(), newMetaInfo);

        RegisteredKeyValueStateBackendMetaInfo<N, SV> registerResult = newMetaInfo;

        IS is =
                (IS)
                        stateFactory.createState(
                                stateDesc,
                                registerResult,
                                keySerializer,
                                MemoryMappedKeyedStateBackend.this);
        stateToStateName.put(is, stateDesc.getName());
        stateNameToState.put(stateDesc.getName(), is);
        return is;
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {
        throw new NotImplementedException("TODO");
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        throw new NotImplementedException("TODO");
    }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int count = kvStateInformation.size();
        //        int count = 0;

        //        for (RegisteredKeyValueStateBackendMetaInfo metaInfo :
        // kvStateInformation.values()) {
        //            // TODO maybe filterOrTransform only for k/v states
        //            try (RocksIteratorWrapper rocksIterator =
        //                         RocksDBOperationUtils.getRocksIterator(
        //                                 db, metaInfo.columnFamilyHandle, readOptions)) {
        //                rocksIterator.seekToFirst();
        //
        //                while (rocksIterator.isValid()) {
        //                    count++;
        //                    rocksIterator.next();
        //                }
        //            }
        //        }

        return count;
    }

    SerializedCompositeKeyBuilder<K> getSharedKeyBuilder() {
        return sharedKeyBuilder;
    }
}
