package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractManagedMemoryStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;

/** */
@PublicEvolving
public class MemoryMappedStateBackend extends AbstractManagedMemoryStateBackend
        implements ConfigurableStateBackend {

    /** Creates a new {@code MemoryMappedStateBackend} for storing local state. */
    public MemoryMappedStateBackend() {}

    /**
     * Private constructor that creates a re-configured copy of the state backend.
     *
     * @param original The state backend to re-configure.
     * @param config The configuration.
     * @param classLoader The class loader.
     */
    private MemoryMappedStateBackend(
            MemoryMappedStateBackend original, ReadableConfig config, ClassLoader classLoader) {}

    // ------------------------------------------------------------------------
    //  Reconfiguration
    // ------------------------------------------------------------------------

    /**
     * Creates a copy of this state backend that uses the values defined in the configuration for
     * fields where that were not yet specified in this state backend.
     *
     * @param config The configuration.
     * @param classLoader The class loader.
     * @return The re-configured variant of the state backend
     */
    @Override
    public MemoryMappedStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return new MemoryMappedStateBackend(this, config, classLoader);
    }

    // ------------------------------------------------------------------------
    //  State holding data structures
    // ------------------------------------------------------------------------

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {
        return createKeyedStateBackend(
                env,
                jobID,
                operatorIdentifier,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                kvStateRegistry,
                ttlTimeProvider,
                metricGroup,
                stateHandles,
                cancelStreamRegistry,
                1.0);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction)
            throws IOException {
        ExecutionConfig executionConfig = env.getExecutionConfig();
        LocalRecoveryConfig localRecoveryConfig =
                env.getTaskStateManager().createLocalRecoveryConfig();
        StreamCompressionDecorator keyGroupCompressionDecorator =
                getCompressionDecorator(executionConfig);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(metricGroup).build();

        MemoryMappedKeyedStateBackendBuilder<K> builder =
                new MemoryMappedKeyedStateBackendBuilder<>(
                        operatorIdentifier,
                        env.getUserCodeClassLoader().asClassLoader(),
                        kvStateRegistry,
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        executionConfig,
                        localRecoveryConfig,
                        ttlTimeProvider,
                        latencyTrackingStateConfig,
                        metricGroup,
                        stateHandles,
                        keyGroupCompressionDecorator,
                        cancelStreamRegistry);
        return builder.build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {

        // the default for MemoryMappedStateBackend; eventually there can be a operator state
        // backend based on
        // Memory Mapped Files, too.
        final boolean asyncSnapshots = true;
        return new DefaultOperatorStateBackendBuilder(
                        env.getUserCodeClassLoader().asClassLoader(),
                        env.getExecutionConfig(),
                        asyncSnapshots,
                        stateHandles,
                        cancelStreamRegistry)
                .build();
    }

    // ------------------------------------------------------------------------
    //  Parameters
    // ------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "MemoryMappedStateBackend{}";
    }
}
