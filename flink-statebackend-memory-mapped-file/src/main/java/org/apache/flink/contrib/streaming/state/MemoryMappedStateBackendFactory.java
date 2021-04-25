package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/** A factory that creates an {@link MemoryMappedStateBackend} from a configuration. */
@PublicEvolving
public class MemoryMappedStateBackendFactory
        implements StateBackendFactory<MemoryMappedStateBackend> {
    @Override
    public MemoryMappedStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new MemoryMappedStateBackend().configure(config, classLoader);
    }
}
