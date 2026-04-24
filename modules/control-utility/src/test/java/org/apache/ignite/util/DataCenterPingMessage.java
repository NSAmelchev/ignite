package org.apache.ignite.util;

import java.util.LinkedHashMap;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

/** */
public class DataCenterPingMessage implements DiscoveryCustomMessage, Message {
    /** */
    @Order(0)
    IgniteUuid id = IgniteUuid.randomUuid();

    /** Node ID -> message process timestamp. */
    @Order(1)
    LinkedHashMap<UUID, Long> path = new LinkedHashMap<>();

    /** Empty constructor for {@link MessageFactory}. */
    public DataCenterPingMessage() {
        // No-op.
    }

    /** */
    public void addTime(UUID nodeId) {
        path.put(nodeId, System.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoverySpiCustomMessage ackMessage() {
        return new DataCenterPingAckMessage(path);
    }
}
