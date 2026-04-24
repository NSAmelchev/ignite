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
public class DataCenterPingAckMessage implements DiscoveryCustomMessage, Message {
    /** */
    @Order(0)
    IgniteUuid id = IgniteUuid.randomUuid();

    /** Node ID -> message process timestamp. */
    @Order(1)
    LinkedHashMap<UUID, Long> path;

    /** */
    @Order(2)
    long createTime;

    /** Empty constructor for {@link MessageFactory}. */
    public DataCenterPingAckMessage() {
        // No-op.
    }

    /** Empty constructor for {@link MessageFactory}. */
    public DataCenterPingAckMessage(LinkedHashMap<UUID, Long> path) {
        id = IgniteUuid.randomUuid();
        this.path = path;
        createTime = System.currentTimeMillis();
    }

    /** @return Message path over cluster with timestamps. */
    public LinkedHashMap<UUID, Long> path() {
        return path;
    }

    /** */
    public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoverySpiCustomMessage ackMessage() {
        return null;
    }
}
