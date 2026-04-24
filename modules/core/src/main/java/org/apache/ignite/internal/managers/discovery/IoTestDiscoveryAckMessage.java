package org.apache.ignite.internal.managers.discovery;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

/** */
public class IoTestDiscoveryAckMessage implements DiscoveryCustomMessage {
    /** */
    @Order(0)
    IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    @Order(1)
    long testId;

    /** */
    @Order(2)
    long reqCreateTs;

    /** Node ID -> message process timestamp. */
    @Order(3)
    LinkedHashMap<UUID, Long> procTsMillis;

    /** */
    @Order(4)
    List<Long> rcvTs;

    /** */
    @Order(5)
    List<Long> sndTs;

    /** */
    @Order(6)
    long ackCreateTs;

    /** */
    @Order(7)
    long ackCreateTsMillis;

    /** Empty constructor for {@link MessageFactory}. */
    public IoTestDiscoveryAckMessage() {
        // No-op.
    }

    /** */
    public IoTestDiscoveryAckMessage(IoTestDiscoveryMessage msg) {
        id = IgniteUuid.randomUuid();
        this.testId = msg.testId;
        this.reqCreateTs = msg.reqCreateTs;
        this.procTsMillis = msg.procTsMillis;
        this.rcvTs = msg.rcvTs;
        this.sndTs = msg.sndTs;
        ackCreateTs = System.nanoTime();
        ackCreateTsMillis = System.currentTimeMillis();
    }

    /** */
    public long testId() {
        return testId;
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
