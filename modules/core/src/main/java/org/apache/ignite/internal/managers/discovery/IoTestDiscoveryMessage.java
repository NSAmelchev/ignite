package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

/**
 * IO test message flow:
 * <p>
 *     <b>Send request</b>
 * </p>
 * <p>{@link #reqCreateTs} Request create timestamp (JVM time).
 * <p>
 *     (queue time)
 * </p>
 * <p>{@link #reqSndTs} Request send timestamp (JVM time).
 * <p>{@link #reqSndTsMillis} Request send timestamp (System time).
 * <p>
 *     (network latency, sending to remote node)
 * <p>
 */
public class IoTestDiscoveryMessage implements DiscoveryCustomMessage, MarshallableMessage {
    /** */
    @Order(0)
    IgniteUuid id;

    /** */
    @Order(1)
    long testId;

    /** */
    @Order(2)
    byte[] payload;

    /** */
    @Order(3)
    long reqCreateTs;

    /** Node ID -> message process timestamp. */
    @Order(4)
    LinkedHashMap<UUID, Long> procTsMillis;

    /** */
    @Order(5)
    List<Long> rcvTs;

    /** */
    @Order(6)
    List<Long> sndTs;

    /** Empty constructor for {@link MessageFactory}. */
    public IoTestDiscoveryMessage() {
        // No-op.
    }

    /** */
    public IoTestDiscoveryMessage(long testId, byte[] payload) {
        id = IgniteUuid.randomUuid();
        this.testId = testId;
        this.payload = payload;
        reqCreateTs = System.nanoTime();
        procTsMillis = new LinkedHashMap<>();
        rcvTs = new ArrayList<>();
        sndTs = new ArrayList<>();
    }

    /** */
    public void onProcessed(UUID nodeId) {
        procTsMillis.put(nodeId, System.currentTimeMillis());
    }

    /**
     * This method is called to initialize tracing variables.
     * TODO: introduce direct message lifecycle API?
     */
    public void onAfterRead() {
        rcvTs.add(System.nanoTime());
    }

    /**
     * This method is called to initialize tracing variables.
     * TODO: introduce direct message lifecycle API?
     */
    public void onBeforeWrite() {
        sndTs.add(System.nanoTime());
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        onBeforeWrite();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        onAfterRead();
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
        return new IoTestDiscoveryAckMessage(this);
    }
}
