/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

/** Runs a bounded latency test through the discovery ring. */
public class IoTestDiscoveryHandler {
    /** Cancellation polling interval. */
    private static final long CANCEL_POLL_INTERVAL_MILLIS = 100;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** Pending tests. */
    private final ConcurrentHashMap<IgniteUuid, IoTestDiscoveryFuture> ioTests = new ConcurrentHashMap<>();

    /** Ensures that only one test runs on the coordinator. */
    private final AtomicBoolean testRunning = new AtomicBoolean();

    /** @param ctx Kernal context. */
    public IoTestDiscoveryHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(IoTestDiscoveryMessage.class, (topVer, snd, msg) ->
            msg.onProcessed(ctx.localNodeId()));

        ctx.discovery().setCustomEventListener(IoTestDiscoveryAckMessage.class, (topVer, snd, msg) -> {
            if (!U.isLocalNodeCoordinator(ctx.discovery()))
                return;

            IoTestDiscoveryFuture fut = ioTests.get(msg.requestId());

            if (fut != null)
                fut.onAck(msg);
            else if (log.isDebugEnabled())
                log.debug("Ignoring unknown discovery IO test acknowledgement: " + msg.requestId());
        });
    }

    /**
     * @param samples Number of samples.
     * @param intervalMillis Interval between samples.
     * @param payloadSize Payload size.
     * @param cancelled Cancellation flag.
     * @return Test report.
     */
    public String runTest(int samples, long intervalMillis, int payloadSize, BooleanSupplier cancelled) {
        A.ensure(ctx.discovery().getInjectedDiscoverySpi() instanceof TcpDiscoverySpi,
            "Discovery IO test requires TcpDiscoverySpi.");
        A.ensure(ctx.discovery().aliveServerNodes().size() > 1,
            "Discovery IO test requires at least two server nodes.");
        A.notNull(cancelled, "cancelled");
        A.ensure(testRunning.compareAndSet(false, true), "Discovery IO test is already running.");

        try {
            byte[] payload = new byte[payloadSize];
            long topVer = ctx.discovery().topologyVersion();
            long timeout = ctx.config().getNetworkTimeout();
            List<Long> ringTimes = new ArrayList<>(samples);
            List<UUID> path = null;

            for (int i = 0; i < samples; i++) {
                ensureNotCancelled(cancelled);
                ensureTopology(topVer);

                IoTestDiscoveryFuture fut = send(payload);
                IoTestDiscoveryResult res;

                try {
                    res = await(fut, timeout, cancelled);
                }
                catch (IgniteCheckedException e) {
                    if (ctx.discovery().topologyVersion() != topVer)
                        throw new IgniteException("Topology changed during discovery IO test.", e);

                    throw new IgniteException("Discovery IO test sample timed out or failed.", e);
                }
                finally {
                    ioTests.remove(fut.requestId, fut);
                }

                ensureTopology(topVer);

                if (path == null)
                    path = new ArrayList<>(res.ack.path);
                else if (!path.equals(res.ack.path))
                    throw new IgniteException("Discovery ring path changed during the test.");

                ringTimes.add(res.ringTimeNanos);

                if (i + 1 < samples)
                    sleep(intervalMillis, cancelled);
            }

            return formatSummary(payloadSize, intervalMillis, ringTimes, path);
        }
        finally {
            testRunning.set(false);
        }
    }

    /** Sends one test message. */
    private IoTestDiscoveryFuture send(byte[] payload) {
        A.ensure(U.isLocalNodeCoordinator(ctx.discovery()), "Should be executed on the coordinator node.");

        IoTestDiscoveryMessage msg = new IoTestDiscoveryMessage(payload);
        IoTestDiscoveryFuture fut = new IoTestDiscoveryFuture(msg.id());

        ioTests.put(msg.id(), fut);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** Fails the test if topology changed. */
    private void ensureTopology(long topVer) {
        if (ctx.discovery().topologyVersion() != topVer)
            throw new IgniteException("Topology changed during discovery IO test.");
    }

    /** Waits for one sample while observing job cancellation. */
    private static IoTestDiscoveryResult await(
        IoTestDiscoveryFuture fut,
        long timeout,
        BooleanSupplier cancelled
    ) throws IgniteCheckedException {
        long startNanos = System.nanoTime();
        long remaining = Math.max(1, timeout);

        while (true) {
            ensureNotCancelled(cancelled);

            try {
                IoTestDiscoveryResult res = fut.get(Math.min(remaining, CANCEL_POLL_INTERVAL_MILLIS));

                ensureNotCancelled(cancelled);

                return res;
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                ensureNotCancelled(cancelled);

                remaining = timeout - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                if (remaining <= 0)
                    throw e;
            }
        }
    }

    /** Sleeps between samples while observing job cancellation. */
    private static void sleep(long millis, BooleanSupplier cancelled) {
        for (long remaining = millis; remaining > 0; ) {
            ensureNotCancelled(cancelled);

            long delay = Math.min(remaining, CANCEL_POLL_INTERVAL_MILLIS);

            try {
                U.sleep(delay);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Discovery IO test was interrupted.", e);
            }

            remaining -= delay;
        }
    }

    /** Fails the test if its management job was cancelled. */
    private static void ensureNotCancelled(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean())
            throw new IgniteException("Discovery IO test was cancelled.");
    }

    /** Formats a compact report. */
    private String formatSummary(int payloadSize, long intervalMillis, List<Long> ringTimes, List<UUID> path) {
        ringTimes.sort(Long::compare);

        StringBuilder sb = new StringBuilder();

        sb.append("TcpDiscoverySpi ring test\n");
        sb.append("Coordinator: ").append(ctx.localNodeId()).append('\n');
        sb.append("Samples: ").append(ringTimes.size()).append(" | Interval: ").append(intervalMillis).append(" ms\n");
        sb.append("Request payload: ").append(payloadSize).append(" bytes\n");
        sb.append("Request path: ");

        for (UUID nodeId : path)
            sb.append(nodeId).append(" -> ");

        sb.append(ctx.localNodeId()).append('\n');
        sb.append("Ring traversal (us): min=").append(toMicros(ringTimes.get(0)))
            .append(", p50=").append(toMicros(percentile(ringTimes, 50)))
            .append(", p95=").append(toMicros(percentile(ringTimes, 95)))
            .append(", max=").append(toMicros(ringTimes.get(ringTimes.size() - 1)))
            .append('\n');

        return sb.toString();
    }

    /** Returns the nearest-rank percentile. */
    private static long percentile(List<Long> sorted, int percentile) {
        int idx = (int)Math.ceil(sorted.size() * percentile / 100.0) - 1;

        return sorted.get(idx);
    }

    /** Converts nanoseconds to microseconds. */
    private static long toMicros(long nanos) {
        return TimeUnit.NANOSECONDS.toMicros(nanos);
    }

    /** Pending discovery test. */
    private class IoTestDiscoveryFuture extends GridFutureAdapter<IoTestDiscoveryResult> {
        /** Request ID. */
        private final IgniteUuid requestId;

        /** Local start timestamp. */
        private final long startNanos = System.nanoTime();

        /** @param requestId Request ID. */
        IoTestDiscoveryFuture(IgniteUuid requestId) {
            this.requestId = requestId;
        }

        /** Completes this future with an acknowledgement. */
        void onAck(IoTestDiscoveryAckMessage ack) {
            onDone(new IoTestDiscoveryResult(ack, System.nanoTime() - startNanos));
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(IoTestDiscoveryResult res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                ioTests.remove(requestId, this);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IoTestDiscoveryFuture.class, this);
        }
    }

    /** Result of one ring traversal. */
    private static class IoTestDiscoveryResult {
        /** Acknowledgement. */
        private final IoTestDiscoveryAckMessage ack;

        /** Ring traversal time. */
        private final long ringTimeNanos;

        /** */
        IoTestDiscoveryResult(IoTestDiscoveryAckMessage ack, long ringTimeNanos) {
            this.ack = ack;
            this.ringTimeNanos = ringTimeNanos;
        }
    }
}
