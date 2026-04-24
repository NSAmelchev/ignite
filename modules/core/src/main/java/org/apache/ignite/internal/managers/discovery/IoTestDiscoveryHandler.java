package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

public class IoTestDiscoveryHandler {
    /** */
    private final static AtomicLong ID_GEN = new AtomicLong();

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ConcurrentHashMap<Long, IoTestDiscoveryFuture> ioTests = new ConcurrentHashMap<>();

    /** */
    public IoTestDiscoveryHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(IoTestDiscoveryMessage.class, (topVer, snd, msg) -> {
            msg.onProcessed(ctx.localNodeId());
        });

        ctx.discovery().setCustomEventListener(IoTestDiscoveryAckMessage.class, (topVer, snd, msg) -> {
            if (!U.isLocalNodeCoordinator(ctx.discovery()))
                return;

            IoTestDiscoveryFuture fut = ioTests.get(msg.testId());

            if (fut == null)
                LT.warn(log, "Failed to find IO test future [msg=" + msg + ']');
            else
                fut.onDone(msg);
        });
    }

    /**
     * @param payload Payload.
     * @return Response future.
     */
    public IgniteInternalFuture<IoTestDiscoveryAckMessage> sendIoDiscoveryTest(byte[] payload) {
        A.ensure(ctx.discovery().mutableCustomMessages(), "DiscoverySpi should support mutable custom messages.");
        A.ensure(U.isLocalNodeCoordinator(ctx.discovery()), "Should be executed on the coordinator node.");

        long id = ID_GEN.getAndIncrement();

        IoTestDiscoveryFuture fut = new IoTestDiscoveryFuture(id);

        ioTests.put(id, fut);

        try {
            IoTestDiscoveryMessage msg = new IoTestDiscoveryMessage(id, payload);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    public String runTest(long warmup, long duration, byte[] payload) {
        A.ensure(warmup >= 0, "warmup must be >= 0");
        A.ensure(duration >= 0, "duration must be >= 0");

        // Warmup.
        long start = U.currentTimeMillis();

        while (System.currentTimeMillis() - start < warmup) {
            try {
                sendIoDiscoveryTest(payload).get();
            }
            catch (Exception e) {
                throw new IgniteException("Failed to run IO test.", e);
            }
        }

        // Measure.
        List<IoTestDiscoveryAckMessage> res = new ArrayList<>();

        start = U.currentTimeMillis();

        while ((U.currentTimeMillis() - start < duration) || res.isEmpty()) {
            try {
                IoTestDiscoveryAckMessage msg = sendIoDiscoveryTest(payload).get();

                if (msg != null)
                    res.add(msg);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to run IO test.", e);
            }
        }

        return formatSummary(res);
    }

    /** */
    private String formatSummary(Collection<IoTestDiscoveryAckMessage> res) {
        List<Long> ringTimes = new ArrayList<>();
        Map<UUID, List<Long>> nodeQueues = new HashMap<>();
        Map<UUID, List<Long>> hopLatencies = new HashMap<>();

        for (IoTestDiscoveryAckMessage r : res) {
            long ringTime = TimeUnit.NANOSECONDS.toMillis(r.ackCreateTs - r.reqCreateTs);

            ringTimes.add(ringTime);

            var nodes = r.procTsMillis.keySet().toArray(new UUID[0]);
            var proc = r.procTsMillis.values().toArray(new Long[0]);

            List<Long> rcv = r.rcvTs;
            List<Long> snd = r.sndTs;

            int n = Math.min(Math.min(rcv.size(), snd.size()), nodes.length);

            for (int i = 0; i < n; i++) {
                long q = TimeUnit.NANOSECONDS.toMillis(snd.get(i) - rcv.get(i));

                nodeQueues.computeIfAbsent(nodes[i], k -> new ArrayList<>()).add(q);
            }

            for (int i = 1; i < proc.length; i++) {
                long delta = proc[i] - proc[i - 1];

                hopLatencies.computeIfAbsent(nodes[i - 1], k -> new ArrayList<>()).add(delta);
            }

            // Final hop: last node -> coordinator.
            if (proc.length > 0) {
                UUID lastNode = nodes[nodes.length - 1];
                long lastTs = proc[proc.length - 1];
                long delta = r.ackCreateTsMillis - lastTs;

                hopLatencies.computeIfAbsent(lastNode, k -> new ArrayList<>()).add(delta);
            }
        }

        ringTimes.sort(Long::compare);

        long avgRing = (long) ringTimes.stream().mapToLong(x -> x).average().orElse(0);
        long maxRing = ringTimes.get(ringTimes.size() - 1);
        long p95Ring = ringTimes.get(Math.max((int)(ringTimes.size() * 0.95) - 1, 0));

        StringBuilder sb = new StringBuilder();

        sb.append("IO Discovery Summary\n");
        sb.append("Runs: ").append(res.size()).append("\n");

        sb.append("Ring time (ms): avg=").append(avgRing)
            .append(", p95=").append(p95Ring)
            .append(", max=").append(maxRing).append("\n");

        sb.append("Node queue (ms):\n");

        UUID worstNode = null;
        long worstAvg = Long.MIN_VALUE;

        for (var e : nodeQueues.entrySet()) {
            List<Long> vals = e.getValue();

            long avg = (long) vals.stream().mapToLong(x -> x).average().orElse(0);
            long max = vals.stream().mapToLong(x -> x).max().orElse(0);

            if (avg > worstAvg) {
                worstAvg = avg;
                worstNode = e.getKey();
            }

            sb.append("  ").append(e.getKey())
                .append(": avg=").append(avg)
                .append(", max=").append(max)
                .append("\n");
        }

        if (worstNode != null) {
            sb.append("Hotspot node: ").append(worstNode)
                .append(" (avg queue ").append(worstAvg).append(" ms)\n");
        }

        sb.append("Hop latency (ms):\n");

        UUID worstHop = null;
        long worstHopAvg = Long.MIN_VALUE;

        for (var e : hopLatencies.entrySet()) {
            List<Long> vals = e.getValue();

            long avg = (long) vals.stream().mapToLong(x -> x).average().orElse(0);
            long max = vals.stream().mapToLong(x -> x).max().orElse(0);

            if (avg > worstHopAvg) {
                worstHopAvg = avg;
                worstHop = e.getKey();
            }

            sb.append("  ").append(e.getKey())
                .append(": avg=").append(avg)
                .append(", max=").append(max)
                .append("\n");
        }

        if (worstHop != null) {
            sb.append("Slowest hop: ").append(worstHop)
                .append(" (avg ").append(worstHopAvg).append(" ms)\n");
        }

        return sb.toString();
    }

    /** */
    private class IoTestDiscoveryFuture extends GridFutureAdapter<IoTestDiscoveryAckMessage> {
        /** */
        private final long id;

        /** @param id Test ID. */
        IoTestDiscoveryFuture(long id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(IoTestDiscoveryAckMessage res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                ioTests.remove(id);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IoTestDiscoveryFuture.class, this);
        }
    }
}
