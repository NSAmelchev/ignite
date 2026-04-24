/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.communication;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor.newFixedThreadPool;

/** */
public class IoTestHandler {
    /** */
    private final static AtomicLong ID_GEN = new AtomicLong();

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ConcurrentHashMap<Long, IoTestFuture> ioTests = new ConcurrentHashMap<>();

    /** */
    public IoTestHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        log = ctx.log(getClass());

        ctx.io().addMessageListener(GridTopic.TOPIC_IO_TEST, (nodeId, msg, plc) -> {
            IgniteIoTestMessage msg0 = (IgniteIoTestMessage)msg;

            if (msg0.request()) {
                IgniteIoTestMessage res = new IgniteIoTestMessage(msg0);

                res.onRequestProcessed();

                try {
                    ctx.io().sendToGridTopic(nodeId, GridTopic.TOPIC_IO_TEST, res, GridIoPolicy.SYSTEM_POOL);
                }
                catch (Exception e) {
                    LT.warn(log, "Failed to send IO test response [nodeId=" + nodeId + "]", e);
                }
            }
            else {
                msg0.onResponseProcessed();

                IoTestFuture fut = ioTests.get(msg0.testId());

                if (fut == null)
                    LT.warn(log, "Failed to find IO test future [msg=" + msg0 + ']');
                else
                    fut.onDone(msg0);
            }
        });
    }

    /**
     * @param nodes Nodes.
     * @param payload Payload.
     * @param procFromNioThread If {@code true} message is processed from NIO thread.
     * @return Response future.
     */
    public GridCompoundFuture<IgniteIoTestMessage, Void> sendIoTest(
        List<ClusterNode> nodes,
        byte[] payload,
        boolean procFromNioThread
    ) {
        GridCompoundFuture<IgniteIoTestMessage, Void> resFut = new GridCompoundFuture<>();

        nodes.forEach(n -> resFut.add(sendIoTest(n, payload, procFromNioThread)));

        resFut.markInitialized();

        return resFut;
    }

    /**
     * @param node Node.
     * @param payload Payload.
     * @param procFromNioThread If {@code true} message is processed from NIO thread.
     * @return Response future.
     */
    public IgniteInternalFuture<IgniteIoTestMessage> sendIoTest(
        ClusterNode node,
        byte[] payload,
        boolean procFromNioThread
    ) {
        long id = ID_GEN.getAndIncrement();

        IoTestFuture fut = new IoTestFuture(id);

        ioTests.put(id, fut);

        try {
            IgniteIoTestMessage msg = new IgniteIoTestMessage(id, payload, procFromNioThread);

            ctx.io().sendToGridTopic(node, GridTopic.TOPIC_IO_TEST, msg, GridIoPolicy.SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param warmup Warmup duration in milliseconds.
     * @param duration Test duration in milliseconds.
     * @param threads Thread count.
     * @param latencyLimit Max latency in nanoseconds.
     * @param rangesCnt Ranges count in resulting histogram.
     * @param payLoadSize Payload size in bytes.
     * @param procFromNioThread {@code True} to process requests in NIO threads.
     * @param nodes Nodes participating in test.
     */
    public IgniteInternalFuture<String> runIoTest(
        final long warmup,
        final long duration,
        final int threads,
        final long latencyLimit,
        final int rangesCnt,
        final int payLoadSize,
        final boolean procFromNioThread,
        final List<ClusterNode> nodes
    ) {
        GridFutureAdapter<String> testRes = new GridFutureAdapter<>();

        ExecutorService svc = newFixedThreadPool("io-latency-inspector", ctx.igniteInstanceName(), threads + 1);

        final AtomicBoolean warmupFinished = new AtomicBoolean();
        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier bar = new CyclicBarrier(threads + 1);
        final LongAdder cnt = new LongAdder();
        final long sleepDuration = 5000;
        final byte[] payLoad = new byte[payLoadSize];
        final Map<UUID, IoTestThreadLocalNodeResults>[] res = new Map[threads];

        boolean failed = true;

        try {
            svc.execute(new Runnable() {
                @Override public void run() {
                    boolean failed = true;

                    try {
                        bar.await();

                        long start = System.currentTimeMillis();

                        if (log.isInfoEnabled())
                            log.info("IO test started " +
                                "[warmup=" + warmup +
                                ", duration=" + duration +
                                ", threads=" + threads +
                                ", latencyLimit=" + latencyLimit +
                                ", rangesCnt=" + rangesCnt +
                                ", payLoadSize=" + payLoadSize +
                                ", procFromNioThreads=" + procFromNioThread + ']'
                            );

                        for (;;) {
                            if (!warmupFinished.get() && System.currentTimeMillis() - start > warmup) {
                                if (log.isInfoEnabled())
                                    log.info("IO test warmup finished.");

                                warmupFinished.set(true);

                                start = System.currentTimeMillis();
                            }

                            if (warmupFinished.get() && System.currentTimeMillis() - start > duration) {
                                if (log.isInfoEnabled())
                                    log.info("IO test finished, will wait for all threads to finish.");

                                done.set(true);

                                bar.await();

                                failed = false;

                                break;
                            }

                            if (log.isInfoEnabled())
                                log.info("IO test [opsCnt/sec=" + (cnt.sumThenReset() * 1000 / sleepDuration) +
                                    ", warmup=" + !warmupFinished.get() +
                                    ", elapsed=" + (System.currentTimeMillis() - start) + ']');

                            Thread.sleep(sleepDuration);
                        }

                        // At this point all threads have finished the test and
                        // stored data to the resulting array of maps.
                        // Need to iterate it over and sum values for all threads.
                        testRes.onDone(printIoTestResults(res));
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        U.error(log, "IO test failed.", e);
                    }
                    finally {
                        if (failed)
                            bar.reset();
                    }
                }
            });

            for (int i = 0; i < threads; i++) {
                final int i0 = i;

                res[i] = U.newHashMap(nodes.size());

                svc.execute(new Runnable() {
                    @Override public void run() {
                        boolean failed = true;
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();
                        int size = nodes.size();
                        Map<UUID, IoTestThreadLocalNodeResults> res0 = res[i0];

                        try {
                            boolean warmupFinished0 = false;

                            bar.await();

                            for (;;) {
                                if (done.get())
                                    break;

                                if (!warmupFinished0)
                                    warmupFinished0 = warmupFinished.get();

                                ClusterNode node = nodes.get(rnd.nextInt(size));

                                IgniteIoTestMessage msg = sendIoTest(node, payLoad, procFromNioThread).get();

                                cnt.increment();

                                IoTestThreadLocalNodeResults nodeRes = res0.computeIfAbsent(node.id(),
                                    k -> new IoTestThreadLocalNodeResults(rangesCnt, latencyLimit));

                                nodeRes.onResult(msg);
                            }

                            bar.await();

                            failed = false;
                        }
                        catch (Exception e) {
                            U.error(log, "IO test worker thread failed.", e);
                        }
                        finally {
                            if (failed)
                                bar.reset();
                        }
                    }
                });
            }

            failed = false;
        }
        finally {
            if (failed)
                U.shutdownNow(GridIoManager.class, svc, log);
        }

        return testRes;
    }

    /**
     * @param rawRes Resulting map.
     */
    private String printIoTestResults(
        Map<UUID, IoTestThreadLocalNodeResults>[] rawRes
    ) {
        Map<UUID, IoTestNodeResults> res = new HashMap<>();

        for (Map<UUID, IoTestThreadLocalNodeResults> r : rawRes) {
            for (Map.Entry<UUID, IoTestThreadLocalNodeResults> e : r.entrySet()) {
                IoTestNodeResults r0 = res.get(e.getKey());

                if (r0 == null)
                    res.put(e.getKey(), r0 = new IoTestNodeResults());

                r0.add(e.getValue());
            }
        }

        StringBuilder b = new StringBuilder(U.nl())
            .append("IO test results (round-trip count per each latency bin).")
            .append(U.nl());

        for (Map.Entry<UUID, IoTestNodeResults> e : res.entrySet()) {
            ClusterNode node = ctx.discovery().node(e.getKey());

            long binLatencyMcs = e.getValue().binLatencyMcs();

            b.append("Node ID: ").append(e.getKey()).append(" (addrs=")
                .append(node != null ? node.addresses().toString() : "n/a")
                .append(", binLatency=").append(binLatencyMcs).append("mcs")
                .append(')').append(U.nl());

            b.append("Latency bin, mcs | Count exclusive | Percentage exclusive | " +
                "Count inclusive | Percentage inclusive ").append(U.nl());

            long[] nodeRes = e.getValue().resLatency;

            long sum = 0;

            for (int i = 0; i < nodeRes.length; i++)
                sum += nodeRes[i];

            long curSum = 0;

            for (int i = 0; i < nodeRes.length; i++) {
                curSum += nodeRes[i];

                if (i < nodeRes.length - 1)
                    b.append(String.format("<%11d mcs | %15d | %19.6f%% | %15d | %19.6f%%\n",
                        (i + 1) * binLatencyMcs,
                        nodeRes[i], (100.0 * nodeRes[i]) / sum,
                        curSum, (100.0 * curSum) / sum));
                else
                    b.append(String.format(">%11d mcs | %15d | %19.6f%% | %15d | %19.6f%%\n",
                        i * binLatencyMcs,
                        nodeRes[i], (100.0 * nodeRes[i]) / sum,
                        curSum, (100.0 * curSum) / sum));
            }

            b.append(U.nl()).append("Total latency (ns): ").append(U.nl())
                .append(String.format("%15d", e.getValue().totalLatency)).append(U.nl());

            b.append(U.nl()).append("Max latencies (ns):").append(U.nl());
            format(b, e.getValue().maxLatency);

            b.append(U.nl()).append("Max request send queue times (ns):").append(U.nl());
            format(b, e.getValue().maxReqSendQueueTime);

            b.append(U.nl()).append("Max request receive queue times (ns):").append(U.nl());
            format(b, e.getValue().maxReqRcvQueueTime);

            b.append(U.nl()).append("Max response send queue times (ns):").append(U.nl());
            format(b, e.getValue().maxResSendQueueTime);

            b.append(U.nl()).append("Max response receive queue times (ns):").append(U.nl());
            format(b, e.getValue().maxResRcvQueueTime);

            b.append(U.nl()).append("Max request wire times (millis):").append(U.nl());
            format(b, e.getValue().maxReqWireTimeMillis);

            b.append(U.nl()).append("Max response wire times (millis):").append(U.nl());
            format(b, e.getValue().maxResWireTimeMillis);

            b.append(U.nl());
        }

        return b.toString();
    }

    /**
     * @param b Builder.
     * @param pairs Pairs to format.
     */
    private static void format(StringBuilder b, Collection<IgnitePair<Long>> pairs) {
        for (IgnitePair<Long> p : pairs) {
            b.append(String.format("%15d", p.get1()))
                .append(" ")
                .append(IgniteUtils.DEBUG_DATE_FMT.format(Instant.ofEpochMilli(p.get2())))
                .append(U.nl());
        }
    }

    /** */
    private class IoTestFuture extends GridFutureAdapter<IgniteIoTestMessage> {
        /** */
        private final long id;

        /** @param id Test ID. */
        IoTestFuture(long id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(IgniteIoTestMessage res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                ioTests.remove(id);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IoTestFuture.class, this);
        }
    }

    /** */
    private static class IoTestThreadLocalNodeResults {
        /** */
        private final long[] resLatency;

        /** */
        private final int rangesCnt;

        /** */
        private long totalLatency;

        /** */
        private long maxLatency;

        /** */
        private long maxLatencyTs;

        /** */
        private long maxReqSendQueueTime;

        /** */
        private long maxReqSendQueueTimeTs;

        /** */
        private long maxReqRcvQueueTime;

        /** */
        private long maxReqRcvQueueTimeTs;

        /** */
        private long maxResSendQueueTime;

        /** */
        private long maxResSendQueueTimeTs;

        /** */
        private long maxResRcvQueueTime;

        /** */
        private long maxResRcvQueueTimeTs;

        /** */
        private long maxReqWireTimeMillis;

        /** */
        private long maxReqWireTimeTs;

        /** */
        private long maxResWireTimeMillis;

        /** */
        private long maxResWireTimeTs;

        /** */
        private final long latencyLimit;

        /**
         * @param rangesCnt Ranges count.
         * @param latencyLimit
         */
        public IoTestThreadLocalNodeResults(int rangesCnt, long latencyLimit) {
            this.rangesCnt = rangesCnt;
            this.latencyLimit = latencyLimit;

            resLatency = new long[rangesCnt + 1];
        }

        /** */
        public void onResult(IgniteIoTestMessage msg) {
            long now = System.currentTimeMillis();

            long latency = msg.responseProcessedTs() - msg.requestCreateTs();

            int idx = latency >= latencyLimit ?
                rangesCnt /* Timed out. */ :
                (int)Math.floor((1.0 * latency) / ((1.0 * latencyLimit) / rangesCnt));

            resLatency[idx]++;

            totalLatency += latency;

            if (maxLatency < latency) {
                maxLatency = latency;
                maxLatencyTs = now;
            }

            long reqSndQueueTime = msg.requestSendTs() - msg.requestCreateTs();

            if (maxReqSendQueueTime < reqSndQueueTime) {
                maxReqSendQueueTime = reqSndQueueTime;
                maxReqSendQueueTimeTs = now;
            }

            long reqRcvQueueTime = msg.requestProcessTs() - msg.requestReceiveTs();

            if (maxReqRcvQueueTime < reqRcvQueueTime) {
                maxReqRcvQueueTime = reqRcvQueueTime;
                maxReqRcvQueueTimeTs = now;
            }

            long resSndQueueTime = msg.responseSendTs() - msg.requestProcessTs();

            if (maxResSendQueueTime < resSndQueueTime) {
                maxResSendQueueTime = resSndQueueTime;
                maxResSendQueueTimeTs = now;
            }

            long resRcvQueueTime = msg.responseProcessedTs() - msg.responseReceiveTs();

            if (maxResRcvQueueTime < resRcvQueueTime) {
                maxResRcvQueueTime = resRcvQueueTime;
                maxResRcvQueueTimeTs = now;
            }

            long reqWireTimeMillis = msg.requestReceivedTsMillis() - msg.requestSendTsMillis();

            if (maxReqWireTimeMillis < reqWireTimeMillis) {
                maxReqWireTimeMillis = reqWireTimeMillis;
                maxReqWireTimeTs = now;
            }

            long resWireTimeMillis = msg.responseReceivedTsMillis() - msg.requestSendTsMillis();

            if (maxResWireTimeMillis < resWireTimeMillis) {
                maxResWireTimeMillis = resWireTimeMillis;
                maxResWireTimeTs = now;
            }
        }
    }

    /** */
    private static class IoTestNodeResults {
        /** */
        private long latencyLimit;

        /** */
        private long[] resLatency;

        /** */
        private long totalLatency;

        /** */
        private Collection<IgnitePair<Long>> maxLatency = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxReqSendQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxReqRcvQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxResSendQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxResRcvQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxReqWireTimeMillis = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxResWireTimeMillis = new ArrayList<>();

        /**
         * @param res Node results to add.
         */
        public void add(IoTestThreadLocalNodeResults res) {
            if (resLatency == null) {
                resLatency = res.resLatency.clone();
                latencyLimit = res.latencyLimit;
            }
            else {
                assert latencyLimit == res.latencyLimit;
                assert resLatency.length == res.resLatency.length;

                for (int i = 0; i < resLatency.length; i++)
                    resLatency[i] += res.resLatency[i];
            }

            totalLatency += res.totalLatency;

            maxLatency.add(F.pair(res.maxLatency, res.maxLatencyTs));
            maxReqSendQueueTime.add(F.pair(res.maxReqSendQueueTime, res.maxReqSendQueueTimeTs));
            maxReqRcvQueueTime.add(F.pair(res.maxReqRcvQueueTime, res.maxReqRcvQueueTimeTs));
            maxResSendQueueTime.add(F.pair(res.maxResSendQueueTime, res.maxResSendQueueTimeTs));
            maxResRcvQueueTime.add(F.pair(res.maxResRcvQueueTime, res.maxResRcvQueueTimeTs));
            maxReqWireTimeMillis.add(F.pair(res.maxReqWireTimeMillis, res.maxReqWireTimeTs));
            maxResWireTimeMillis.add(F.pair(res.maxResWireTimeMillis, res.maxResWireTimeTs));
        }

        /**
         * @return Bin latency in microseconds.
         */
        public long binLatencyMcs() {
            if (resLatency == null)
                throw new IllegalStateException();

            return latencyLimit / (1000 * (resLatency.length - 1));
        }
    }
}
