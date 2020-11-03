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

package org.apache.ignite.spi.tracing;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.tracing.TracingSpiType.PERFORMANCE_STATISTICS_TRACING_SPI;
import static org.apache.ignite.spi.tracing.PerformanceStatisticsTracingSpi.Span;
import static org.apache.ignite.spi.tracing.PerformanceStatisticsTracingSpi.Span.NOOP_INSTANCE;

/** */
@IgniteSpiMultipleInstancesSupport(value = true)
@IgniteSpiConsistencyChecked(optional = true)
public class PerformanceStatisticsTracingSpi extends IgniteSpiAdapter implements TracingSpi<Span> {
    /** Performance statistics writer. {@code Null} if collecting statistics disabled. */
    @Nullable private volatile FilePerformanceStatisticsWriter writer;

    /** Synchronization mutex for start/stop collecting performance statistics operations. */
    private final Object mux = new Object();

    /** {@inheritDoc} */
    @NotNull @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        if (SpanType.TX.spanName().equals(name))
            return new TxSpan();
        else if (SpanType.CACHE_GET.spanName().equals(name))
            return new CacheOpSpan(OperationType.CACHE_GET);

        return NOOP_INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable byte[] serializedSpan) throws Exception {
        // TODO disable serialization
        assert false;

        return NOOP_INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        // TODO disable serialization
        assert false;

        return new byte[0];
    }

    /** {@inheritDoc} */
    @Override public byte type() {
        return PERFORMANCE_STATISTICS_TRACING_SPI.index();
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        startWriter();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        stopWriter();
    }

    /** Starts performance statistics writer. */
    private void startWriter() {
        try {
            synchronized (mux) {
                if (writer != null)
                    return;

                writer = new FilePerformanceStatisticsWriter(((IgniteEx)ignite).context());

                writer.start();
            }

            log.info("Performance statistics writer started.");
        }
        catch (Exception e) {
            log.error("Failed to start performance statistics writer.", e);
        }
    }

    /** Stops performance statistics writer. */
    private void stopWriter() {
        synchronized (mux) {
            if (writer == null)
                return;

            FilePerformanceStatisticsWriter writer = this.writer;

            this.writer = null;

            writer.stop();
        }

        log.info("Performance statistics writer stopped.");
    }

    /** */
    static class Span implements SpiSpecificSpan {
        /** */
        static Span NOOP_INSTANCE = new Span();

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan addTag(String tagName, String tagVal) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan addLog(String logDesc) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan setStatus(SpanStatus spanStatus) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan end() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean isEnded() {
            return true;
        }
    }

    /** */
    private class TrackableSpan extends Span {
        /** Start time. */
        final long startTime;

        /** Start time in nanoseconds to measure duration. */
        final long startTimeNanos;

        /** Duration. */
        long duration;

        /** Flag indicates that span is ended. */
        volatile boolean ended;

        /** */
        TrackableSpan() {
            startTime = U.currentTimeMillis();
            startTimeNanos = System.nanoTime();
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan end() {
            ended = true;

            duration = System.nanoTime() - startTimeNanos;

            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean isEnded() {
            return ended;
        }
    }

    /** */
    private class CacheOpSpan extends TrackableSpan {
        /** */
        private final OperationType opType;

        /** */
        private int cacheId;

        /** */
        CacheOpSpan(OperationType opType) {
            this.opType = opType;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan addTag(String tagName, String tagVal) {
            if ("cacheId".equals(tagName))
                cacheId = Integer.parseInt(tagVal);

            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan end() {
            super.end();

            writer.cacheOperation(opType, cacheId, startTime, duration);

            return this;
        }
    }

    /** */
    private class TxSpan extends TrackableSpan {
        /** */
        private boolean commited;

        /** */
        private GridIntList cacheIds;

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan addTag(String tagName, String tagVal) {
            if ("commited".equals(tagName))
                commited = Boolean.parseBoolean(tagVal);

            if ("cacheIds".equals(tagName)) {
                String[] ids = tagVal.substring(1, tagVal.length() - 1).split(",");

                cacheIds = new GridIntList(ids.length);

                for (String id : ids)
                    cacheIds.add(Integer.parseInt(id));
            }

            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan end() {
            super.end();

            // TODO can be ended by children span
            if (cacheIds == null)
                return this;

            writer.transaction(cacheIds, startTime, duration, commited);

            return this;
        }
    }
}

