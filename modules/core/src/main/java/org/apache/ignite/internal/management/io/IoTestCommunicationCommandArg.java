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

package org.apache.ignite.internal.management.io;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;

/** */
public class IoTestCommunicationCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Argument(description = "Node ID to run test from.")
    UUID nodeId;

    /** */
    @Order(1)
    @Argument(optional = true, description = "Warmup duration (millis).")
    long warmup = TimeUnit.SECONDS.toMillis(15);

    /** */
    @Order(2)
    @Argument(optional = true, description = "Test duration (millis).")
    long duration = TimeUnit.SECONDS.toMillis(30);

    /** */
    @Order(3)
    @Argument(optional = true, description = "Threads count.")
    int threads = 4;

    /** */
    @Order(4)
    @Argument(optional = true, description = "Maximum latency expected (nanos).")
    long maxLatency = TimeUnit.MILLISECONDS.toNanos(100);

    /** */
    @Order(5)
    @Argument(optional = true, description = "Ranges count for histogram.")
    int rangesCnt = 5;

    /** */
    @Order(6)
    @Argument(optional = true, description = "Payload size (bytes).")
    int payLoadSize = 100;

    /** */
    @Order(7)
    @Argument(optional = true, description = "Process requests in NIO-threads flag.")
    boolean procFromNioThread;

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public long warmup() {
        return warmup;
    }

    /** */
    public void warmup(long warmup) {
        this.warmup = warmup;
    }

    /** */
    public long duration() {
        return duration;
    }

    /** */
    public void duration(long duration) {
        this.duration = duration;
    }

    /** */
    public int threads() {
        return threads;
    }

    /** */
    public void threads(int threads) {
        this.threads = threads;
    }

    /** */
    public long maxLatency() {
        return maxLatency;
    }

    /** */
    public void maxLatency(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    /** */
    public int rangesCnt() {
        return rangesCnt;
    }

    /** */
    public void rangesCnt(int rangesCnt) {
        this.rangesCnt = rangesCnt;
    }

    /** */
    public int payLoadSize() {
        return payLoadSize;
    }

    /** */
    public void payLoadSize(int payLoadSize) {
        this.payLoadSize = payLoadSize;
    }

    /** */
    public boolean procFromNioThread() {
        return procFromNioThread;
    }

    /** */
    public void procFromNioThread(boolean procFromNioThread) {
        this.procFromNioThread = procFromNioThread;
    }
}
