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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;

/**
 * Tests strings caching.
 */
public class StringCacheTest extends AbstractPerformanceStatisticsTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheTaskName() throws Exception {
        IgniteEx ignite = startGrid(0);

        String testTaskName = "TestTask";
        int executions = 5;

        startCollectStatistics();

        for (int i = 0; i < executions; i++) {
            ignite.compute().withName(testTaskName).run(new IgniteRunnable() {
                @Override public void run() {
                    // No-op.
                }
            });
        }

        AtomicInteger tasks = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
                int affPartId) {
                if (testTaskName.equals(taskName))
                    tasks.incrementAndGet();
            }
        });

        assertEquals(executions, tasks.get());

        long expLen = taskRecordSize(testTaskName.getBytes().length, false) +
            taskRecordSize(0, true) * (executions - 1) +
            jobRecordSize() * executions +
            /*opType*/ 2 * executions;

        List<File> files = statisticsFiles();

        assertEquals(1, files.size());
        assertEquals(expLen, files.get(0).length());
    }

    /** @throws Exception If failed. */
    @Test
    public void testStringForwardRead() throws Exception {
        IgniteEx ignite = startGrid(0);

        String testTaskName = "TestTask-";
        int executions = 20;
        int threadCnt = 8;

        startCollectStatistics();

        CyclicBarrier barrier = new CyclicBarrier(threadCnt);

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < executions; i++) {
                try {
                    barrier.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (Exception ignored) {
                    // No-op.
                }

                ignite.compute().withName(testTaskName + i).run(new IgniteRunnable() {
                    @Override public void run() {
                        // No-op.
                    }
                });
            }
        }, threadCnt, "load");

        AtomicInteger tasks = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
                int affPartId) {
                assertNotNull(taskName);

                tasks.incrementAndGet();
            }
        });

        assertEquals(executions * threadCnt, tasks.get());
    }
}
