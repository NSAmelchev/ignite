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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/** Tests Communication SPI test messages. */
public class IgniteIoTestMessagesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** */
    @Test
    public void testIoTestMessages() throws Exception {
        IgniteEx from = grid(0);
        IgniteEx target = grid(1);
        ClusterNode to = from.cluster().node(grid(1).localNode().id());
        byte[] payload = new byte[1024];

        for (int i = 0; i < payload.length; i++)
            payload[i] = (byte)(31 * i + 7);

        for (boolean procFromNioThread : new boolean[] {false, true}) {
            AtomicReference<String> threadName = new AtomicReference<>();
            CountDownLatch msgRcvd = new CountDownLatch(1);
            GridMessageListener lsnr = (nodeId, msg, plc) -> {
                if (msg instanceof IgniteIoTestMessage && ((IgniteIoTestMessage)msg).request()) {
                    threadName.set(Thread.currentThread().getName());
                    msgRcvd.countDown();
                }
            };

            target.context().io().addMessageListener(GridTopic.TOPIC_IO_TEST, lsnr);

            try {
                IgniteIoTestMessage res = from.context().io().ioTest()
                    .sendIoTest(to, payload, procFromNioThread)
                    .get();

                assertTrue(msgRcvd.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                assertFalse(res.request());
                assertEquals(procFromNioThread, res.processFromNioThread());
                assertArrayEquals(payload, res.payload);
                assertEquals(procFromNioThread, threadName.get().contains("grid-nio-worker-tcp-comm"));
                assertTrue(res.reqCreateTs != 0);
                assertTrue(res.reqSndTs != 0);
                assertTrue(res.reqSndTsMillis > 0);
                assertTrue(res.reqRcvTs != 0);
                assertTrue(res.reqRcvTsMillis > 0);
                assertTrue(res.reqProcTs != 0);
                assertTrue(res.resSndTs != 0);
                assertTrue(res.resSndTsMillis > 0);
                assertTrue(res.resRcvTs != 0);
                assertTrue(res.resRcvTsMillis > 0);
                assertTrue(res.resProcTs != 0);
                assertTrue(res.roundTripNanos() >= 0);
                assertTrue(res.requestSendQueueNanos() >= 0);
                assertTrue(res.requestReceiveQueueNanos() >= 0);
                assertTrue(res.responseSendQueueNanos() >= 0);
                assertTrue(res.responseReceiveQueueNanos() >= 0);
            }
            finally {
                assertTrue(target.context().io().removeMessageListener(GridTopic.TOPIC_IO_TEST, lsnr));
            }
        }

        IgniteIoTestMessage msg = new IgniteIoTestMessage();

        msg.reqSndTsMillis = 1_000;
        msg.reqRcvTsMillis = 2_500;
        msg.resSndTsMillis = 3_000;
        msg.resRcvTsMillis = 5_000;

        assertEquals(1_500, msg.requestWireTimeMillis());
        assertEquals(2_000, msg.responseWireTimeMillis());
    }

    /** */
    @Test
    public void testOnlyOneRunAndCancellation() throws Exception {
        IgniteEx from = grid(0);
        ClusterNode to = from.cluster().node(grid(1).localNode().id());
        IoTestHandler hnd = from.context().io().ioTest();
        IgniteInternalFuture<String> fut = hnd.runIoTest(
            0,
            TimeUnit.MINUTES.toMillis(1),
            1,
            TimeUnit.MILLISECONDS.toNanos(10),
            2,
            1,
            false,
            Collections.singletonList(to)
        );

        try {
            try {
                hnd.runIoTest(0, 10, 1, TimeUnit.MILLISECONDS.toNanos(10), 2, 1, false,
                    Collections.singletonList(to));

                fail("Concurrent IO test must be rejected.");
            }
            catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("already running"));
            }
        }
        finally {
            if (!fut.isDone())
                fut.cancel();
        }

        assertTrue(fut.isCancelled());
        for (int i = 0; i < 2; i++) {
            assertTrue(hnd.runIoTest(0, 10, 1, TimeUnit.MILLISECONDS.toNanos(10), 2, 1, false,
                Collections.singletonList(to)).get().contains("Communication SPI test"));
        }
    }
}
