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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests Mvcc coordinator change on client reconnect.
 */
public class CacheMvccClientReconnectTest extends GridCommonAbstractTest {
    /** */
    final CountDownLatch latch = new CountDownLatch(1);

    /** */
    final CountDownLatch reconnect = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains("client")) {
            cfg.setClientMode(true);

            Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    try {
                        // Wait for the discovery notifier worker processed client disconnection.
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        log.error("Unexpected exception.", e);

                        fail("Unexpected exception: " + e.getMessage());
                    }

                    return true;
                }
            }, new int[] {EVT_NODE_JOINED});

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    reconnect.countDown();

                    return true;
                }
            }, new int[] {EVT_CLIENT_NODE_RECONNECTED});

            cfg.setLocalEventListeners(lsnrs);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteEx client = startGrid("client");

        MvccProcessor coordProc = client.context().coordinators();

        // Creates the join event.
        startGrid(1);

        waitForRemoteNodes(ignite, 2);

        stopGrid(0);
        stopGrid(1);

        // Wait for the discovery notifier worker processed client disconnection.
        assertTrue("Failed to wait for client disconnected.",
            waitForCondition(() -> client.cluster().clientReconnectFuture() != null, 10_000));

        assertTrue(client.context().clientDisconnected());

        ignite = startGrid(0);

        // Wait for GridDiscoveryManager processed EVT_CLIENT_NODE_RECONNECTED
        // and starts to notify grid components by the onLocalJoin method.
        assertTrue(waitForCondition(() -> !client.context().clientDisconnected(), 10_000));

        // Continue processing events.
        latch.countDown();

        client.cluster().clientReconnectFuture().get();

        assertTrue("Failed to wait for client reconnect event.", reconnect.await(10, SECONDS));

        assertEquals(ignite.localNode().id(), coordProc.currentCoordinator().nodeId());

        awaitPartitionMapExchange();
    }
}