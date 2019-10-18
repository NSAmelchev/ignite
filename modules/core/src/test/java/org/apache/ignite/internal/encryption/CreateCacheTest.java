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

package org.apache.ignite.internal.encryption;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

/**
 * Tests cache start with fake group key.
 */
public class CreateCacheTest extends AbstractEncryptionTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCommunicationSpi(new TestTcpCommunicationSpi());

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrid(GRID_0);
        startGrid(GRID_1);
        IgniteEx client = startGrid(getConfiguration("client").setClientMode(true));

        client.cluster().active(true);

        log.info("@@@ START CACHE");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setEncryptionEnabled(true));

        awaitPartitionMapExchange();
    }

    /** */
    private static class TestTcpCommunicationSpi extends TcpCommunicationSpi {
        @Override public void sendMessage(ClusterNode node, Message msg0,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg0 instanceof GridIoMessage) {
                Message msg = ((GridIoMessage)msg0).message();

                // Simulate fake key.
                if (msg instanceof GenerateEncryptionKeyResponse)
                    ((GenerateEncryptionKeyResponse)msg).encryptionKeys().forEach(Arrays::sort);
            }

            super.sendMessage(node, msg0, ackC);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        cleanPersistenceDir();
    }
}
