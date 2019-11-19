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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponse;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.managers.encryption.GridEncryptionManager.ENCRYPTION_KEY_PREFIX;
import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests master key change process.
 */
@SuppressWarnings("ThrowableNotThrown")
public class MasterKeyChangeTest extends AbstractEncryptionTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRecoveryKeysOnClusterRestart() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2).get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

        checkEncryptedCaches(grids.get1(), grids.get2());

        stopAllGrids();

        startTestGrids(false);

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

        checkEncryptedCaches(grid(GRID_0), grid(GRID_1));
    }

    /** @throws Exception If failed. */
    @Test
    public void testManualRecovery() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        stopGrid(GRID_1);

        grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2).get();

        assertEquals(MASTER_KEY_NAME_2, grids.get1().encryption().getMasterKeyName());

        System.setProperty(IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP, MASTER_KEY_NAME_2);

        try {
            IgniteEx ignite = startGrid(GRID_1);

            assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

            checkEncryptedCaches(grids.get1(), ignite);
        }
        finally {
            System.clearProperty(IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP);
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectCacheStartDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2);

        commSpi.waitForBlocked();

        assertThrowsWithCause(() -> {
            grids.get1().getOrCreateCache(new CacheConfiguration<>("newCache").setEncryptionEnabled(true));
        }, IgniteCheckedException.class);

        commSpi.stopBlock();

        fut.get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

        checkEncryptedCaches(grids.get1(), grids.get2());
    }

    /**
     * Checks that the cache start will be rejected if group keys generated before the master key change.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRejectCacheStartOnClientDuringRotation() throws Exception {
        IgniteEx srv = startGrid(GRID_0);

        IgniteEx client = startGrid(getConfiguration("client").setClientMode(true));

        srv.cluster().active(true);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(srv);

        commSpi.blockMessages((node, message) -> message instanceof GenerateEncryptionKeyResponse);

        String cacheName = "userCache";

        IgniteInternalFuture cacheStartFut = runAsync(() -> {
            client.getOrCreateCache(new CacheConfiguration<>(cacheName).setEncryptionEnabled(true));
        });

        commSpi.waitForBlocked();

        IgniteFuture<Void> fut = srv.encryption().changeMasterKey(MASTER_KEY_NAME_2);

        commSpi.stopBlock();

        assertThrowsWithCause(() -> cacheStartFut.get(), IgniteCheckedException.class);

        fut.get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

        srv.cache(cacheName);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectNodeJoinDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2);

        commSpi.waitForBlocked();

        assertThrowsWithCause(() -> startGrid(3), IgniteCheckedException.class);

        commSpi.stopBlock();

        fut.get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

        checkEncryptedCaches(grids.get1(), grids.get2());
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectMasterKeyChangeDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2);

        commSpi.waitForBlocked();

        // Stops block subsequent changes.
        commSpi.stopBlock(false, null, true, false);

        assertThrowsWithCause(() -> grids.get2().encryption().changeMasterKey(MASTER_KEY_NAME_3).get(),
            IgniteException.class);

        // Unblocks first change.
        commSpi.stopBlock();

        fut.get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));

        checkEncryptedCaches(grids.get1(), grids.get2());
    }

    /** @throws Exception If failed. */
    @Test
    public void testMasterKeyChangeOnInactiveAndReadonlyCluster() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);

        assertFalse(grid0.cluster().active());

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        assertThrowsWithCause(() -> grid0.encryption().changeMasterKey(MASTER_KEY_NAME_2).get(), IgniteException.class);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        grid0.cluster().active(true);

        grid0.cluster().readOnly(true);

        grid0.encryption().changeMasterKey(MASTER_KEY_NAME_2).get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));
    }

    /** @throws Exception If failed. */
    @Test
    public void testRecoveryFromWalWithCacheOperations() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        IgniteEx grid0 = grids.get1();

        grid0.cluster().active(true);

        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>(cacheName())
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setEncryptionEnabled(true);

        IgniteCache<Long, String> cache1 = grids.get2().createCache(ccfg);

        assertEquals(DEFAULT_MASTER_KEY_NAME, grid0.encryption().getMasterKeyName());

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)grid0.context()
            .cache().context().database();

        // Prevent checkpoints to recovery from WAL.
        dbMgr.enableCheckpoints(false).get();

        AtomicLong cnt = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture loadFut = runAsync(() -> {
            while (!stop.get()) {
                try (Transaction tx = grids.get2().transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache1.put(cnt.get(), String.valueOf(cnt.get()));

                    tx.commit();

                    cnt.incrementAndGet();
                }
            }
        });

        // Put some data before master key change.
        waitForCondition(() -> cnt.get() >= 20, 10_000);

        grid0.encryption().changeMasterKey(MASTER_KEY_NAME_2).get();

        MetaStorage metaStorage = grid0.context().cache().context().database().metaStorage();

        DynamicCacheDescriptor desc = grid0.context().cache().cacheDescriptor(cacheName());

        Serializable oldKey = metaStorage.read(ENCRYPTION_KEY_PREFIX + desc.groupId());

        assertNotNull(oldKey);

        dbMgr.checkpointReadLock();

        // Simulate cache key write error.
        metaStorage.write(ENCRYPTION_KEY_PREFIX + desc.groupId(), new byte[0]);

        dbMgr.checkpointReadUnlock();

        // Put some data after master key change.
        long oldCnt = cnt.get();

        waitForCondition(() -> cnt.get() >= oldCnt + 20, 10_000);

        stop.set(true);
        loadFut.get();

        stopGrid(GRID_0, true);

        IgniteEx grid = startGrid(GRID_0);

        assertEquals(MASTER_KEY_NAME_2, grid(GRID_0).encryption().getMasterKeyName());

        IgniteCache<Long, String> cache0 = grid.cache(cacheName());

        for (long i = 0; i < cnt.get(); i++) {
            assertEquals(String.valueOf(i), cache0.get(i));
            assertEquals(String.valueOf(i), cache1.get(i));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
