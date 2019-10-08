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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests master key change process with master key consistency problems.
 */
public class MasterKeyChangeConsistencyCheckTest extends AbstractEncryptionTest {
    /** */
    private AtomicBoolean simulateOtherDigest = new AtomicBoolean();

    /** */
    private AtomicBoolean simulateSetMasterKeyError = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        TestKeystoreEncryptionSpi encSpi = new TestKeystoreEncryptionSpi();

        encSpi.setKeyStorePath(keystorePath());
        encSpi.setKeyStorePassword(keystorePassword());

        cfg.setEncryptionSpi(encSpi);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectMasterKeyRotationWithKeyConsistencyProblems() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        simulateOtherDigest.set(true);

        assertTrue(checkMasterKeyId(MASTER_KEY_ID));

        assertThrowsAnyCause(log, () -> {
            grids.get1().encryption().changeMasterKey(MASTER_KEY_ID_2);

            return null;
        }, IgniteException.class, "Master key digest consistency check failed");

        assertTrue(checkMasterKeyId(MASTER_KEY_ID));

        simulateOtherDigest.set(false);

        simulateSetMasterKeyError.set(true);

        assertThrowsAnyCause(log, () -> {
            grids.get1().encryption().changeMasterKey(MASTER_KEY_ID_2);

            return null;
        }, IgniteSpiException.class, "Test error.");

        assertTrue(checkMasterKeyId(MASTER_KEY_ID));

        simulateSetMasterKeyError.set(false);

        grids.get2().encryption().changeMasterKey(MASTER_KEY_ID_2);

        assertTrue(checkMasterKeyId(MASTER_KEY_ID_2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        simulateOtherDigest.set(false);
        simulateSetMasterKeyError.set(false);
    }

    /** */
    private class TestKeystoreEncryptionSpi extends KeystoreEncryptionSpi {
        /** {@inheritDoc} */
        @Override public byte[] masterKeyDigest() {
            if (simulateOtherDigest.get() && ignite.name().equals(GRID_1))
                return new byte[0];

            return super.masterKeyDigest();
        }

        /** {@inheritDoc} */
        @Override public void setMasterKeyId(String masterKeyId) {
            if (simulateSetMasterKeyError.get() && ignite.name().equals(GRID_1) && masterKeyId.equals(MASTER_KEY_ID_2))
                throw new IgniteSpiException("Test error.");

            super.setMasterKeyId(masterKeyId);
        }
    }
}
