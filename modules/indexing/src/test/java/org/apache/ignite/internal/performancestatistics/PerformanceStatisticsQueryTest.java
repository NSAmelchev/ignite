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

package org.apache.ignite.internal.performancestatistics;

import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.util.regex.Pattern.compile;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/** Tests query performance statistics. */
public class PerformanceStatisticsQueryTest extends AbstractPerformanceStatisticsTest {
    /** Cache entry count. */
    private static final int ENTRY_COUNT = 100;

    /** Client. */
    private static IgniteEx client;

    /** Cache. */
    private static IgniteCache<Integer, Integer> cache;

    /** Log of grid0. */
    private static ListeningTestLogger log0;

    /** Log of grid1. */
    private static ListeningTestLogger log1;

    /** Log of client. */
    private static ListeningTestLogger clientLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        cfg.setGridLogger(testLog);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            log0 = testLog;

        if (getTestIgniteInstanceName(1).equals(igniteInstanceName))
            log1 = testLog;

        if ("client".equals(igniteInstanceName))
            clientLog = testLog;

        return cfg;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(2);

        client = startClientGrid("client");

        client.cluster().state(ACTIVE);

        cache = client.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class)
                    .setTableName(DEFAULT_CACHE_NAME)))
        );

        for (int i = 0; i < ENTRY_COUNT; i++)
            cache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        log0.clearListeners();
        log1.clearListeners();
        clientLog.clearListeners();
    }

    /** */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        String sql = "SELECT * FROM " + DEFAULT_CACHE_NAME;

        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setSchema(DEFAULT_CACHE_NAME);

        checkQuery(qry, sql);
    }

    /** */
    @Test
    public void testScanQuery() throws Exception {
        checkQuery(new ScanQuery<>(), DEFAULT_CACHE_NAME);
    }

    /** Check query. */
    private void checkQuery(Query<?> qry, String text) throws Exception {
        client.cluster().state(INACTIVE);
        client.cluster().state(ACTIVE);

        LogListener lsnr0 = readsListener(true, true);
        LogListener lsnr1 = readsListener(true, true);
        LogListener clientLsnr = LogListener.matches("query ").andMatches("text=" + text).times(1).build();

        log0.registerListener(lsnr0);
        log1.registerListener(lsnr1);
        clientLog.registerListener(clientLsnr);

        startCollectStatistics();

        int size = cache.query(qry).getAll().size();

        assertEquals(ENTRY_COUNT, size);

        stopCollectStatisticsAndCheck(lsnr0, lsnr1, clientLsnr);

        lsnr0 = readsListener(true, false);
        lsnr1 = readsListener(true, false);
        clientLsnr.reset();

        log0.registerListener(lsnr0);
        log1.registerListener(lsnr1);

        startCollectStatistics();

        size = cache.query(qry).getAll().size();

        assertEquals(ENTRY_COUNT, size);

        stopCollectStatisticsAndCheck(lsnr0, lsnr1, clientLsnr);
    }

    /** @return Log listener for given reads. */
    private LogListener readsListener(boolean hasLogicalReads, boolean hasPhysicalReads) {
        String logical = hasLogicalReads ? "[1-9]\\d*" : "0";

        String physical = hasPhysicalReads ? "[1-9]\\d*" : "0";

        return LogListener
            .matches("queryReads ")
            .andMatches(compile("logicalReads=" + logical + ", physicalReads=" + physical))
            .times(1)
            .build();
    }
}
