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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;

/** Tests query performance statistics. */
@RunWith(Parameterized.class)
public class PerformanceStatisticsQueryTest extends AbstractPerformanceStatisticsTest {
    /** Cache entry count. */
    private static final int ENTRY_COUNT = 100;

    /** Test cache 2 name. */
    private static final String CACHE_2 = "cache2";

    /** Test SQL table name. */
    private static final String SQL_TABLE = "test";

    /** Page size. */
    @Parameterized.Parameter
    public int pageSize;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "pageSize={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{ENTRY_COUNT}, {ENTRY_COUNT / 10}});
    }

    /** Client. */
    private static IgniteEx client;

    /** Cache. */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(2);

        client = startClientGrid("client");

        client.cluster().state(ACTIVE);

        cache = client.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setSqlSchema(DFLT_SCHEMA)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class)
                    .setTableName(DEFAULT_CACHE_NAME)))
        );

        IgniteCache<Object, Object> cache2 = client.getOrCreateCache(new CacheConfiguration<>()
            .setName(CACHE_2)
            .setSqlSchema(DFLT_SCHEMA)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Long.class, Long.class)
                    .setTableName(CACHE_2)))
        );

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, i);
            cache2.put(i, i * 2);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cache.query(new SqlFieldsQuery("drop table if exists " + SQL_TABLE));
    }

    /** @throws Exception If failed. */
    @Test
    public void testScanQuery() throws Exception {
        ScanQuery<Object, Object> qry = new ScanQuery<>().setPageSize(pageSize);

        checkQuery(SCAN, qry, DEFAULT_CACHE_NAME);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        String sql = "select * from " + DEFAULT_CACHE_NAME;

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setPageSize(pageSize);

        checkQuery(SQL_FIELDS, qry, sql);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsJoinQuery() throws Exception {
        String sql = "select * from " + DEFAULT_CACHE_NAME + " a inner join " + CACHE_2 + " b on a._key = b._key";

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setPageSize(pageSize);

        checkQuery(SQL_FIELDS, qry, sql);
    }

    /** Check query. */
    private void checkQuery(GridCacheQueryType type, Query<?> qry, String text) throws Exception {
        client.cluster().state(INACTIVE);
        client.cluster().state(ACTIVE);

        runQueryAndCheck(type, qry, text, true, true);

        runQueryAndCheck(type, qry, text, true, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testDdlAndDmlQueries() throws Exception {
        String sql = "create table " + SQL_TABLE + " (id int, val varchar, primary key (id))";

        runQueryAndCheck(SQL_FIELDS, new SqlFieldsQuery(sql), sql, false, false);

        sql = "insert into " + SQL_TABLE + " (id) values (1)";

        runQueryAndCheck(SQL_FIELDS, new SqlFieldsQuery(sql), sql, false, false);

        sql = "update " + SQL_TABLE + " set val = 'abc'";

        runQueryAndCheck(SQL_FIELDS, new SqlFieldsQuery(sql), sql, true, false);
    }

    /** Runs query and checks statistics. */
    private void runQueryAndCheck(GridCacheQueryType expType, Query<?> qry, String expText, boolean hasLogicalReads,
        boolean hasPhysicalReads)
        throws Exception {
        startCollectStatistics();

        cache.query(qry).getAll();

        Set<UUID> readsNodes = new HashSet<>();

        if (hasLogicalReads)
            client.cluster().forServers().nodes().forEach(node -> readsNodes.add(node.id()));

        AtomicInteger queryCnt = new AtomicInteger();
        AtomicInteger readsCnt = new AtomicInteger();
        HashSet<Long> qryIds = new HashSet<>();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
                long duration, boolean success) {
                queryCnt.incrementAndGet();
                qryIds.add(id);

                assertEquals(client.localNode().id(), nodeId);
                assertEquals(expType, type);
                assertEquals(expText, text);
                assertTrue(startTime > 0);
                assertTrue(duration >= 0);
                assertTrue(success);
            }

            @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
                long logicalReads, long physicalReads) {
                readsCnt.incrementAndGet();
                qryIds.add(id);
                readsNodes.remove(nodeId);

                assertEquals(expType, type);
                assertEquals(client.localNode().id(), queryNodeId);
                assertTrue(logicalReads > 0);
                assertTrue(hasPhysicalReads ? physicalReads > 0 : physicalReads == 0);
            }
        });

        assertEquals(1, queryCnt.get());
        assertTrue("Query reads expected on nodes: " + readsNodes, readsNodes.isEmpty());
        assertEquals(1, qryIds.size());
    }

    /** @throws Exception If failed. */
    @Test
    public void testMultipleStatementsSql() throws Exception {
        LinkedList<String> expQrs = new LinkedList<>();

        expQrs.add("create table " + SQL_TABLE + " (id int primary key, val varchar)");
        expQrs.add("insert into " + SQL_TABLE + " (id, val) values (1, 'a')");
        expQrs.add("insert into " + SQL_TABLE + " (id, val) values (2, 'b'), (3, 'c')");

        LinkedList<String> qrsWithReads = new LinkedList<>();

        qrsWithReads.add("select * from " + SQL_TABLE);
        qrsWithReads.add("update " + SQL_TABLE + " set val = 'd' where id = 1");

        expQrs.addAll(qrsWithReads);

        startCollectStatistics();

        SqlFieldsQuery qry = new SqlFieldsQuery(F.concat(expQrs, ";"));

        List<FieldsQueryCursor<List<?>>> res = client.context().query().querySqlFields(qry, true, false);

        assertEquals("Unexpected cursors count: " + res.size(), expQrs.size(), res.size());

        res.get(3).getAll();

        HashSet<Long> qryIds = new HashSet<>();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
                long duration, boolean success) {
                if (qrsWithReads.contains(text))
                    qryIds.add(id);

                assertEquals(client.localNode().id(), nodeId);
                assertEquals(SQL_FIELDS, type);
                assertTrue("Unexpected query: " + text, expQrs.remove(text));
                assertTrue(startTime > 0);
                assertTrue(duration >= 0);
                assertTrue(success);
            }

            @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
                long logicalReads, long physicalReads) {
                qryIds.add(id);

                assertEquals(SQL_FIELDS, type);
                assertEquals(client.localNode().id(), queryNodeId);
                assertTrue(logicalReads > 0);
                assertEquals(0, physicalReads);
            }
        });

        assertTrue("Queries was not handled: " + expQrs, expQrs.isEmpty());
        assertEquals("Unexpected IDs: " + qryIds, qrsWithReads.size(), qryIds.size());
    }
}
