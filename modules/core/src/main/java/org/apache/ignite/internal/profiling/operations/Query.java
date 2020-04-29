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

package org.apache.ignite.internal.profiling.operations;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;

import static org.apache.ignite.internal.profiling.operations.ProfilingOperation.OperationType.QUERY;

/** Query. */
public class Query implements ProfilingOperation {
    /** Cache query type. */
    private final GridCacheQueryType type;

    /** Query text in case of SQL query. Cache name in case of SCAN query. */
    private final String text;

    /** Originating node id (as part of global query id). */
    private final UUID queryNodeId;

    /** Query id. */
    private final long id;

    /** Start time. */
    private final long startTime;

    /** Duration. */
    private final long duration;

    /** Success flag. */
    private final boolean success;

    /**
     * @param type Cache query type.
     * @param text Query text in case of SQL query. Cache name in case of SCAN query.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param startTime Start time.
     * @param duration Duration.
     * @param success Success flag.
     */
    public Query(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime, long duration,
        boolean success) {
        this.type = type;
        this.text = text;
        this.queryNodeId = queryNodeId;
        this.id = id;
        this.startTime = startTime;
        this.duration = duration;
        this.success = success;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return QUERY;
    }

    /** @return Cache query type. */
    public GridCacheQueryType queryType() {
        return type;
    }

    /** @return Query text in case of SQL query. Cache name in case of SCAN query. */
    public String text() {
        return text;
    }

    /** @return Originating node id (as part of global query id). */
    public UUID queryNodeId() {
        return queryNodeId;
    }

    /** @return Query id. */
    public long id() {
        return id;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Duration. */
    public long duration() {
        return duration;
    }

    /** @return Success flag. */
    public boolean success() {
        return success;
    }
}
