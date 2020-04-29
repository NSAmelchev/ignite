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

/** Query reads. */
public class QueryReads implements ProfilingOperation {
    /** Cache query type. */
    private final GridCacheQueryType type;

    /** Originating node id. */
    private final UUID queryNodeId;

    /** Query id. */
    private final long id;

    /** Number of logical reads. */
    private final long logicalReads;

    /** Number of physical reads. */
    private final long physicalReads;

    public QueryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads) {
        this.type = type;
        this.queryNodeId = queryNodeId;
        this.id = id;
        this.logicalReads = logicalReads;
        this.physicalReads = physicalReads;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return OperationType.QUERY_READS;
    }

    /** @return Cache query type. */
    public GridCacheQueryType queryType() {
        return type;
    }

    /** @return Originating node id. */
    public UUID queryNodeId() {
        return queryNodeId;
    }

    /** @return Query id. */
    public long id() {
        return id;
    }

    /** @return Number of logical reads. */
    public long logicalReads() {
        return logicalReads;
    }

    /** @return Number of physical reads. */
    public long physicalReads() {
        return physicalReads;
    }
}
