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

/** Cache operation. */
public class CacheOperation implements ProfilingOperation {
    /** Operation type. */
    private final CacheOperationType type;

    /** Cache id. */
    private final int cacheId;

    /** Start time. */
    private final long startTime;

    /** Duration. */
    private final long duration;

    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time.
     * @param duration Duration.
     */
    public CacheOperation(CacheOperationType type, int cacheId, long startTime, long duration) {
        this.type = type;
        this.cacheId = cacheId;
        this.startTime = startTime;
        this.duration = duration;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return OperationType.CACHE_OPERATION;
    }

    /** @return Operation type. */
    public CacheOperationType operationType() {
        return type;
    }

    /** @return Cache id. */
    public int cacheId() {
        return cacheId;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Duration. */
    public long duration() {
        return duration;
    }

    /** Cache operation type. */
    public enum CacheOperationType {
        /** */
        GET,

        /** */
        PUT,

        /** */
        REMOVE,

        /** */
        GET_AND_PUT,

        /** */
        GET_AND_REMOVE,

        /** */
        INVOKE,

        /** */
        LOCK,

        /** */
        GET_ALL,

        /** */
        PUT_ALL,

        /** */
        REMOVE_ALL,

        /** */
        INVOKE_ALL
    }
}
