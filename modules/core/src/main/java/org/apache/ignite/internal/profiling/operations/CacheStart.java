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

import org.jetbrains.annotations.Nullable;

/** Cache start. */
public class CacheStart implements ProfilingOperation {
    /** Cache id. */
    private final int cacheId;

    /** Start time. */
    private final long startTime;

    /** Cache name. */
    private final String cacheName;

    /** Group name. */
    private final String groupName;

    /** User cache flag. */
    private final boolean userCache;

    /**
     * @param cacheId Cache id.
     * @param startTime Start time.
     * @param cacheName Cache name.
     * @param groupName Group name.
     * @param userCache User cache flag.
     */
    public CacheStart(int cacheId, long startTime, String cacheName, String groupName, boolean userCache) {
        this.cacheId = cacheId;
        this.startTime = startTime;
        this.cacheName = cacheName;
        this.groupName = groupName;
        this.userCache = userCache;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return OperationType.CACHE_START;
    }

    /** @return Cache id. */
    public int cacheId() {
        return cacheId;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Cache name. */
    public String cacheName() {
        return cacheName;
    }

    /** @return Group name. */
    @Nullable public String groupName() {
        return groupName;
    }

    /** @return User cache flag. */
    public boolean userCache() {
        return userCache;
    }
}
