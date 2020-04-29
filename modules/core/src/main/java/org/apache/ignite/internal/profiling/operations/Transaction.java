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

/** Transaction. */
public class Transaction implements ProfilingOperation {
    /** Cache IDs. */
    private final int[] cacheIds;

    /** Start time. */
    private final long startTime;

    /** Duration. */
    private final long duration;

    /** {@code True} if commited. */
    private final boolean commit;

    /**
     * @param cacheIds Cache IDs.
     * @param startTime Start time.
     * @param duration Duration.
     * @param commit {@code True} if commited.
     */
    public Transaction(int[] cacheIds, long startTime, long duration, boolean commit) {
        this.cacheIds = cacheIds;
        this.startTime = startTime;
        this.duration = duration;
        this.commit = commit;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return OperationType.TRANSACTION;
    }

    /** @return Cache IDs. */
    public int[] cacheIds() {
        return cacheIds;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Duration. */
    public long duration() {
        return duration;
    }

    /** @return {@code True} if commited. */
    public boolean commit() {
        return commit;
    }
}
