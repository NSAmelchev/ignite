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

import org.apache.ignite.lang.IgniteUuid;

/** Job. */
public class Job implements ProfilingOperation {
    /** Session id. */
    private final IgniteUuid sesId;

    /** Time job spent on waiting queue. */
    private final long queuedTime;

    /** Start time. */
    private final long startTime;

    /** Job execution time. */
    private final long duration;

    /** {@code True} if job is timed out. */
    private final boolean isTimedOut;

    /**
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time.
     * @param duration Job execution time.
     * @param isTimedOut {@code True} if job is timed out.
     */
    public Job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean isTimedOut) {
        this.sesId = sesId;
        this.queuedTime = queuedTime;
        this.startTime = startTime;
        this.duration = duration;
        this.isTimedOut = isTimedOut;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return OperationType.JOB;
    }

    /** @return Session id. */
    public IgniteUuid sesId() {
        return sesId;
    }

    /** @return Time job spent on waiting queue. */
    public long queuedTime() {
        return queuedTime;
    }

    /** @return Start time. */
    public long dtartTime() {
        return startTime;
    }

    /** @return Job execution time. */
    public long duration() {
        return duration;
    }

    /** @return {@code True} if job is timed out. */
    public boolean timedOut() {
        return isTimedOut;
    }
}
