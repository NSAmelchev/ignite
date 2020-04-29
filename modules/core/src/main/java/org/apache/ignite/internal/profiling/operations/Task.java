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

/** Task. */
public class Task implements ProfilingOperation {
    /** Session id. */
    private final IgniteUuid sesId;

    /** Task name. */
    private final String taskName;

    /** Start time. */
    private final long startTime;

    /** Duration. */
    private final long duration;

    /** Affinity partition id. */
    private final int affPartId;

    /**
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    public Task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        this.sesId = sesId;
        this.taskName = taskName;
        this.startTime = startTime;
        this.duration = duration;
        this.affPartId = affPartId;
    }

    /** {@inheritDoc} */
    @Override public OperationType type() {
        return OperationType.TASK;
    }

    /** @return Session id. */
    public IgniteUuid sesId() {
        return sesId;
    }

    /** @return Task name. */
    public String taskName() {
        return taskName;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Duration. */
    public long duration() {
        return duration;
    }

    /** @return Affinity partition id. */
    public int affPartId() {
        return affPartId;
    }
}
