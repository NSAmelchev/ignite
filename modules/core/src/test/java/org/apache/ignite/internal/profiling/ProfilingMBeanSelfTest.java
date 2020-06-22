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

package org.apache.ignite.internal.profiling;

import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** Tests {@link IgniteProfilingMbeanImpl}. */
public class ProfilingMBeanSelfTest extends AbstractProfilingTest {
    /** @throws Exception If failed. */
    @Test
    public void testStartStop() throws Exception {
        IgniteEx srv0 = startGrid(0);
        IgniteEx srv1 = startGrid(1);

        IgniteProfilingMBean profilingMBean0 = getMBean(srv0.name());
        IgniteProfilingMBean profilingMBean1 = getMBean(srv1.name());

        assertFalse(profilingMBean0.enabled());
        assertFalse(profilingMBean1.enabled());

        profilingMBean0.start();

        assertTrue(profilingMBean0.enabled());
        assertTrue(profilingMBean1.enabled());

        profilingMBean0.stop();

        assertFalse(profilingMBean0.enabled());
        assertFalse(profilingMBean1.enabled());
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Ignite profiling MBean.
     */
    private IgniteProfilingMBean getMBean(String igniteInstanceName) {
        return getMxBean(igniteInstanceName, "Profiling", IgniteProfilingMbeanImpl.class,
            IgniteProfilingMBean.class);
    }
}
