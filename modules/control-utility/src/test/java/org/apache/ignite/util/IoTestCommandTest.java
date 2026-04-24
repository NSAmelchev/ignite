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

package org.apache.ignite.util;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.io.IoTestCommand;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.SystemViewCommandTest.NODE_ID;

/**
 * Tests for the {@link IoTestCommand}.
 */
public class IoTestCommandTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testCommunication() throws Exception {
        IgniteEx srv = startGrids(3);

        executeCommand(EXIT_CODE_OK, "--io-test", "communication", NODE_ID, srv.localNode().id().toString());
    }

    /** */
    @Test
    public void testDiscovery() throws Exception {
        IgniteEx srv = startGrids(3);

        executeCommand(EXIT_CODE_OK, "--io-test", "discovery");
    }
}
