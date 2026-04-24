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

package org.apache.ignite.internal.management.io;

import java.util.ArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/** */
public class IoTestCommunicationTask extends VisorOneNodeTask<IoTestCommunicationCommandArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<IoTestCommunicationCommandArg, String> job(IoTestCommunicationCommandArg arg) {
        return new IoTestJob(arg, false);
    }

    /** */
    private static class IoTestJob extends VisorJob<IoTestCommunicationCommandArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected IoTestJob(IoTestCommunicationCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(IoTestCommunicationCommandArg arg) throws IgniteException {
            try {
                return ignite.context().io().ioTest().runIoTest(
                    arg.warmup(),
                    arg.duration(),
                    arg.threads(),
                    arg.maxLatency(),
                    arg.rangesCnt(),
                    arg.payLoadSize(),
                    arg.procFromNioThread(),
                    new ArrayList<>(ignite.cluster().forServers().forRemotes().nodes())
                ).get();
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
