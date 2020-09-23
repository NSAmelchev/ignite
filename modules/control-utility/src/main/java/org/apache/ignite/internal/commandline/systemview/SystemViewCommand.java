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

package org.apache.ignite.internal.commandline.systemview;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleAttributeType;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTaskArg;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTaskResult;
import org.apache.ignite.spi.systemview.view.SystemView;

import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleAttributeType.DATE;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleAttributeType.NUMBER;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleAttributeType.STRING;

/** Represents command for {@link SystemView} content printing. */
public class SystemViewCommand implements Command<VisorSystemViewTaskArg> {
    /** Column separator. */
    public static final String COLUMN_SEPARATOR = "    ";

    /**
     * Argument for the system view content obtainig task.
     * @see VisorSystemViewTask
     */
    private VisorSystemViewTaskArg taskArg;

    /** ID of the node to get the system view content from. */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try {
            VisorSystemViewTaskResult res;

            try (GridClient client = Command.startClient(clientCfg)) {
                res = executeTaskByNameOnNode(
                    client,
                    VisorSystemViewTask.class.getName(),
                    taskArg,
                    nodeId,
                    clientCfg
                );
            }

            if (res != null)
                printSystemViewContent(res, log);
            else
                log.info("No system view with specified name was found [name=" + taskArg.systemViewName() + "]");

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * Prints system view content obtained via {@link VisorSystemViewTask} execution.
     *
     * @param taskRes Result of {@link VisorSystemViewTask} execution.
     * @param log Logger.
     */
    private void printSystemViewContent(VisorSystemViewTaskResult taskRes, Logger log) {
        List<String> titles = taskRes.attributes();
        List<List<?>> viewRows = taskRes.rows();

        List<Integer> colSzs = titles.stream().map(String::length).collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(viewRows.size());

        viewRows.forEach(sysViewRow -> {
            ListIterator<Integer> colSzIter = colSzs.listIterator();

            rows.add(sysViewRow.stream().map(attr -> {
                String val = String.valueOf(attr);

                colSzIter.set(Math.max(colSzIter.next(), val.length()));

                return val;
            }).collect(Collectors.toList()));
        });

        printRow(titles, nCopies(titles.size(), STRING), colSzs, log);

        rows.forEach(row -> printRow(row, taskRes.types(), colSzs, log));
    }

    /**
     * Prints row content with respect to column types and colSzs.
     *
     * @param row Row which content should be printed.
     * @param types Column types in sequential order for decent row formatting.
     * @param colSzs Column colSzs in sequential order for decent row formatting.
     * @param log Logger.
     */
    private void printRow(
        Collection<String> row,
        Collection<SimpleAttributeType> types,
        Collection<Integer> colSzs,
        Logger log
    ) {
        Iterator<SimpleAttributeType> typeIter = types.iterator();
        Iterator<Integer> lenIter = colSzs.iterator();

        log.info(row.stream().map(colVal -> {
            SimpleAttributeType colType = typeIter.next();

            int colLen = lenIter.next();

            String format = colType == DATE || colType == NUMBER ?
                "%" + colLen + "s" :
                "%-" + colLen + "s";

            return String.format(format, colVal);
        }).collect(Collectors.joining(COLUMN_SEPARATOR)));
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        nodeId = null;

        String sysViewName = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            SystemViewCommandArg cmdArg = CommandArgUtils.of(arg, SystemViewCommandArg.class);

            if (cmdArg == NODE_ID) {
                String nodeIdArg = argIter.nextArg(
                    "ID of the node from which system view content should be obtained is expected.");

                try {
                    nodeId = UUID.fromString(nodeIdArg);
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Failed to parse " + NODE_ID + " command argument." +
                        " String representation of \"java.util.UUID\" is exepected. For example:" +
                        " 123e4567-e89b-42d3-a456-556642440000", e);
                }
            }
            else {
                if (sysViewName != null)
                    throw new IllegalArgumentException("Multiple system view names are not supported.");

                sysViewName = arg;
            }
        }

        if (sysViewName == null) {
            throw new IllegalArgumentException(
                "The name of the system view for which its content should be printed is expected.");
        }

        taskArg = new VisorSystemViewTaskArg(sysViewName);
    }

    /** {@inheritDoc} */
    @Override public VisorSystemViewTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new HashMap<>();

        params.put("node_id", "ID of the node to get the system view from. If not set, random node will be chosen.");
        params.put("system_view_name", "Name of the system view which content should be printed." +
            " Both \"SQL\" and \"Java\" styles of system view name are supported" +
            " (e.g. SQL_TABLES and sql.tables will be handled similarly).");

        Command.usage(log, "Print system view content:", SYSTEM_VIEW, params, optional(NODE_ID, "node_id"),
            "system_view_name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SYSTEM_VIEW.toCommandName();
    }
}
