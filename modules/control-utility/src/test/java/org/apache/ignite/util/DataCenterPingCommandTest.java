package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;

public class DataCenterPingCommandTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(
                new TestPluginProvider(),
                new MessagesPluginProvider(DataCenterPingMessage.class, DataCenterPingAckMessage.class)
            );
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrids(4);

        TestPluginProvider provider = (TestPluginProvider)srv.context().pluginProvider("test");

        provider.ping();
    }

    /** */
    public class TestPluginProvider extends AbstractTestPluginProvider {
        /** */
        IgniteEx srv;

        /** */
        volatile List<DataCenterPingAckMessage> curReq;

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
            srv = (IgniteEx)ctx.grid();

            srv.context().discovery().setCustomEventListener(DataCenterPingMessage.class, (topVer, snd, msg) -> {
                doSleep(30);
                msg.addTime(srv.cluster().localNode().id());
            });

            srv.context().discovery().setCustomEventListener(DataCenterPingAckMessage.class, (topVer, snd, msg) -> {
                if (curReq != null)
                    curReq.add(msg);
            });
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "test";
        }

        public void ping() {
            curReq = new CopyOnWriteArrayList<>();

            try {
                for (int i = 0; i < 3; i++)
                    srv.context().discovery().sendCustomEvent(new DataCenterPingMessage());

                while (curReq.size() < 3)
                    U.sleep(10);

                print(curReq);

                curReq = null;
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }

        /** */
        void print(List<DataCenterPingAckMessage> req) {
            LinkedHashMap<UUID, List<Long>> res = new LinkedHashMap<>();

            req.forEach(message -> {
                UUID prevNodeId = null;
                long prevTimestamp = -1;

                for (Map.Entry<UUID, Long> e : message.path().entrySet()) {
                    UUID nodeId = e.getKey();
                    Long timestamp = e.getValue();

                    if (prevTimestamp != -1) {
                        res.computeIfAbsent(prevNodeId, k -> new ArrayList<>())
                            .add(timestamp - prevTimestamp);
                    }

                    prevTimestamp = timestamp;
                    prevNodeId = nodeId;
                }

                // From last node in a ring to the coordinator.
                res.computeIfAbsent(prevNodeId, k -> new ArrayList<>())
                    .add(prevTimestamp - message.createTime());
            });

            List<List<?>> tblData = new ArrayList<>();

            for (Map.Entry<UUID, List<Long>> e : res.entrySet()) {
                List<Long> latencies = e.getValue();

                tblData.add(Arrays.asList(
                    e.getKey(),
                    String.format("%.1f", latencies.stream().mapToLong(Long::longValue).average().orElse(-1.0)),
                    latencies.stream().mapToLong(Long::longValue).max().orElse(-1)
                ));
            }

            SystemViewCommand.printTable(
                F.asList("fromNodeId", "avg (ms)", "max (ms)"),
                F.asList(STRING, NUMBER, NUMBER, NUMBER),
                tblData,
                System.out::println
            );
        }
    }
}
