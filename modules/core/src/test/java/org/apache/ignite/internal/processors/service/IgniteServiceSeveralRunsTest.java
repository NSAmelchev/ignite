package org.apache.ignite.internal.processors.service;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Created by Amelchev Nikita on 01.02.2017.
 */
public class IgniteServiceSeveralRunsTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setClientMode(gridName.endsWith("4"));
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSeveralRunsServices() throws Exception {
        for (int i = 1; i <= 15; i++) {
            System.out.println("Example #"+i);
            runServiceExample("4");
        }
    }

    private void runServiceExample(String gridName) throws Exception {
        try (Ignite ignite = Ignition.start(getConfiguration(gridName))) {
            // Deploy services only on server nodes.
            IgniteServices svcs = ignite.services(ignite.cluster().forServers());

            try {
                // Deploy cluster singleton.
                svcs.deployClusterSingleton("myClusterSingletonService", new SimpleMapServiceImpl());

                // Deploy node singleton.
                svcs.deployNodeSingleton("myNodeSingletonService", new SimpleMapServiceImpl());

                // Deploy 2 instances, regardless of number nodes.
                svcs.deployMultiple("myMultiService",
                    new SimpleMapServiceImpl(),
                    2 /*total number*/,
                    0 /*0 for unlimited*/);

                // Example for using a service proxy
                // to access a remotely deployed service.
                serviceProxyExample(ignite);

                // Example for auto-injecting service proxy
                // into remote closure execution.
                serviceInjectionExample(ignite);
            }
            finally {
                // Undeploy all services.
                ignite.services().cancelAll();
            }
        }
    }

    /**
     * Simple example to demonstrate service proxy invocation of a remotely deployed service.
     *
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    private void serviceProxyExample(Ignite ignite) throws Exception {
        System.out.println(">>>");
        System.out.println(">>> Starting service proxy example.");
        System.out.println(">>>");

        // Get a sticky proxy for node-singleton map service.
        SimpleMapService<Integer, String> mapSvc = ignite.services().serviceProxy("myNodeSingletonService",
            SimpleMapService.class,
            true);

        int cnt = 10;

        // Each service invocation will go over a proxy to some remote node.
        // Since service proxy is sticky, we will always be contacting the same remote node.
        for (int i = 0; i < cnt; i++)
            mapSvc.put(i, Integer.toString(i));

        // Get size from remotely deployed service instance.
        int mapSize = mapSvc.size();

        System.out.println("Map service size: " + mapSize);

        if (mapSize != cnt)
            throw new Exception("Invalid map size [expected=" + cnt + ", actual=" + mapSize + ']');
    }

    /**
     * Simple example to demonstrate how to inject service proxy into distributed closures.
     *
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    private void serviceInjectionExample(Ignite ignite) throws Exception {
        System.out.println(">>>");
        System.out.println(">>> Starting service injection example.");
        System.out.println(">>>");

        // Get a sticky proxy for cluster-singleton map service.
        SimpleMapService<Integer, String> mapSvc = ignite.services().serviceProxy("myClusterSingletonService",
            SimpleMapService.class,
            true);

        int cnt = 10;

        // Each service invocation will go over a proxy to the remote cluster-singleton instance.
        for (int i = 0; i < cnt; i++)
            mapSvc.put(i, Integer.toString(i));

        // Broadcast closure to every node.
        final Collection<Integer> mapSizes = ignite.compute().broadcast(new SimpleClosure());

        System.out.println("Closure execution result: " + mapSizes);

        // Since we invoked the same cluster-singleton service instance
        // from all the remote closure executions, they should all return
        // the same size equal to 'cnt' value.
        for (int mapSize : mapSizes)
            if (mapSize != cnt)
                throw new Exception("Invalid map size [expected=" + cnt + ", actual=" + mapSize + ']');
    }

    /**
     * Simple closure to demonstrate auto-injection of the service proxy.
     */
    private class SimpleClosure implements IgniteCallable<Integer> {
        // Auto-inject service proxy.
        @ServiceResource(serviceName = "myClusterSingletonService", proxyInterface = SimpleMapService.class)
        private transient SimpleMapService mapSvc;

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            int mapSize = mapSvc.size();

            System.out.println("Executing closure [mapSize=" + mapSize + ']');

            return mapSize;
        }
    }

    class SimpleMapServiceImpl<K, V> implements Service, SimpleMapService<K, V> {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Underlying cache map. */
        private IgniteCache<K, V> cache;

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            cache.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public V get(K key) {
            return cache.get(key);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            cache.clear();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return cache.size();
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            ignite.destroyCache(ctx.name());

            System.out.println("Service was cancelled: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // Create a new cache for every service deployment.
            // Note that we use service name as cache name, which allows
            // for each service deployment to use its own isolated cache.
            cache = ignite.getOrCreateCache(new CacheConfiguration<K, V>(ctx.name()));

            System.out.println("Service was initialized: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            System.out.println("Executing distributed service: " + ctx.name());
        }
    }

    interface SimpleMapService<K, V> {
        /**
         * Puts key-value pair into map.
         *
         * @param key Key.
         * @param val Value.
         */
        void put(K key, V val);

        /**
         * Gets value based on key.
         *
         * @param key Key.
         * @return Value.
         */
        V get(K key);

        /**
         * Clears map.
         */
        void clear();

        /**
         * @return Map size.
         */
        int size();
    }
}
