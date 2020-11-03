package org.apache.ignite.internal.processors;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.PerformanceStatisticsTracingSpi;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.spi.tracing.Scope.CACHE_API;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

public class PerfStatTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTracingSpi(new PerformanceStatisticsTracingSpi());

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void test() throws Exception {
        int duration = 30_000;

        IgniteEx srv = startGrids(2);

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10_000; i++) {
            cache.put(i, i);
        }

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().
                withIncludedScopes(Collections.singleton(CACHE_API)).
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());
        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(CACHE_API).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());


        System.out.println("MY tracing cfg="+srv.tracingConfiguration().getAll(Scope.TX));
        long start = U.currentTimeMillis();

        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (U.currentTimeMillis() - start < duration) {
                cache.get(ThreadLocalRandom.current().nextInt(10_000));

                cnt.incrementAndGet();
            }
        }, 4, "load");

        long precCnt = 0;

        while (U.currentTimeMillis() - start < duration) {
            System.out.println("MY load=" + (cnt.get() - precCnt) + " ops");

            precCnt = cnt.get();

            U.sleep(1000);
        }

        fut.get();
    }


    /** @throws Exception If failed. */
    @Test
    public void test0() throws Exception {
        IgniteEx srv = startGrids(2);

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().
                withIncludedScopes(Collections.singleton(CACHE_API)).
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());
        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(CACHE_API).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        System.out.println("MY start TX");
        try (Transaction tx = srv.transactions().txStart()) {
            U.sleep(1000);
            System.out.println("MY put get START");
            cache.put(1, 1);
            cache.get(1);
            cache.get(1);
            cache.get(1);
            System.out.println("MY put get END");

            U.sleep(1000);
            System.out.println("MY commit TX");
            tx.commit();

            U.sleep(1000);
        }
        System.out.println("MY close TX");

    }
}
