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

package org.apache.ignite.examples.streaming;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.streamer.router.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Example to demonstrate streamer with multiple stages. This example builds price
 * bars which are entities that aggregate prices over some time interval. For each
 * interval a price bar holds the following metrics:
 * <ul>
 * <li>Open price - the first price in the time interval.</li>
 * <li>High price - the highest price in the interval.</li>
 * <li>Low price - the lowest price in the interval.</li>
 * <li>Close prices - the last price in the interval.</li>
 * </ul>
 * In this example trade quotes for several instruments are streamed into the system.
 * Constructing of price bars is performed in two stages. The first stage builds price bars
 * for one second intervals, and the second stage used results of the first stage to
 * build price bars for every 2 second interval.
 * <p>
 * Note, the bars in the example are not real charting bars, but rather a simplification
 * with purpose to demonstrate multi-stage streaming processing.
 * <p>
 * Remote nodes should always be started with special configuration file:
 * {@code 'ignite.{sh|bat} examples/config/example-streamer.xml'}.
 * When starting nodes this way JAR file containing the examples code
 * should be placed to {@code IGNITE_HOME/libs} folder. You can build
 * {@code gridgain-examples.jar} by running {@code mvn package} in
 * {@code IGNITE_HOME/examples} folder. After that {@code gridgain-examples.jar}
 * will be generated by Maven in {@code IGNITE_HOME/examples/target} folder.
 * <p>
 * Alternatively you can run {@link StreamingNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-streamer.xml} configuration.
 */
public class StreamingPriceBarsExample {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Count of total numbers to generate. */
    private static final int CNT = 10000000;

    /** The list of instruments. */
    private static final String[] INSTRUMENTS = {"IBM", "GOOG", "MSFT", "GE"};

    /** The list of initial instrument prices. */
    private static final double[] INITIAL_PRICES = {194.9, 893.49, 34.21, 23.24};

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Timer timer = new Timer("priceBars");

        // Start grid.
        final Ignite g = Ignition.start("examples/config/example-streamer.xml");

        System.out.println();
        System.out.println(">>> Streaming price bars example started.");

        try {
            TimerTask task = scheduleQuery(g, timer);

            streamData(g);

            // Force one more run to get final results.
            task.run();

            timer.cancel();

            // Reset all streamers on all nodes to make sure that
            // consecutive executions start from scratch.
            g.compute().broadcast(new Runnable() {
                @Override public void run() {
                    if (!ExamplesUtils.hasStreamer(g, "priceBars"))
                        System.err.println("Default streamer not found (is example-streamer.xml " +
                            "configuration used on all nodes?)");
                    else {
                        IgniteStreamer streamer = g.streamer("priceBars");

                        System.out.println("Clearing bars from streamer.");

                        streamer.reset();
                    }
                }
            });
        }
        finally {
            Ignition.stop(true);
        }
    }

    /**
     * Schedules the query to periodically output built bars to the console.
     *
     * @param g Grid.
     * @param timer Timer.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Ignite g, Timer timer) {
        TimerTask task = new TimerTask() {
            @Override public void run() {
                final IgniteStreamer streamer = g.streamer("priceBars");

                try {
                    Collection<Bar> bars = streamer.context().reduce(
                        // This closure will execute on remote nodes.
                        new IgniteClosure<StreamerContext, Collection<Bar>>() {
                            @Override public Collection<Bar> apply(StreamerContext ctx) {
                                Collection<Bar> values = ctx.<String, Bar>localSpace().values();

                                Collection<Bar> res = new ArrayList<>(values.size());

                                for (Bar bar : values)
                                    res.add(bar.copy());

                                return res;
                            }
                        },
                        // The reducer will always execute locally, on the same node
                        // that submitted the query.
                        new IgniteReducer<Collection<Bar>, Collection<Bar>>() {
                            private final Collection<Bar> res = new ArrayList<>();

                            @Override public boolean collect(@Nullable Collection<Bar> col) {
                                res.addAll(col);

                                return true;
                            }

                            @Override public Collection<Bar> reduce() {
                                return res;
                            }
                        }
                    );

                    for (Bar bar : bars)
                        System.out.println(bar.toString());

                    System.out.println("-----------------");
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(task, 2000, 2000);

        return task;
    }

    /**
     * Streams random prices into the system.
     *
     * @param g Grid.
     * @throws IgniteCheckedException If failed.
     */
    private static void streamData(final Ignite g) throws IgniteCheckedException {
        IgniteStreamer streamer = g.streamer("priceBars");

        for (int i = 0; i < CNT; i++) {
            for (int j = 0; j < INSTRUMENTS.length; j++) {
                // Use gaussian distribution to ensure that
                // numbers closer to 0 have higher probability.
                double price = round2(INITIAL_PRICES[j] + RAND.nextGaussian());

                Quote quote = new Quote(INSTRUMENTS[j], price);

                streamer.addEvent(quote);
            }
        }
    }

    /**
     * Rounds double value to two significant signs.
     *
     * @param val value to be rounded.
     * @return rounded double value.
     */
    private static double round2(double val) {
        return Math.floor(100 * val + 0.5) / 100;
    }

    /**
     * Trade quote that is streamed into the system.
     */
    private static class Quote implements StreamerAffinityEventRouter.AffinityEvent {
        /** Instrument symbol. */
        private final String symbol;

        /** Price. */
        private final double price;

        /**
         * @param symbol Symbol.
         * @param price Price.
         */
        Quote(String symbol, double price) {
            this.symbol = symbol;
            this.price = price;
        }

        /**
         * @return Symbol.
         */
        public String symbol() {
            return symbol;
        }

        /**
         * @return Price.
         */
        public double price() {
            return price;
        }

        /** {@inheritDoc} */
        @Override public String affinityKey() {
            return symbol;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Quote [symbol=" + symbol + ", price=" + price + ']';
        }
    }

    /**
     * The bar that is build by the streamer.
     */
    private static class Bar implements StreamerAffinityEventRouter.AffinityEvent {
        /** Instrument symbol. */
        private final String symbol;

        /** Open price. */
        private volatile double open;

        /** High price. */
        private volatile double high;

        /** Low price. */
        private volatile double low = Long.MAX_VALUE;

        /** Close price. */
        private volatile double close;

        /**
         * @param symbol Symbol.
         */
        Bar(String symbol) {
            this.symbol = symbol;
        }

        /**
         * @return Copy of this instance.
         */
        public synchronized Bar copy() {
            Bar res = new Bar(symbol);

            res.open = open;
            res.high = high;
            res.low = low;
            res.close = close;

            return res;
        }

        /**
         * Updates this bar with last price.
         *
         * @param price Price.
         */
        public synchronized void update(double price) {
            if (open == 0)
                open = price;

            high = Math.max(high, price);
            low = Math.min(low, price);
            close = price;
        }

        /**
         * Updates this bar with next bar.
         *
         * @param bar Next bar.
         */
        public synchronized void update(Bar bar) {
            if (open == 0)
                open = bar.open;

            high = Math.max(high, bar.high);
            low = Math.min(low, bar.low);
            close = bar.close;
        }

        /**
         * @return Symbol.
         */
        public String symbol() {
            return symbol;
        }

        /**
         * @return Open price.
         */
        public double open() {
            return open;
        }

        /**
         * @return High price.
         */
        public double high() {
            return high;
        }

        /**
         * @return Low price.
         */
        public double low() {
            return low;
        }

        /**
         * @return Close price.
         */
        public double close() {
            return close;
        }

        /** {@inheritDoc} */
        @Override public String affinityKey() {
            return symbol;
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return "Bar [symbol=" + symbol + ", open=" + open + ", high=" + high + ", low=" + low +
                ", close=" + close + ']';
        }
    }

    /**
     * The first stage where 1 second bars are built.
     */
    @SuppressWarnings({ "PublicInnerClass", "unchecked" })
    public static class FirstStage implements StreamerStage<Quote> {
        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection<Quote> quotes) {
            StreamerWindow win = ctx.window("stage1");

            // Add numbers to window.
            win.enqueueAll(quotes);

            Collection<Quote> polled = win.pollEvictedBatch();

            if (!polled.isEmpty()) {
                Map<String, Bar> map = new HashMap<>();

                for (Quote quote : polled) {
                    String symbol = quote.symbol();

                    Bar bar = map.get(symbol);

                    if (bar == null)
                        map.put(symbol, bar = new Bar(symbol));

                    bar.update(quote.price());
                }

                return Collections.<String, Collection<?>>singletonMap(ctx.nextStageName(), map.values());
            }

            return null;
        }
    }

    /**
     * The second stage where 2 second bars are built.
     */
    @SuppressWarnings({ "PublicInnerClass", "unchecked" })
    public static class SecondStage implements StreamerStage<Bar> {
        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection<Bar> bars) {
            ConcurrentMap<String, Bar> loc = ctx.localSpace();

            StreamerWindow win = ctx.window("stage2");

            // Add numbers to window.
            win.enqueueAll(bars);

            Collection<Bar> polled = win.pollEvictedBatch();

            if (!polled.isEmpty()) {
                Map<String, Bar> map = new HashMap<>();

                for (Bar polledBar : polled) {
                    String symbol = polledBar.symbol();

                    Bar bar = map.get(symbol);

                    if (bar == null)
                        map.put(symbol, bar = new Bar(symbol));

                    bar.update(polledBar);
                }

                loc.putAll(map);
            }

            return null;
        }
    }
}
