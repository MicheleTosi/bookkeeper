package org.apache.bookkeeper.bookie;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.bookkeeper.stats.*;

import java.util.concurrent.TimeUnit;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public class TestStatsLogger implements StatsLogger {

    public static final org.apache.bookkeeper.bookie.TestStatsLogger INSTANCE = new org.apache.bookkeeper.bookie.TestStatsLogger();

    static class TestOpStatsLogger implements OpStatsLogger {

        private int numSuccessfulEvent=0;
        private int numFailedEvent=0;

        final OpStatsData testOpStats = new OpStatsData(0, 0, 0, new long[6]);

        @Override
        public void registerFailedEvent(long eventLatency, TimeUnit unit) {
            //nop
        }

        @Override
        public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
            //nop
        }

        @Override
        public void registerSuccessfulValue(long value) {
            numSuccessfulEvent+=value;
        }

        @Override
        public void registerFailedValue(long value) {
            numFailedEvent+=value;
        }

        @Override
        public OpStatsData toOpStatsData() {
            return testOpStats;
        }

        @Override
        public void clear() {
            // nop
        }

        public int getNumSuccessfulEvent(){
            return numSuccessfulEvent;
        }

        public int getNumFailedEvent(){
            return numFailedEvent;
        }
    }
    static org.apache.bookkeeper.bookie.TestStatsLogger.TestOpStatsLogger testOpStatsLogger = new org.apache.bookkeeper.bookie.TestStatsLogger.TestOpStatsLogger();

    /**
     * A <i>no-op</i> {@code Counter}.
     */
    static class TestCounter implements Counter {
        @Override
        public void clear() {
            // nop
        }

        @Override
        public void inc() {
            // nop
        }

        @Override
        public void dec() {
            // nop
        }

        @Override
        public void addCount(long delta) {
            // nop
        }

        @Override
        public void addLatency(long eventLatency, TimeUnit unit) {
            // nop
        }

        @Override
        public Long get() {
            return 0L;
        }
    }
    static org.apache.bookkeeper.bookie.TestStatsLogger.TestCounter testCounter = new org.apache.bookkeeper.bookie.TestStatsLogger.TestCounter();

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return testOpStatsLogger;
    }

    @Override
    public Counter getCounter(String name) {
        return testCounter;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        // nop
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        // nop
    }

    @Override
    public StatsLogger scope(String name) {
        return this;
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // nop
    }

    @Override
    public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
        return getOpStatsLogger(name);
    }

    @Override
    public Counter getThreadScopedCounter(String name) {
        return getCounter(name);
    }
}
