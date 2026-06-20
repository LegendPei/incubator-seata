/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.server.storage.rocksdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class RocksDBWalSyncControllerTest {

    @Test
    void testWriteThresholdTriggersWalSync() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 2L, true);

        controller.afterWrite();
        Assertions.assertEquals(0, syncer.flushCount);

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(3));
        controller.afterWrite();

        RocksDBWalSyncStats stats = controller.stats();
        Assertions.assertEquals(1, syncer.flushCount);
        Assertions.assertEquals(1L, stats.getSyncCount());
        Assertions.assertEquals(0L, stats.getUnsyncedWriteRequests());
        Assertions.assertEquals(2L, stats.getMaxUnsyncedWriteRequests());
        Assertions.assertEquals(0L, stats.getLastSyncCostMillis());
    }

    @Test
    void testIntervalTaskTriggersWalSync() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 10L, 100L, true);

        controller.afterWrite();
        nowMillis.addAndGet(11L);
        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
        executor.runFixedDelayTask();

        Assertions.assertEquals(1, syncer.flushCount);
        Assertions.assertEquals(1L, controller.stats().getSyncCount());
    }

    @Test
    void testWalSyncFailureKeepsUnsyncedWrites() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        syncer.fail = true;
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 1L, true);

        controller.afterWrite();

        RocksDBWalSyncStats stats = controller.stats();
        Assertions.assertEquals(1, syncer.flushCount);
        Assertions.assertEquals(0L, stats.getSyncCount());
        Assertions.assertEquals(1L, stats.getSyncFailureCount());
        Assertions.assertEquals(1L, stats.getUnsyncedWriteRequests());
        Assertions.assertEquals("boom", stats.getLastSyncError());
    }

    @Test
    void testUnsyncedWindowStartsAtFirstUnsyncedWrite() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 100L, true);

        controller.afterWrite();
        nowMillis.addAndGet(15L);
        controller.afterWrite();

        RocksDBWalSyncStats stats = controller.stats();
        Assertions.assertEquals(2L, stats.getUnsyncedWriteRequests());
        Assertions.assertEquals(15L, stats.getUnsyncedMillis());
        Assertions.assertEquals(15L, stats.getMaxUnsyncedMillis());
    }

    @Test
    void testCloseRunsFinalWalSync() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 100L, true);

        controller.afterWrite();
        Assertions.assertEquals(0, syncer.flushCount);

        controller.close();

        Assertions.assertEquals(1, syncer.flushCount);
        Assertions.assertTrue(executor.isShutdown());
        Assertions.assertEquals(0L, controller.stats().getUnsyncedWriteRequests());
    }

    @Test
    void testDisabledStatsAreEmpty() {
        RocksDBWalSyncController controller = RocksDBWalSyncController.disabled();

        controller.afterWrite();

        RocksDBWalSyncStats stats = controller.stats();
        Assertions.assertEquals(RocksDBWalSyncMode.NONE, stats.getMode());
        Assertions.assertEquals(0L, stats.getSyncCount());
    }

    private RocksDBWalSyncController controller(
            FakeWalSyncer syncer,
            ManualScheduledExecutor executor,
            AtomicLong nowMillis,
            AtomicLong nowNanos,
            long intervalMillis,
            long writeThreshold,
            boolean syncOnShutdown) {
        return new RocksDBWalSyncController(
                RocksDBWalSyncMode.PERIODIC,
                syncer,
                intervalMillis,
                writeThreshold,
                syncOnShutdown,
                1000L,
                executor,
                true,
                nowMillis::get,
                nowNanos::get);
    }

    private static final class FakeWalSyncer implements RocksDBWalSyncController.WalSyncer {
        private int flushCount;
        private long sequence;
        private boolean fail;

        @Override
        public void flushWal(boolean sync) throws Exception {
            flushCount++;
            if (fail) {
                throw new Exception("boom");
            }
            sequence++;
        }

        @Override
        public long latestSequenceNumber() {
            return sequence;
        }
    }

    private static final class ManualScheduledExecutor extends AbstractExecutorService
            implements ScheduledExecutorService {
        private boolean shutdown;
        private Runnable fixedDelayTask;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return new DoneScheduledFuture<>(null);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(java.util.concurrent.Callable<V> callable, long delay, TimeUnit unit) {
            return new DoneScheduledFuture<>(null);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return scheduleWithFixedDelay(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            fixedDelayTask = command;
            return new DoneScheduledFuture<>(null);
        }

        private void runFixedDelayTask() {
            if (fixedDelayTask != null) {
                fixedDelayTask.run();
            }
        }
    }

    private static final class DoneScheduledFuture<V> implements ScheduledFuture<V> {
        private final V value;

        private DoneScheduledFuture(V value) {
            this.value = value;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0L;
        }

        @Override
        public int compareTo(Delayed other) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public V get() {
            return value;
        }

        @Override
        public V get(long timeout, TimeUnit unit) {
            return value;
        }
    }
}
