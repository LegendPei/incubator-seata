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

import org.apache.seata.common.exception.StoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    void testClosePropagatesFinalWalSyncFailure() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 100L, true);

        controller.afterWrite();
        syncer.fail = true;

        StoreException exception = Assertions.assertThrows(StoreException.class, controller::close);

        Assertions.assertTrue(exception.getMessage().contains("shutdown"));
        Assertions.assertEquals(1, syncer.flushCount);
        Assertions.assertTrue(executor.isShutdown());
        Assertions.assertEquals(1L, controller.stats().getSyncFailureCount());
        Assertions.assertEquals(1L, controller.stats().getUnsyncedWriteRequests());
    }

    @Test
    void testCloseWaitsForInFlightBackgroundSyncWhenFinalSyncIsDisabled() throws Exception {
        BlockingWalSyncer syncer = new BlockingWalSyncer();
        TrackingScheduledExecutor executor = new TrackingScheduledExecutor();
        RocksDBWalSyncController controller = new RocksDBWalSyncController(
                RocksDBWalSyncMode.PERIODIC,
                syncer,
                25L,
                1L,
                false,
                1000L,
                executor,
                true,
                System::currentTimeMillis,
                System::nanoTime);
        ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> closeThread = new AtomicReference<>();

        try {
            controller.afterWrite();
            Assertions.assertTrue(syncer.awaitSyncStarted(5, TimeUnit.SECONDS));
            Future<?> close = closeExecutor.submit(() -> {
                closeThread.set(Thread.currentThread());
                controller.close();
            });
            Assertions.assertTrue(executor.awaitShutdownNow(5, TimeUnit.SECONDS));
            waitUntil(() -> close.isDone() || closeThread.get().getState() == Thread.State.BLOCKED);

            Assertions.assertFalse(close.isDone());
            syncer.releaseSync();

            close.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(executor.isTerminated());
        } finally {
            syncer.releaseSync();
            controller.close();
            closeExecutor.shutdownNow();
            closeExecutor.awaitTermination(5, TimeUnit.SECONDS);
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testAfterWriteDoesNotThrowWhenBackgroundSchedulingIsRejected() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 1L, false);
        executor.reject = true;

        Assertions.assertDoesNotThrow(controller::afterWrite);
        Assertions.assertEquals(1L, controller.stats().getUnsyncedWriteRequests());

        executor.reject = false;
        controller.requestSync("retry-after-rejection");
        Assertions.assertEquals(1, syncer.flushCount);
        Assertions.assertEquals(0L, controller.stats().getUnsyncedWriteRequests());
    }

    @Test
    void testAfterWriteDoesNotThrowWhenExecutorRejectsDuringShutdown() {
        AtomicLong nowMillis = new AtomicLong(1000L);
        AtomicLong nowNanos = new AtomicLong(10_000L);
        FakeWalSyncer syncer = new FakeWalSyncer();
        ManualScheduledExecutor executor = new ManualScheduledExecutor();
        RocksDBWalSyncController controller = controller(syncer, executor, nowMillis, nowNanos, 1000L, 1L, false);

        executor.shutdown();

        Assertions.assertDoesNotThrow(controller::afterWrite);
        Assertions.assertEquals(0, syncer.flushCount);
        Assertions.assertEquals(1L, controller.stats().getUnsyncedWriteRequests());
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

    private void waitUntil(Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + 3000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.evaluate()) {
                return;
            }
            Thread.sleep(20L);
        }
        Assertions.fail("condition was not satisfied before timeout");
    }

    @FunctionalInterface
    private interface Condition {
        boolean evaluate();
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

    private static final class BlockingWalSyncer implements RocksDBWalSyncController.WalSyncer {
        private final CountDownLatch syncStarted = new CountDownLatch(1);
        private final CountDownLatch releaseSync = new CountDownLatch(1);
        private long sequence;

        @Override
        public void flushWal(boolean sync) {
            syncStarted.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    releaseSync.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            sequence++;
        }

        @Override
        public long latestSequenceNumber() {
            return sequence;
        }

        private boolean awaitSyncStarted(long timeout, TimeUnit unit) throws InterruptedException {
            return syncStarted.await(timeout, unit);
        }

        private void releaseSync() {
            releaseSync.countDown();
        }
    }

    private static final class TrackingScheduledExecutor extends ScheduledThreadPoolExecutor {
        private final CountDownLatch shutdownNowCalled = new CountDownLatch(1);

        private TrackingScheduledExecutor() {
            super(1);
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownNowCalled.countDown();
            return super.shutdownNow();
        }

        private boolean awaitShutdownNow(long timeout, TimeUnit unit) throws InterruptedException {
            return shutdownNowCalled.await(timeout, unit);
        }
    }

    private static final class ManualScheduledExecutor extends AbstractExecutorService
            implements ScheduledExecutorService {
        private boolean reject;
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
            if (reject || shutdown) {
                throw new RejectedExecutionException(shutdown ? "executor shutdown" : "executor rejected task");
            }
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
