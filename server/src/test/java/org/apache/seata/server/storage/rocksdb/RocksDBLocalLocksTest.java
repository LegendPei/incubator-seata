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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

class RocksDBLocalLocksTest {

    private static final long TIMEOUT_SECONDS = 5;

    @Test
    void testLockAllOrdersStripesConsistently() throws Exception {
        RocksDBLocalLocks locks = new RocksDBLocalLocks(1024);
        ReentrantLock[] stripes = lockStripes(locks);
        RocksDBLocalLocks.LockScope stripe31Blocker = locks.lock(new byte[] {0});
        RocksDBLocalLocks.LockScope stripe961Blocker = locks.lock(new byte[] {0, 0});
        CountDownLatch firstStart = new CountDownLatch(1);
        CountDownLatch secondStart = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread first = lockAllWorker(
                locks, Arrays.asList(new byte[] {0}, new byte[] {0, 0}), firstStart, completed, failure, "31-961");
        Thread second = lockAllWorker(
                locks, Arrays.asList(new byte[] {0, 0}, new byte[] {3, 1}), secondStart, completed, failure, "961-31");

        try {
            first.start();
            second.start();
            firstStart.countDown();
            awaitCondition(
                    () -> stripes[31].hasQueuedThread(first), "First worker did not queue for stripe 31 in time");
            secondStart.countDown();
            awaitCondition(
                    () -> stripes[31].hasQueuedThread(second) || stripes[961].hasQueuedThread(second),
                    "Second worker did not queue for its first stripe in time");

            stripe31Blocker.close();
            stripe31Blocker = null;
            awaitCondition(
                    () -> stripes[961].hasQueuedThread(first), "First worker did not queue for stripe 961 in time");
            stripe961Blocker.close();
            stripe961Blocker = null;

            Assertions.assertTrue(
                    completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Both workers must complete; timeout suggests a possible ABBA deadlock between stripes 31 and 961");
            Assertions.assertNull(failure.get(), "Lock worker failed: " + failure.get());
        } finally {
            if (stripe961Blocker != null) {
                stripe961Blocker.close();
            }
            if (stripe31Blocker != null) {
                stripe31Blocker.close();
            }
        }
    }

    @Test
    void testLockAllDeduplicatesSameStripe() throws Exception {
        RocksDBLocalLocks locks = new RocksDBLocalLocks(1);

        try (RocksDBLocalLocks.LockScope ignored =
                locks.lockAll(Arrays.asList(new byte[] {1}, new byte[] {2}, new byte[] {1}))) {
            Assertions.assertEquals(1, lockStripes(locks)[0].getHoldCount());
        }
    }

    @Test
    void testLockAllHandlesEmptyCollection() {
        RocksDBLocalLocks locks = new RocksDBLocalLocks(1);

        try (RocksDBLocalLocks.LockScope ignored = locks.lockAll(Collections.emptyList())) {
            Assertions.assertNotNull(ignored);
        }
    }

    @Test
    void testRejectsNullKey() {
        RocksDBLocalLocks locks = new RocksDBLocalLocks(1);

        Assertions.assertThrows(NullPointerException.class, () -> locks.lock(null));
        Assertions.assertThrows(NullPointerException.class, () -> locks.lockAll(Arrays.asList(new byte[] {1}, null)));
    }

    @Test
    void testLockScopeReleasesLockWhenProtectedWorkThrows() throws Exception {
        RocksDBLocalLocks locks = new RocksDBLocalLocks(1);

        Assertions.assertThrows(IllegalStateException.class, () -> {
            try (RocksDBLocalLocks.LockScope ignored = locks.lock(new byte[] {1})) {
                throw new IllegalStateException("test failure");
            }
        });

        CountDownLatch acquired = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(
                () -> {
                    try (RocksDBLocalLocks.LockScope ignored = locks.lock(new byte[] {1})) {
                        acquired.countDown();
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                },
                "lock-after-exception");
        worker.setDaemon(true);
        worker.start();

        Assertions.assertTrue(
                acquired.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "Lock scope did not release its lock after protected work threw");
        worker.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        Assertions.assertFalse(worker.isAlive(), "Worker did not finish after acquiring the released lock");
        Assertions.assertNull(failure.get(), "Lock worker failed: " + failure.get());
    }

    private static Thread lockAllWorker(
            RocksDBLocalLocks locks,
            Collection<byte[]> keys,
            CountDownLatch start,
            CountDownLatch completed,
            AtomicReference<Throwable> failure,
            String name) {
        Thread worker = new Thread(
                () -> {
                    try {
                        if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                            throw new AssertionError("Worker was not started in time");
                        }
                        try (RocksDBLocalLocks.LockScope ignored = locks.lockAll(keys)) {}
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    } finally {
                        completed.countDown();
                    }
                },
                "lock-order-" + name);
        worker.setDaemon(true);
        return worker;
    }

    private static ReentrantLock[] lockStripes(RocksDBLocalLocks locks) throws Exception {
        Field field = RocksDBLocalLocks.class.getDeclaredField("locks");
        field.setAccessible(true);
        return (ReentrantLock[]) field.get(locks);
    }

    private static void awaitCondition(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(condition.getAsBoolean(), message);
    }
}
