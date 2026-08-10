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
import org.apache.seata.core.model.GlobalStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.WriteBatch;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

class RocksDBStoreEngineTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void afterEach() {
        RocksDBStoreEngineFactory.destroy();
    }

    @Test
    void testPutGetDelete() {
        try (RocksDBStoreEngine engine = open("db", true)) {
            byte[] key = RocksDBKeyCodec.encodeXid("xid-1");
            byte[] value = RocksDBValueCodec.encode(
                    RocksDBValueCodec.ValueType.GLOBAL_SESSION, "global".getBytes(StandardCharsets.UTF_8));

            engine.put(RocksDBColumnFamily.GLOBAL_SESSION, key, value);

            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.GLOBAL_SESSION, key));

            engine.delete(RocksDBColumnFamily.GLOBAL_SESSION, key);
            Assertions.assertNull(engine.get(RocksDBColumnFamily.GLOBAL_SESSION, key));
        }
    }

    @Test
    void testBatchPutAndDelete() throws Exception {
        try (RocksDBStoreEngine engine = open("batch-put-delete", true)) {
            byte[] key = "key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);

            try (WriteBatch batch = new WriteBatch()) {
                engine.put(batch, RocksDBColumnFamily.DEFAULT, key, value);
                engine.write(batch);
            }
            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.DEFAULT, key));

            try (WriteBatch batch = new WriteBatch()) {
                engine.delete(batch, RocksDBColumnFamily.DEFAULT, key);
                engine.write(batch);
            }
            Assertions.assertNull(engine.get(RocksDBColumnFamily.DEFAULT, key));
        }
    }

    @Test
    void testPrefixScan() {
        try (RocksDBStoreEngine engine = open("scan", false)) {
            byte[] value = "branch".getBytes(StandardCharsets.UTF_8);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 1L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 2L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-2", 1L), value);

            List<RocksDBStoreEngine.RocksDBEntry> entries =
                    engine.prefixScan(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1"));

            Assertions.assertEquals(2, entries.size());
        }
    }

    @Test
    void testScanByPrefixCanSeekAndStopWithStats() {
        try (RocksDBStoreEngine engine = open("bounded-scan", false)) {
            byte[] prefix = RocksDBKeyCodec.encodeGlobalStatusPrefix(GlobalStatus.Begin);
            engine.put(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Begin, 100L, "tx-before"),
                    "tx-before".getBytes(StandardCharsets.UTF_8));
            engine.put(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Begin, 200L, "tx-first"),
                    "tx-first".getBytes(StandardCharsets.UTF_8));
            engine.put(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Begin, 300L, "tx-second"),
                    "tx-second".getBytes(StandardCharsets.UTF_8));
            engine.put(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Begin, 400L, "tx-after"),
                    "tx-after".getBytes(StandardCharsets.UTF_8));
            engine.put(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Committing, 100L, "tx-other-status"),
                    "tx-other-status".getBytes(StandardCharsets.UTF_8));

            List<String> xids = new ArrayList<>();
            RocksDBStoreEngine.ScanStats stats = engine.scanByPrefix(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusSeekKey(GlobalStatus.Begin, 200L),
                    prefix,
                    0,
                    (key, value) -> RocksDBKeyCodec.extractBeginTimeFromStatusIndexKey(key) <= 300L,
                    (key, value) -> xids.add(new String(value, StandardCharsets.UTF_8)));

            Assertions.assertEquals(List.of("tx-first", "tx-second"), xids);
            Assertions.assertEquals(3, stats.getRowsScanned());
            Assertions.assertEquals(2, stats.getRowsReturned());
            Assertions.assertFalse(stats.isLimitReached());
        }
    }

    @Test
    void testBoundedScanFilterRejectsMaintenanceLockUpgrade() {
        StoreException exception = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (RocksDBStoreEngine engine = open("bounded-scan-lock-upgrade", false)) {
                byte[] prefix = "bounded-upgrade".getBytes(StandardCharsets.UTF_8);
                engine.put(
                        RocksDBColumnFamily.DEFAULT,
                        "bounded-upgrade-key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8));

                StoreException failure = Assertions.assertThrows(
                        StoreException.class,
                        () -> engine.scanByPrefix(
                                RocksDBColumnFamily.DEFAULT,
                                prefix,
                                prefix,
                                1,
                                (key, value) -> {
                                    engine.withMaintenanceLock(() -> null);
                                    return true;
                                },
                                (key, value) -> {}));

                Assertions.assertFalse(engine.isClosed());
                return failure;
            }
        });

        Assertions.assertTrue(exception.getMessage().contains("read-to-write lock upgrade is not allowed"));
    }

    @Test
    void testStreamingScanConsumerRejectsCloseUpgrade() {
        StoreException exception = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (RocksDBStoreEngine engine = open("streaming-scan-close-upgrade", false)) {
                byte[] prefix = "streaming-upgrade".getBytes(StandardCharsets.UTF_8);
                byte[] key = "streaming-upgrade-key".getBytes(StandardCharsets.UTF_8);
                byte[] value = "value".getBytes(StandardCharsets.UTF_8);
                engine.put(RocksDBColumnFamily.DEFAULT, key, value);

                StoreException failure = Assertions.assertThrows(
                        StoreException.class,
                        () -> engine.scanByPrefix(RocksDBColumnFamily.DEFAULT, prefix, (entryKey, entryValue) ->
                                engine.close()));

                Assertions.assertFalse(engine.isClosed());
                Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.DEFAULT, key));
                return failure;
            }
        });

        Assertions.assertTrue(exception.getMessage().contains("read-to-write lock upgrade is not allowed"));
    }

    @Test
    void testPrefixExistsAndDeleteByPrefix() {
        try (RocksDBStoreEngine engine = open("delete-prefix", false)) {
            byte[] value = "branch".getBytes(StandardCharsets.UTF_8);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 1L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 2L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-2", 1L), value);

            Assertions.assertTrue(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1")));
            Assertions.assertFalse(engine.prefixExists(
                    RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("missing")));

            engine.deleteByPrefix(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1"));

            Assertions.assertFalse(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1")));
            Assertions.assertTrue(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-2")));
            Assertions.assertEquals(
                    1,
                    engine.prefixScan(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-2"))
                            .size());
        }
    }

    @Test
    void testRangeDeleteByPrefixDeletesSamePrefixOnly() {
        try (RocksDBStoreEngine engine = open("delete-range-prefix", false, true)) {
            byte[] value = "branch".getBytes(StandardCharsets.UTF_8);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 1L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 2L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-2", 1L), value);

            engine.deleteByPrefix(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1"));

            Assertions.assertFalse(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1")));
            Assertions.assertTrue(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-2")));
        }
    }

    @Test
    void testRangeDeleteByBranchPrefixKeepsSameXidOtherBranches() {
        try (RocksDBStoreEngine engine = open("delete-range-branch-prefix", false, true)) {
            byte[] lockKey1 = RocksDBKeyCodec.encodeRowLock("resource", "table", "pk-1");
            byte[] lockKey2 = RocksDBKeyCodec.encodeRowLock("resource", "table", "pk-2");
            byte[] value = "lock".getBytes(StandardCharsets.UTF_8);
            engine.put(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndex("xid-1", 1L, lockKey1),
                    value);
            engine.put(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndex("xid-1", 2L, lockKey2),
                    value);

            engine.deleteByPrefix(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndexBranchPrefix("xid-1", 1L));

            Assertions.assertFalse(engine.prefixExists(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndexBranchPrefix("xid-1", 1L)));
            Assertions.assertTrue(engine.prefixExists(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndexBranchPrefix("xid-1", 2L)));
        }
    }

    @Test
    void testDeleteByPrefixFallsBackToScanWhenRangeUpperBoundUnavailable() {
        try (RocksDBStoreEngine engine = open("delete-range-fallback", false, true)) {
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);
            byte[] matchingKey = new byte[] {(byte) 0xff, 1};
            byte[] otherKey = new byte[] {0, 1};
            engine.put(RocksDBColumnFamily.DEFAULT, matchingKey, value);
            engine.put(RocksDBColumnFamily.DEFAULT, otherKey, value);

            engine.deleteByPrefix(RocksDBColumnFamily.DEFAULT, new byte[] {(byte) 0xff});

            Assertions.assertNull(engine.get(RocksDBColumnFamily.DEFAULT, matchingKey));
            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.DEFAULT, otherKey));
        }
    }

    @Test
    void testBatchDeleteByPrefixUsesRangeDeleteWhenEnabled() throws Exception {
        try (RocksDBStoreEngine engine = open("delete-range-batch", false, true)) {
            byte[] value = "branch".getBytes(StandardCharsets.UTF_8);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 1L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-1", 2L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-2", 1L), value);

            try (WriteBatch batch = new WriteBatch()) {
                Assertions.assertTrue(engine.deleteByPrefix(
                        batch, RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1")));
                engine.write(batch);
            }

            Assertions.assertFalse(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-1")));
            Assertions.assertTrue(
                    engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix("xid-2")));
        }
    }

    @Test
    void testMetadataInitialized() {
        try (RocksDBStoreEngine engine = open("metadata", true)) {
            byte[] formatVersion =
                    engine.get(RocksDBColumnFamily.METADATA, "format_version".getBytes(StandardCharsets.UTF_8));

            Assertions.assertEquals(
                    Integer.toString(RocksDBStoreEngine.FORMAT_VERSION),
                    new String(formatVersion, StandardCharsets.UTF_8));
        }
    }

    @Test
    void testPhase3IndexColumnFamiliesAvailable() {
        try (RocksDBStoreEngine engine = open("phase3-index-cf", true)) {
            byte[] statusKey = RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Begin, 1L, "xid-index");
            byte[] transactionIdKey = RocksDBKeyCodec.encodeTransactionIdIndex(1L);
            byte[] value = "xid-index".getBytes(StandardCharsets.UTF_8);

            engine.put(RocksDBColumnFamily.GLOBAL_STATUS_INDEX, statusKey, value);
            engine.put(RocksDBColumnFamily.TRANSACTION_ID_INDEX, transactionIdKey, value);

            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.GLOBAL_STATUS_INDEX, statusKey));
            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.TRANSACTION_ID_INDEX, transactionIdKey));
        }
    }

    @Test
    void testFlushPersistsEveryOpenedColumnFamily() throws Exception {
        try (RocksDBStoreEngine engine = open("flush-all-column-families", true)) {
            for (RocksDBColumnFamily columnFamily : RocksDBColumnFamily.values()) {
                engine.put(
                        columnFamily,
                        ("key-" + columnFamily.getName()).getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8));
            }

            engine.flush();

            for (RocksDBColumnFamily columnFamily : RocksDBColumnFamily.values()) {
                Assertions.assertTrue(
                        engine.getLongProperty(columnFamily, "rocksdb.total-sst-files-size") > 0,
                        () -> "column family was not flushed: " + columnFamily.getName());
            }
        }
    }

    @Test
    void testOpenRejectsUnsupportedFormatVersion() {
        RocksDBStoreConfig config = config("metadata-unsupported", true);
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            engine.put(
                    RocksDBColumnFamily.METADATA,
                    "format_version".getBytes(StandardCharsets.UTF_8),
                    Integer.toString(RocksDBStoreEngine.FORMAT_VERSION + 1).getBytes(StandardCharsets.UTF_8));
        }

        StoreException exception = Assertions.assertThrows(StoreException.class, () -> RocksDBStoreEngine.open(config));

        Assertions.assertTrue(exception.getMessage().contains("unsupported RocksDB format version"));
    }

    @Test
    void testOpenRejectsInvalidFormatVersion() {
        RocksDBStoreConfig config = config("metadata-invalid", true);
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            engine.put(
                    RocksDBColumnFamily.METADATA,
                    "format_version".getBytes(StandardCharsets.UTF_8),
                    "invalid".getBytes(StandardCharsets.UTF_8));
        }

        StoreException exception = Assertions.assertThrows(StoreException.class, () -> RocksDBStoreEngine.open(config));

        Assertions.assertTrue(exception.getMessage().contains("invalid RocksDB format version metadata"));
    }

    @Test
    void testFactoryReturnsSharedInstanceForSamePath() {
        RocksDBStoreConfig config = config("factory", true);

        RocksDBStoreEngine first = RocksDBStoreEngineFactory.getInstance(config);
        RocksDBStoreEngine second = RocksDBStoreEngineFactory.getInstance(config);

        Assertions.assertSame(first, second);
        Assertions.assertTrue(first.isSyncWrite());
    }

    @Test
    void testLifecycleWriteLockGuardRejectsReadToWriteUpgrade() throws Exception {
        try (RocksDBStoreEngine engine = open("lifecycle-lock-upgrade-guard", false)) {
            ReentrantReadWriteLock lifecycleLock = maintenanceLock(engine);
            lifecycleLock.readLock().lock();
            try {
                StoreException exception = Assertions.assertThrows(
                        StoreException.class, engine::ensureLifecycleWriteLockAcquisitionAllowed);

                Assertions.assertTrue(
                        exception.getMessage().contains("read-to-write lock upgrade is not allowed"));
            } finally {
                lifecycleLock.readLock().unlock();
            }
        }
    }

    @Test
    void testWriteOwnerCanReenterLifecycleWriteLockFromScan() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (RocksDBStoreEngine engine = open("lifecycle-write-lock-reentry", false)) {
                byte[] prefix = "write-owner-reentry".getBytes(StandardCharsets.UTF_8);
                engine.put(
                        RocksDBColumnFamily.DEFAULT,
                        "write-owner-reentry-key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8));

                engine.withMaintenanceLock(() -> {
                    engine.scanByPrefix(RocksDBColumnFamily.DEFAULT, prefix, (key, value) ->
                            engine.withMaintenanceLock(() -> null));
                    return null;
                });
            }
        });
    }

    @Test
    void testFactoryDestroyRejectedFromScanPreservesOpenEngine() {
        StoreException exception = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            RocksDBStoreConfig config = config("factory-destroy-lock-upgrade", false);
            RocksDBStoreEngine engine = RocksDBStoreEngineFactory.getInstance(config);
            byte[] prefix = "factory-upgrade".getBytes(StandardCharsets.UTF_8);
            byte[] key = "factory-upgrade-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);
            engine.put(RocksDBColumnFamily.DEFAULT, key, value);

            StoreException failure = Assertions.assertThrows(
                    StoreException.class,
                    () -> engine.scanByPrefix(RocksDBColumnFamily.DEFAULT, prefix, (entryKey, entryValue) ->
                            RocksDBStoreEngineFactory.destroy()));

            Assertions.assertFalse(engine.isClosed());
            Assertions.assertSame(engine, RocksDBStoreEngineFactory.getInstance(config));
            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.DEFAULT, key));
            return failure;
        });

        Assertions.assertTrue(exception.getMessage().contains("read-to-write lock upgrade is not allowed"));
    }

    @Test
    void testFactoryGetAndDestroyUseSingleSynchronizationProtocol() throws Exception {
        RocksDBStoreConfig config = config("factory-lifecycle", true);
        RocksDBStoreEngine engine = RocksDBStoreEngineFactory.getInstance(config);
        ReentrantReadWriteLock lifecycleLock = maintenanceLock(engine);
        BlockingPathConfig requestedConfig = new BlockingPathConfig(config);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<Thread> destroyThread = new AtomicReference<>();
        CountDownLatch destroyStarted = new CountDownLatch(1);
        Future<RocksDBStoreEngine> get = null;
        Future<?> destroy = null;

        lifecycleLock.readLock().lock();
        try {
            get = executor.submit(() -> RocksDBStoreEngineFactory.getInstance(requestedConfig));
            Assertions.assertTrue(requestedConfig.awaitPathRead(5, TimeUnit.SECONDS));
            destroy = executor.submit(() -> {
                destroyThread.set(Thread.currentThread());
                destroyStarted.countDown();
                RocksDBStoreEngineFactory.destroy();
            });
            Assertions.assertTrue(destroyStarted.await(5, TimeUnit.SECONDS));
            Future<?> destroyFuture = destroy;
            waitUntil(() -> destroyFuture.isDone()
                    || lifecycleLock.hasQueuedThread(destroyThread.get())
                    || destroyThread.get().getState() == Thread.State.BLOCKED);

            requestedConfig.releasePathRead();
            Future<RocksDBStoreEngine> getFuture = get;
            RocksDBStoreEngine returned =
                    Assertions.assertDoesNotThrow(() -> getFuture.get(5, TimeUnit.SECONDS));

            Assertions.assertSame(engine, returned);
            Assertions.assertFalse(returned.isClosed());
            Assertions.assertEquals(config, returned.getConfig());
        } finally {
            requestedConfig.releasePathRead();
            lifecycleLock.readLock().unlock();
            if (destroy != null) {
                destroy.get(5, TimeUnit.SECONDS);
            }
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFactoryRejectsDifferentPath() {
        RocksDBStoreEngineFactory.getInstance(config("factory-a", true));

        Assertions.assertThrows(
                StoreException.class, () -> RocksDBStoreEngineFactory.getInstance(config("factory-b", true)));
    }

    @Test
    void testFactoryRejectsDifferentSyncWrite() {
        RocksDBStoreConfig config = config("factory-sync", true);
        RocksDBStoreEngineFactory.getInstance(config);

        Assertions.assertThrows(
                StoreException.class,
                () -> RocksDBStoreEngineFactory.getInstance(new RocksDBStoreConfig(config.getDbPath(), false)));
    }

    @Test
    void testOpenWithTunedOptions() {
        RocksDBStoreConfig config = tunedConfig("tuned", true);

        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            byte[] key = RocksDBKeyCodec.encodeXid("xid-tuned");
            byte[] value = RocksDBValueCodec.encode(
                    RocksDBValueCodec.ValueType.GLOBAL_SESSION, "global".getBytes(StandardCharsets.UTF_8));

            engine.put(RocksDBColumnFamily.GLOBAL_SESSION, key, value);

            Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.GLOBAL_SESSION, key));
        }
    }

    @Test
    void testOpenRejectsInvalidCompressionType() {
        RocksDBStoreConfig config = tunedConfig("invalid-compression", true, "unknown");

        StoreException exception = Assertions.assertThrows(StoreException.class, () -> RocksDBStoreEngine.open(config));

        Assertions.assertTrue(exception.getMessage().contains("unsupported RocksDB compression type"));
    }

    @Test
    void testPeriodicWalSyncStatsAfterWrite() throws Exception {
        RocksDBStoreConfig config = periodicWalSyncConfig("periodic-wal-sync", false, 1L);

        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            byte[] key = RocksDBKeyCodec.encodeXid("xid-periodic");
            byte[] value = "periodic".getBytes(StandardCharsets.UTF_8);
            long syncCountBeforeWrite = engine.diagnostics().getWalSyncStats().getSyncCount();

            engine.put(RocksDBColumnFamily.GLOBAL_SESSION, key, value);

            waitUntil(() -> {
                RocksDBWalSyncStats stats = engine.diagnostics().getWalSyncStats();
                return stats.getSyncCount() > syncCountBeforeWrite && stats.getUnsyncedWriteRequests() == 0;
            });
            RocksDBWalSyncStats stats = engine.diagnostics().getWalSyncStats();
            Assertions.assertEquals(RocksDBWalSyncMode.PERIODIC, stats.getMode());
            Assertions.assertEquals(0L, stats.getSyncFailureCount());
            Assertions.assertEquals(0L, stats.getUnsyncedWriteRequests());
            Assertions.assertTrue(stats.getLastSyncedSequenceNumber() > 0);
        }
    }

    @Test
    void testPeriodicWalSyncDisabledWhenSyncWriteEnabled() {
        RocksDBStoreConfig config = periodicWalSyncConfig("periodic-ignored-by-sync", true, 1L);

        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            byte[] key = RocksDBKeyCodec.encodeXid("xid-sync");
            byte[] value = "sync".getBytes(StandardCharsets.UTF_8);

            engine.put(RocksDBColumnFamily.GLOBAL_SESSION, key, value);

            RocksDBWalSyncStats stats = engine.diagnostics().getWalSyncStats();
            Assertions.assertEquals(RocksDBWalSyncMode.NONE, stats.getMode());
            Assertions.assertEquals(0L, stats.getSyncCount());
        }
    }

    @Test
    void testCloseWaitsForAfterWriteBeforeFinalWalSync() throws Exception {
        RocksDBStoreConfig config = config("close-waits-for-after-write", false);
        RocksDBStoreEngine engine = RocksDBStoreEngine.open(config);
        BlockingAfterWriteClock currentTime = new BlockingAfterWriteClock();
        FailingWalSyncer syncer = new FailingWalSyncer();
        DirectScheduledExecutor walExecutor = new DirectScheduledExecutor();
        replaceWalSyncController(
                engine,
                new RocksDBWalSyncController(
                        RocksDBWalSyncMode.PERIODIC,
                        syncer,
                        1000L,
                        100L,
                        true,
                        1000L,
                        walExecutor,
                        true,
                        currentTime,
                        System::nanoTime));
        ReentrantReadWriteLock lifecycleLock = maintenanceLock(engine);
        ExecutorService lifecycleExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Thread> closeThread = new AtomicReference<>();
        byte[] key = RocksDBKeyCodec.encodeXid("xid-close-race");
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        try {
            Future<?> write = lifecycleExecutor.submit(() -> engine.put(RocksDBColumnFamily.GLOBAL_SESSION, key, value));
            Assertions.assertTrue(currentTime.awaitAfterWrite(5, TimeUnit.SECONDS));
            Future<?> close = lifecycleExecutor.submit(() -> {
                closeThread.set(Thread.currentThread());
                closeStarted.countDown();
                engine.close();
            });
            Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            waitUntil(() -> lifecycleLock.hasQueuedThread(closeThread.get()));

            Assertions.assertFalse(close.isDone());
            currentTime.releaseAfterWrite();

            write.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, syncer.flushCount);
            Assertions.assertTrue(walExecutor.isShutdown());
            try (RocksDBStoreEngine reopened = RocksDBStoreEngine.open(config)) {
                Assertions.assertArrayEquals(value, reopened.get(RocksDBColumnFamily.GLOBAL_SESSION, key));
            }
        } finally {
            currentTime.releaseAfterWrite();
            lifecycleExecutor.shutdownNow();
            lifecycleExecutor.awaitTermination(5, TimeUnit.SECONDS);
            if (!engine.isClosed()) {
                engine.close();
            }
        }
    }

    @Test
    void testCloseWaitsForActiveRead() throws Exception {
        RocksDBStoreEngine engine = open("close-waits-for-read", false);
        ReentrantReadWriteLock lifecycleLock = maintenanceLock(engine);
        ExecutorService lifecycleExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Thread> closeThread = new AtomicReference<>();
        byte[] key = "read-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        engine.put(RocksDBColumnFamily.DEFAULT, key, value);

        try {
            Future<?> read = lifecycleExecutor.submit(() -> {
                try {
                    engine.scanByPrefix(RocksDBColumnFamily.DEFAULT, new byte[0], (entryKey, entryValue) -> {
                        readEntered.countDown();
                        awaitLatch(releaseRead, "active read was not released");
                        throw StopReadAfterCloseException.INSTANCE;
                    });
                } catch (StopReadAfterCloseException ignored) {
                    // Stop before the iterator advances if the old implementation already closed native resources.
                }
            });
            Assertions.assertTrue(readEntered.await(5, TimeUnit.SECONDS));
            Future<?> close = lifecycleExecutor.submit(() -> {
                closeThread.set(Thread.currentThread());
                closeStarted.countDown();
                engine.close();
            });
            Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            waitUntil(() -> close.isDone() || lifecycleLock.hasQueuedThread(closeThread.get()));

            Assertions.assertFalse(close.isDone());
            Assertions.assertTrue(lifecycleLock.hasQueuedThread(closeThread.get()));
            releaseRead.countDown();

            read.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
        } finally {
            releaseRead.countDown();
            lifecycleExecutor.shutdownNow();
            lifecycleExecutor.awaitTermination(5, TimeUnit.SECONDS);
            if (!engine.isClosed()) {
                engine.close();
            }
        }
    }

    @Test
    void testReadOperationsUseStableClosedSemantics() {
        RocksDBStoreEngine engine = RocksDBStoreEngine.open(tunedConfig("reads-after-close", false));
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] prefix = new byte[0];
        engine.close();

        Assertions.assertThrows(StoreException.class, () -> engine.get(RocksDBColumnFamily.DEFAULT, key));
        Assertions.assertThrows(
                StoreException.class, () -> engine.prefixScan(RocksDBColumnFamily.DEFAULT, prefix));
        Assertions.assertThrows(
                StoreException.class,
                () -> engine.scanByPrefix(
                        RocksDBColumnFamily.DEFAULT, prefix, prefix, 1, null, (entryKey, value) -> {}));
        Assertions.assertThrows(
                StoreException.class,
                () -> engine.scanByPrefix(RocksDBColumnFamily.DEFAULT, prefix, (entryKey, value) -> {}));
        Assertions.assertThrows(
                StoreException.class, () -> engine.prefixExists(RocksDBColumnFamily.DEFAULT, prefix));

        RocksDBStoreDiagnostics diagnostics = engine.diagnostics();
        Assertions.assertTrue(diagnostics.isClosed());
        Assertions.assertEquals(0L, engine.getLongProperty(RocksDBStoreDiagnostics.ESTIMATE_NUM_KEYS));
        Assertions.assertEquals(
                0L,
                engine.getLongProperty(
                        RocksDBColumnFamily.DEFAULT, RocksDBStoreDiagnostics.CUR_SIZE_ACTIVE_MEM_TABLE));
        Assertions.assertNull(engine.getProperty("rocksdb.stats"));
        Assertions.assertNull(engine.getProperty(RocksDBColumnFamily.DEFAULT, "rocksdb.stats"));
        Assertions.assertEquals(0L, engine.getBlockCacheUsage());
        Assertions.assertEquals(0L, engine.getBlockCachePinnedUsage());
        Assertions.assertEquals(0L, engine.getBlockCacheCapacity());
    }

    @Test
    void testMutatingOperationsThrowStoreExceptionAfterClose() throws Exception {
        RocksDBStoreEngine engine = open("writes-after-close", false, true);
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        byte[] prefix = "prefix".getBytes(StandardCharsets.UTF_8);
        try (WriteBatch batch = new WriteBatch()) {
            engine.put(batch, RocksDBColumnFamily.DEFAULT, key, value);
            engine.close();

            Assertions.assertThrows(
                    StoreException.class, () -> engine.put(RocksDBColumnFamily.DEFAULT, key, value));
            Assertions.assertThrows(StoreException.class, () -> engine.delete(RocksDBColumnFamily.DEFAULT, key));
            Assertions.assertThrows(
                    StoreException.class, () -> engine.put(batch, RocksDBColumnFamily.DEFAULT, key, value));
            Assertions.assertThrows(
                    StoreException.class, () -> engine.delete(batch, RocksDBColumnFamily.DEFAULT, key));
            Assertions.assertThrows(StoreException.class, () -> engine.write(batch));
            Assertions.assertThrows(
                    StoreException.class, () -> engine.deleteByPrefix(RocksDBColumnFamily.DEFAULT, prefix));
            Assertions.assertThrows(
                    StoreException.class, () -> engine.deleteRangeByPrefix(RocksDBColumnFamily.DEFAULT, prefix));
            Assertions.assertThrows(
                    StoreException.class,
                    () -> engine.deleteByPrefix(batch, RocksDBColumnFamily.DEFAULT, prefix));
            Assertions.assertThrows(
                    StoreException.class,
                    () -> engine.deleteRangeByPrefix(batch, RocksDBColumnFamily.DEFAULT, prefix));
            Assertions.assertThrows(StoreException.class, () -> engine.withMaintenanceLock(() -> null));
            Assertions.assertThrows(StoreException.class, engine::flush);
        } finally {
            engine.close();
        }
    }

    @Test
    void testFactoryCanRecreateEngineAfterFinalWalSyncFailure() throws Exception {
        RocksDBStoreConfig config = config("wal-sync-close-failure-reopen", false);
        RocksDBStoreEngine engine = RocksDBStoreEngineFactory.getInstance(config);
        FailingWalSyncer syncer = new FailingWalSyncer();
        DirectScheduledExecutor executor = new DirectScheduledExecutor();
        replaceWalSyncController(engine, newFailingShutdownSyncController(syncer, executor));

        engine.put(
                RocksDBColumnFamily.GLOBAL_SESSION,
                RocksDBKeyCodec.encodeXid("xid-close-failure"),
                "value".getBytes(StandardCharsets.UTF_8));
        syncer.fail = true;

        StoreException exception = Assertions.assertThrows(StoreException.class, RocksDBStoreEngineFactory::destroy);

        Assertions.assertTrue(exception.getMessage().contains("shutdown"));
        Assertions.assertTrue(engine.isClosed());
        Assertions.assertTrue(executor.isShutdown());
        RocksDBStoreEngine reopened = RocksDBStoreEngineFactory.getInstance(config);
        Assertions.assertNotSame(engine, reopened);
        Assertions.assertNotNull(
                reopened.get(RocksDBColumnFamily.GLOBAL_SESSION, RocksDBKeyCodec.encodeXid("xid-close-failure")));
    }

    @Test
    void testFactoryRejectsDifferentTuningOptions() {
        RocksDBStoreConfig config = tunedConfig("factory-tuning", true);
        RocksDBStoreEngineFactory.getInstance(config);

        Assertions.assertThrows(
                StoreException.class,
                () -> RocksDBStoreEngineFactory.getInstance(new RocksDBStoreConfig(config.getDbPath(), true)));
    }

    @Test
    void testRangeDeleteLeavesNoResidueAfterRestart() {
        String dbName = "range-delete-restart";
        RocksDBStoreConfig config =
                new RocksDBStoreConfig(tempDir.resolve(dbName).toString(), false, true);

        // Phase 1: write data, range delete, close
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            byte[] value = "data".getBytes(StandardCharsets.UTF_8);
            byte[] xidPrefix = RocksDBKeyCodec.encodeXidPrefix("xid-rd");

            engine.put(RocksDBColumnFamily.GLOBAL_SESSION, RocksDBKeyCodec.encodeXid("xid-rd"), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-rd", 1L), value);
            engine.put(RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeBranch("xid-rd", 2L), value);
            byte[] lockKey = RocksDBKeyCodec.encodeRowLock("res", "tbl", "pk-1");
            engine.put(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndex("xid-rd", 1L, lockKey),
                    value);
            engine.put(
                    RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                    RocksDBKeyCodec.encodeGlobalStatusIndex(GlobalStatus.Begin, 100L, "xid-rd"),
                    value);
            engine.put(
                    RocksDBColumnFamily.TRANSACTION_ID_INDEX,
                    RocksDBKeyCodec.encodeTransactionIdIndex(100L),
                    "xid-rd".getBytes(StandardCharsets.UTF_8));
            engine.flush();

            // range delete global + branch sessions + lock index
            engine.deleteByPrefix(RocksDBColumnFamily.GLOBAL_SESSION, xidPrefix);
            engine.deleteByPrefix(RocksDBColumnFamily.BRANCH_SESSION, xidPrefix);
            engine.deleteByPrefix(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX, RocksDBKeyCodec.encodeLockBranchIndexGlobalPrefix("xid-rd"));
            engine.flush();
        }

        // Phase 2: reopen and verify no residue
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            byte[] xidPrefix = RocksDBKeyCodec.encodeXidPrefix("xid-rd");
            Assertions.assertFalse(engine.prefixExists(RocksDBColumnFamily.GLOBAL_SESSION, xidPrefix));
            Assertions.assertFalse(engine.prefixExists(RocksDBColumnFamily.BRANCH_SESSION, xidPrefix));
            Assertions.assertFalse(engine.prefixExists(
                    RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                    RocksDBKeyCodec.encodeLockBranchIndexGlobalPrefix("xid-rd")));
            // transaction ID index and global status index were NOT range-deleted, so they should remain
            Assertions.assertNotNull(engine.get(
                    RocksDBColumnFamily.TRANSACTION_ID_INDEX, RocksDBKeyCodec.encodeTransactionIdIndex(100L)));
        }
    }

    @Test
    void testMultipleOpenCloseCyclesDoNotLeakLocks() {
        String dbName = "open-close-cycles";
        RocksDBStoreConfig config =
                new RocksDBStoreConfig(tempDir.resolve(dbName).toString(), false);

        for (int cycle = 0; cycle < 3; cycle++) {
            try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
                byte[] key = RocksDBKeyCodec.encodeXid("xid-cycle-" + cycle);
                byte[] value = ("cycle-" + cycle).getBytes(StandardCharsets.UTF_8);
                engine.put(RocksDBColumnFamily.GLOBAL_SESSION, key, value);
                Assertions.assertArrayEquals(value, engine.get(RocksDBColumnFamily.GLOBAL_SESSION, key));
            }
        }

        // verify final open still works and data from last cycle is readable
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(config)) {
            byte[] key = RocksDBKeyCodec.encodeXid("xid-cycle-2");
            Assertions.assertNotNull(engine.get(RocksDBColumnFamily.GLOBAL_SESSION, key));
        }
    }

    private RocksDBStoreEngine open(String name, boolean syncWrite) {
        return RocksDBStoreEngine.open(config(name, syncWrite));
    }

    private RocksDBStoreEngine open(String name, boolean syncWrite, boolean enableRangeDelete) {
        return RocksDBStoreEngine.open(
                new RocksDBStoreConfig(tempDir.resolve(name).toString(), syncWrite, enableRangeDelete));
    }

    private RocksDBStoreConfig config(String name, boolean syncWrite) {
        return new RocksDBStoreConfig(tempDir.resolve(name).toString(), syncWrite);
    }

    private RocksDBStoreConfig tunedConfig(String name, boolean syncWrite) {
        return tunedConfig(name, syncWrite, "no");
    }

    private RocksDBStoreConfig tunedConfig(String name, boolean syncWrite, String compressionType) {
        return new RocksDBStoreConfig(
                tempDir.resolve(name).toString(),
                syncWrite,
                1024L * 1024L,
                1024L * 1024L,
                2,
                1,
                2,
                64,
                1024L * 1024L,
                4,
                8,
                12,
                true,
                true,
                compressionType);
    }

    private RocksDBStoreConfig periodicWalSyncConfig(String name, boolean syncWrite, long threshold) {
        return new RocksDBStoreConfig(
                tempDir.resolve(name).toString(),
                syncWrite,
                0L,
                0L,
                0,
                0,
                0,
                0,
                0L,
                0,
                0,
                0,
                false,
                false,
                null,
                false,
                false,
                RocksDBWalSyncMode.PERIODIC,
                1000,
                threshold,
                true,
                1000);
    }

    private RocksDBWalSyncController newFailingShutdownSyncController(
            RocksDBWalSyncController.WalSyncer syncer, ScheduledExecutorService executor) {
        return new RocksDBWalSyncController(
                RocksDBWalSyncMode.PERIODIC,
                syncer,
                1000L,
                100L,
                true,
                1000L,
                executor,
                true,
                System::currentTimeMillis,
                System::nanoTime);
    }

    private void replaceWalSyncController(RocksDBStoreEngine engine, RocksDBWalSyncController controller)
            throws Exception {
        Field field = RocksDBStoreEngine.class.getDeclaredField("walSyncController");
        field.setAccessible(true);
        field.set(engine, controller);
    }

    private ReentrantReadWriteLock maintenanceLock(RocksDBStoreEngine engine) throws ReflectiveOperationException {
        Field field = RocksDBStoreEngine.class.getDeclaredField("maintenanceLock");
        field.setAccessible(true);
        return (ReentrantReadWriteLock) field.get(engine);
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

    private static void awaitLatch(CountDownLatch latch, String failureMessage) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError(failureMessage);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for test barrier", e);
        }
    }

    @FunctionalInterface
    private interface Condition {
        boolean evaluate();
    }

    private static final class FailingWalSyncer implements RocksDBWalSyncController.WalSyncer {
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

    private static final class BlockingAfterWriteClock implements LongSupplier {
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch afterWriteEntered = new CountDownLatch(1);
        private final CountDownLatch releaseAfterWrite = new CountDownLatch(1);

        @Override
        public long getAsLong() {
            if (calls.incrementAndGet() == 2) {
                afterWriteEntered.countDown();
                try {
                    if (!releaseAfterWrite.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("afterWrite barrier was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while waiting at afterWrite barrier", e);
                }
            }
            return 1000L;
        }

        private boolean awaitAfterWrite(long timeout, TimeUnit unit) throws InterruptedException {
            return afterWriteEntered.await(timeout, unit);
        }

        private void releaseAfterWrite() {
            releaseAfterWrite.countDown();
        }
    }

    private static final class BlockingPathConfig extends RocksDBStoreConfig {
        private final CountDownLatch pathRead = new CountDownLatch(1);
        private final CountDownLatch releasePathRead = new CountDownLatch(1);

        private BlockingPathConfig(RocksDBStoreConfig config) {
            super(config.getDbPath(), config.isSyncWrite());
        }

        @Override
        public String getDbPath() {
            pathRead.countDown();
            awaitLatch(releasePathRead, "factory config validation was not released");
            return super.getDbPath();
        }

        private boolean awaitPathRead(long timeout, TimeUnit unit) throws InterruptedException {
            return pathRead.await(timeout, unit);
        }

        private void releasePathRead() {
            releasePathRead.countDown();
        }
    }

    private static final class StopReadAfterCloseException extends RuntimeException {
        private static final StopReadAfterCloseException INSTANCE = new StopReadAfterCloseException();

        private StopReadAfterCloseException() {
            super(null, null, false, false);
        }
    }

    private static final class DirectScheduledExecutor extends AbstractExecutorService
            implements ScheduledExecutorService {
        private boolean shutdown;

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
            return new DoneScheduledFuture<>(null);
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
