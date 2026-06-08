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
package org.apache.seata.server.storage.rocksdb.benchmark;

import org.apache.seata.common.Constants;
import org.apache.seata.common.holder.ObjectHolder;
import org.apache.seata.config.ConfigurationCache;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.core.model.LockStatus;
import org.apache.seata.server.session.BranchSession;
import org.apache.seata.server.session.GlobalSession;
import org.apache.seata.server.session.SessionCondition;
import org.apache.seata.server.storage.rocksdb.RocksDBColumnFamily;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreConfig;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreDiagnostics;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngine;
import org.apache.seata.server.storage.rocksdb.lock.RocksDBLockManager;
import org.apache.seata.server.storage.rocksdb.store.RocksDBTransactionStoreManager;
import org.apache.seata.server.store.TransactionStoreManager.LogOperation;
import org.rocksdb.RocksDB;
import org.springframework.mock.env.MockEnvironment;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Manual benchmark for the RocksDB file mode store.
 *
 * <p>Run from an IDE or with a test classpath. This class is intentionally not named *Test, so it does not run in the
 * regular unit test suite.
 */
public final class RocksDBFileModeBenchmark {

    private static final String CSV_HEADER =
            "scenario,globalCount,branchPerGlobal,lockPerBranch,syncWrite,enableRangeDelete,warmupRounds,"
                    + "measureRounds,batchSize,ops,totalMs,opsPerSecond,p50Ms,p95Ms,p99Ms,dbSizeBytes,fileCount,"
                    + "sstFiles,walFiles,"
                    + "estimateLiveDataSizeBytes,totalSstFilesSizeBytes,pendingCompactionBytes,"
                    + "globalEstimateKeys,branchEstimateKeys,lockEstimateKeys,rocksdbConfigDigest";
    private static final List<String> ALL_BENCHMARKS = Arrays.asList("write", "query", "lock", "cleanup", "restart");
    private static final GlobalStatus[] STATUSES = {
        GlobalStatus.Begin,
        GlobalStatus.Committing,
        GlobalStatus.RollbackRetrying,
        GlobalStatus.AsyncCommitting,
        GlobalStatus.Committed
    };
    private static final GlobalStatus TARGET_STATUS = GlobalStatus.RollbackRetrying;

    private static volatile int sinkCount;
    private static volatile String sinkXid;

    private RocksDBFileModeBenchmark() {}

    public static void main(String[] args) throws Exception {
        Object originalEnvironment =
                ObjectHolder.INSTANCE.getObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT);
        ObjectHolder.INSTANCE.setObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, new MockEnvironment());
        ConfigurationCache.clear();
        try {
            new RocksDBFileModeBenchmark().run(BenchmarkOptions.parse(args));
        } finally {
            ConfigurationCache.clear();
            restoreEnvironment(originalEnvironment);
        }
    }

    private void run(BenchmarkOptions options) throws Exception {
        Path rootPath = options.rootPath();
        Files.createDirectories(rootPath);
        Path runPath = rootPath.resolve("rocksdb-file-mode-" + System.currentTimeMillis());
        Files.createDirectories(runPath);

        printEnvironment(options, rootPath, runPath);
        System.out.println(CSV_HEADER);
        try {
            if (options.isEnabled("write")) {
                runWriteBenchmark(runPath, options);
            }
            if (options.isEnabled("query")) {
                runQueryBenchmark(runPath, options);
            }
            if (options.isEnabled("lock")) {
                runLockBenchmark(runPath, options);
            }
            if (options.isEnabled("cleanup")) {
                runCleanupBenchmark(runPath, options);
            }
            if (options.isEnabled("restart")) {
                runRestartBenchmark(runPath, options);
            }
        } finally {
            if (options.cleanup) {
                deleteRecursively(runPath);
            }
        }
        System.out.println("sinkCount=" + sinkCount);
        System.out.println("sinkXid=" + sinkXid);
    }

    private void runWriteBenchmark(Path runPath, BenchmarkOptions options) throws Exception {
        OperationStats globalAddStats = new OperationStats(options.sampleEvery);
        OperationStats globalUpdateStats = new OperationStats(options.sampleEvery);
        OperationStats globalRemoveStats = new OperationStats(options.sampleEvery);
        OperationStats branchAddStats = new OperationStats(options.sampleEvery);
        OperationStats branchUpdateStats = new OperationStats(options.sampleEvery);
        OperationStats branchRemoveStats = new OperationStats(options.sampleEvery);
        Path lastDbPath = null;

        for (int round = 0; round < options.totalRounds(); round++) {
            BenchmarkDataSet dataSet = BenchmarkDataSet.create(options, round);
            Path dbPath = scenarioPath(runPath, "write-round-" + round);
            lastDbPath = dbPath;
            try (RocksDBStoreEngine engine = open(dbPath, options)) {
                RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
                for (GlobalSession globalSession : dataSet.globalSessions) {
                    measure(
                            globalAddStats,
                            round,
                            options,
                            () -> storeManager.writeSession(LogOperation.GLOBAL_ADD, globalSession));
                    for (BranchSession branchSession : dataSet.branchesOf(globalSession)) {
                        measure(
                                branchAddStats,
                                round,
                                options,
                                () -> storeManager.writeSession(LogOperation.BRANCH_ADD, branchSession));
                    }
                }
                for (GlobalSession globalSession : dataSet.globalSessions) {
                    globalSession.setStatus(nextStatus(globalSession.getStatus()));
                    measure(
                            globalUpdateStats,
                            round,
                            options,
                            () -> storeManager.writeSession(LogOperation.GLOBAL_UPDATE, globalSession));
                    for (BranchSession branchSession : dataSet.branchesOf(globalSession)) {
                        branchSession.setStatus(BranchStatus.PhaseOne_Done);
                        measure(
                                branchUpdateStats,
                                round,
                                options,
                                () -> storeManager.writeSession(LogOperation.BRANCH_UPDATE, branchSession));
                    }
                }
                if (options.branchPerGlobal > 0) {
                    for (GlobalSession globalSession : dataSet.globalSessions) {
                        BranchSession branchSession =
                                dataSet.branchesOf(globalSession).get(0);
                        measure(
                                branchRemoveStats,
                                round,
                                options,
                                () -> storeManager.writeSession(LogOperation.BRANCH_REMOVE, branchSession));
                    }
                }
                for (GlobalSession globalSession : dataSet.globalSessions) {
                    measure(
                            globalRemoveStats,
                            round,
                            options,
                            () -> storeManager.writeSession(LogOperation.GLOBAL_REMOVE, globalSession));
                }
                verifyStoreEmpty(engine);
                engine.flush();
            }
        }

        DbFootprint footprint = DbFootprint.from(lastDbPath);
        emit("write.global_add", options, globalAddStats, footprint);
        emit("write.global_update", options, globalUpdateStats, footprint);
        emit("write.global_remove", options, globalRemoveStats, footprint);
        emit("write.branch_add", options, branchAddStats, footprint);
        emit("write.branch_update", options, branchUpdateStats, footprint);
        emit("write.branch_remove", options, branchRemoveStats, footprint);
    }

    private void runQueryBenchmark(Path runPath, BenchmarkOptions options) throws Exception {
        BenchmarkDataSet dataSet = BenchmarkDataSet.create(options, 0);
        Path dbPath = scenarioPath(runPath, "query");
        OperationStats xidStats = new OperationStats(options.sampleEvery);
        OperationStats transactionIdStats = new OperationStats(options.sampleEvery);
        OperationStats statusStats = new OperationStats(options.sampleEvery);
        OperationStats beginSortedStats = new OperationStats(options.sampleEvery);
        OperationStats fullScanStats = new OperationStats(options.sampleEvery);

        try (RocksDBStoreEngine engine = open(dbPath, options)) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            writeDataSet(storeManager, dataSet);
            engine.flush();

            int iterations = options.totalRounds() * options.batchSize;
            int expectedStatusCount = dataSet.countByStatus(TARGET_STATUS);
            int expectedBeginCount = dataSet.countByStatus(GlobalStatus.Begin);
            for (int i = 0; i < iterations; i++) {
                final int iteration = i;
                int round = i / options.batchSize;
                measure(xidStats, round, options, () -> {
                    GlobalSession globalSession = dataSet.pick(iteration);
                    GlobalSession actual = storeManager.readSession(globalSession.getXid(), false);
                    assertTrue(actual != null, "xid query returned no session");
                    sinkXid = actual.getXid();
                });
                measure(transactionIdStats, round, options, () -> {
                    GlobalSession globalSession = dataSet.pick(iteration * 9973);
                    SessionCondition condition = new SessionCondition();
                    condition.setTransactionId(globalSession.getTransactionId());
                    condition.setLazyLoadBranch(true);
                    List<GlobalSession> actual = storeManager.readSession(condition);
                    assertEquals(1, actual.size(), "transactionId query size");
                    sinkXid = actual.get(0).getXid();
                });
                measure(statusStats, round, options, () -> {
                    SessionCondition condition = new SessionCondition(TARGET_STATUS);
                    condition.setLazyLoadBranch(true);
                    List<GlobalSession> actual = storeManager.readSession(condition);
                    assertEquals(expectedStatusCount, actual.size(), "status query size");
                    sinkCount = actual.size();
                });
                measure(beginSortedStats, round, options, () -> {
                    List<GlobalSession> actual = storeManager.readSortByTimeoutBeginSessions(false);
                    assertEquals(expectedBeginCount, actual.size(), "begin sorted query size");
                    sinkCount = actual.size();
                });
                measure(fullScanStats, round, options, () -> {
                    SessionCondition condition = new SessionCondition();
                    condition.setLazyLoadBranch(true);
                    int count = 0;
                    for (GlobalSession globalSession : storeManager.readSession(condition)) {
                        if (globalSession.getStatus() == TARGET_STATUS) {
                            count++;
                        }
                    }
                    assertEquals(expectedStatusCount, count, "full scan status count");
                    sinkCount = count;
                });
            }
        }

        DbFootprint footprint = DbFootprint.from(dbPath);
        emit("query.xid", options, xidStats, footprint);
        emit("query.transaction_id", options, transactionIdStats, footprint);
        emit("query.status", options, statusStats, footprint);
        emit("query.begin_sorted", options, beginSortedStats, footprint);
        emit("query.full_scan_filter", options, fullScanStats, footprint);
    }

    private void runLockBenchmark(Path runPath, BenchmarkOptions options) throws Exception {
        OperationStats acquireStats = new OperationStats(options.sampleEvery);
        OperationStats conflictAcquireStats = new OperationStats(options.sampleEvery);
        OperationStats conflictCheckStats = new OperationStats(options.sampleEvery);
        OperationStats updateStatusStats = new OperationStats(options.sampleEvery);
        OperationStats releaseBranchStats = new OperationStats(options.sampleEvery);
        OperationStats releaseGlobalStats = new OperationStats(options.sampleEvery);
        OperationStats cleanOrphanStats = new OperationStats(options.sampleEvery);
        Path lastDbPath = null;

        for (int round = 0; round < options.totalRounds(); round++) {
            BenchmarkDataSet dataSet = BenchmarkDataSet.create(options.withAtLeastOneLock(), round);
            Path dbPath = scenarioPath(runPath, "lock-round-" + round);
            lastDbPath = dbPath;
            try (RocksDBStoreEngine engine = open(dbPath, options)) {
                RocksDBLockManager lockManager = new RocksDBLockManager(engine);
                for (BranchSession branchSession : dataSet.allBranches()) {
                    measure(acquireStats, round, options, () -> {
                        boolean locked = lockManager.acquireLock(branchSession);
                        assertTrue(locked, "lock acquire failed");
                    });
                }
                for (GlobalSession globalSession : dataSet.globalSessions) {
                    BranchSession branchSession =
                            dataSet.branchesOf(globalSession).get(0);
                    BranchSession conflict = dataSet.conflictBranch(branchSession);
                    measure(conflictCheckStats, round, options, () -> {
                        boolean lockable = lockManager.isLockable(
                                conflict.getXid(), conflict.getResourceId(), conflict.getLockKey());
                        assertTrue(!lockable, "conflict check should be false");
                    });
                    measure(conflictAcquireStats, round, options, () -> {
                        boolean locked = lockManager.acquireLock(conflict);
                        assertTrue(!locked, "conflict acquire should be false");
                    });
                    measure(
                            updateStatusStats,
                            round,
                            options,
                            () -> lockManager.updateLockStatus(globalSession.getXid(), LockStatus.Rollbacking));
                    measure(releaseBranchStats, round, options, () -> {
                        boolean released = lockManager.releaseLock(branchSession);
                        assertTrue(released, "branch release failed");
                    });
                    if (dataSet.branchesOf(globalSession).size() == 1) {
                        assertTrue(
                                lockManager.acquireLock(branchSession), "lock reacquire before global release failed");
                    }
                    measure(releaseGlobalStats, round, options, () -> {
                        boolean released = lockManager.releaseGlobalSessionLock(globalSession);
                        assertTrue(released, "global release failed");
                    });
                }
                assertTrue(
                        engine.prefixScan(RocksDBColumnFamily.LOCK, new byte[0]).isEmpty(),
                        "lock table should be empty after release");
                assertTrue(
                        engine.prefixScan(RocksDBColumnFamily.LOCK_BRANCH_INDEX, new byte[0])
                                .isEmpty(),
                        "lock branch index should be empty after release");
            }
        }

        Path lastOrphanDbPath = null;
        for (int round = 0; round < options.totalRounds(); round++) {
            Path orphanDbPath = scenarioPath(runPath, "lock-clean-orphan-round-" + round);
            lastOrphanDbPath = orphanDbPath;
            try (RocksDBStoreEngine engine = open(orphanDbPath, options)) {
                RocksDBLockManager lockManager = new RocksDBLockManager(engine);
                BenchmarkDataSet dataSet = BenchmarkDataSet.create(options.withAtLeastOneLock(), round);
                for (BranchSession branchSession : dataSet.allBranches()) {
                    assertTrue(lockManager.acquireLock(branchSession), "orphan lock prepare failed");
                }
                measure(cleanOrphanStats, round, options, () -> {
                    int cleaned = lockManager.cleanOrphanLocks();
                    assertTrue(cleaned > 0, "cleanOrphanLocks should clean prepared locks");
                });
                assertTrue(
                        engine.prefixScan(RocksDBColumnFamily.LOCK, new byte[0]).isEmpty(),
                        "lock table should be empty after clean orphan");
            }
        }

        DbFootprint footprint = DbFootprint.from(lastDbPath);
        emit("lock.acquire", options, acquireStats, footprint);
        emit("lock.conflict_check", options, conflictCheckStats, footprint);
        emit("lock.conflict_acquire", options, conflictAcquireStats, footprint);
        emit("lock.update_status", options, updateStatusStats, footprint);
        emit("lock.release_branch", options, releaseBranchStats, footprint);
        emit("lock.release_global", options, releaseGlobalStats, footprint);
        emit("lock.clean_orphan", options, cleanOrphanStats, DbFootprint.from(lastOrphanDbPath));
    }

    private void runCleanupBenchmark(Path runPath, BenchmarkOptions options) throws Exception {
        OperationStats globalRemoveStats = new OperationStats(options.sampleEvery);
        Path lastDbPath = null;

        for (int round = 0; round < options.totalRounds(); round++) {
            BenchmarkDataSet dataSet = BenchmarkDataSet.create(options, round);
            Path dbPath = scenarioPath(runPath, "cleanup-round-" + round);
            lastDbPath = dbPath;
            try (RocksDBStoreEngine engine = open(dbPath, options)) {
                RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
                writeDataSet(storeManager, dataSet);
                engine.flush();
                for (GlobalSession globalSession : dataSet.globalSessions) {
                    measure(
                            globalRemoveStats,
                            round,
                            options,
                            () -> storeManager.writeSession(LogOperation.GLOBAL_REMOVE, globalSession));
                }
                verifyStoreEmpty(engine);
                engine.flush();
            }
        }

        emit("cleanup.global_remove_with_branches", options, globalRemoveStats, DbFootprint.from(lastDbPath));
    }

    private void runRestartBenchmark(Path runPath, BenchmarkOptions options) throws Exception {
        runRestartScenario(runPath, options, "restart.mixed", null);
        runRestartScenario(runPath, options, "restart.active_only", GlobalStatus.Begin);
        runRestartScenario(runPath, options, "restart.terminal_only", GlobalStatus.Committed);
    }

    private void runRestartScenario(
            Path runPath, BenchmarkOptions options, String scenarioName, GlobalStatus overrideStatus) throws Exception {
        OperationStats restartStats = new OperationStats(options.sampleEvery);
        Path lastDbPath = null;

        for (int round = 0; round < options.totalRounds(); round++) {
            BenchmarkDataSet dataSet = BenchmarkDataSet.create(options, round);
            if (overrideStatus != null) {
                for (GlobalSession globalSession : dataSet.globalSessions) {
                    globalSession.setStatus(overrideStatus);
                }
            }
            Path dbPath = scenarioPath(runPath, scenarioName.replace('.', '_') + "-round-" + round);
            lastDbPath = dbPath;
            try (RocksDBStoreEngine engine = open(dbPath, options)) {
                RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
                writeDataSet(storeManager, dataSet);
                engine.flush();
            }
            final BenchmarkDataSet finalDataSet = dataSet;
            measure(restartStats, round, options, () -> {
                try (RocksDBStoreEngine engine = open(dbPath, options)) {
                    new RocksDBTransactionStoreManager(engine);
                }
            });
            try (RocksDBStoreEngine engine = open(dbPath, options)) {
                RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
                SessionCondition condition = new SessionCondition();
                condition.setLazyLoadBranch(true);
                assertEquals(
                        options.globalCount,
                        storeManager.readSession(condition).size(),
                        scenarioName + " restart global count");
            }
        }

        emit(scenarioName, options, restartStats, DbFootprint.from(lastDbPath));
    }

    private static void writeDataSet(RocksDBTransactionStoreManager storeManager, BenchmarkDataSet dataSet) {
        for (GlobalSession globalSession : dataSet.globalSessions) {
            storeManager.writeSession(LogOperation.GLOBAL_ADD, globalSession);
            for (BranchSession branchSession : dataSet.branchesOf(globalSession)) {
                storeManager.writeSession(LogOperation.BRANCH_ADD, branchSession);
            }
        }
    }

    private static RocksDBStoreEngine open(Path dbPath, BenchmarkOptions options) {
        return open(dbPath, options.syncWrite, options.enableRangeDelete);
    }

    private static RocksDBStoreEngine open(Path dbPath, boolean syncWrite) {
        return open(dbPath, syncWrite, false);
    }

    private static RocksDBStoreEngine open(Path dbPath, boolean syncWrite, boolean enableRangeDelete) {
        return RocksDBStoreEngine.open(new RocksDBStoreConfig(dbPath.toString(), syncWrite, enableRangeDelete));
    }

    private static Path scenarioPath(Path runPath, String scenario) throws IOException {
        Path path = runPath.resolve(scenario);
        deleteRecursively(path);
        Files.createDirectories(path);
        return path;
    }

    private static void measure(OperationStats stats, int round, BenchmarkOptions options, BenchmarkTask task)
            throws Exception {
        long startedAt = System.nanoTime();
        task.run();
        long elapsed = System.nanoTime() - startedAt;
        if (round >= options.warmupRounds) {
            stats.record(elapsed);
        }
    }

    private static void verifyStoreEmpty(RocksDBStoreEngine engine) {
        assertTrue(
                engine.prefixScan(RocksDBColumnFamily.GLOBAL_SESSION, new byte[0])
                        .isEmpty(),
                "global_session should be empty");
        assertTrue(
                engine.prefixScan(RocksDBColumnFamily.BRANCH_SESSION, new byte[0])
                        .isEmpty(),
                "branch_session should be empty");
        assertTrue(
                engine.prefixScan(RocksDBColumnFamily.GLOBAL_STATUS_INDEX, new byte[0])
                        .isEmpty(),
                "global_status_index should be empty");
        assertTrue(
                engine.prefixScan(RocksDBColumnFamily.TRANSACTION_ID_INDEX, new byte[0])
                        .isEmpty(),
                "transaction_id_index should be empty");
    }

    private static GlobalStatus nextStatus(GlobalStatus status) {
        if (status == GlobalStatus.Begin) {
            return GlobalStatus.Committing;
        }
        if (status == GlobalStatus.Committing) {
            return GlobalStatus.CommitRetrying;
        }
        if (status == GlobalStatus.RollbackRetrying) {
            return GlobalStatus.Rollbacking;
        }
        return status;
    }

    private static void emit(String scenario, BenchmarkOptions options, OperationStats stats, DbFootprint footprint) {
        System.out.println(scenario
                + ","
                + options.globalCount
                + ","
                + options.branchPerGlobal
                + ","
                + options.lockPerBranch
                + ","
                + options.syncWrite
                + ","
                + options.enableRangeDelete
                + ","
                + options.warmupRounds
                + ","
                + options.measureRounds
                + ","
                + options.batchSize
                + ","
                + stats.ops()
                + ","
                + millis(stats.totalNanos())
                + ","
                + format(stats.opsPerSecond())
                + ","
                + millis(stats.percentile(50))
                + ","
                + millis(stats.percentile(95))
                + ","
                + millis(stats.percentile(99))
                + ","
                + footprint.sizeBytes
                + ","
                + footprint.fileCount
                + ","
                + footprint.sstFiles
                + ","
                + footprint.walFiles
                + ","
                + footprint.estimateLiveDataSizeBytes
                + ","
                + footprint.totalSstFilesSizeBytes
                + ","
                + footprint.pendingCompactionBytes
                + ","
                + footprint.globalEstimateKeys
                + ","
                + footprint.branchEstimateKeys
                + ","
                + footprint.lockEstimateKeys
                + ","
                + configDigest(options));
    }

    private static void printEnvironment(BenchmarkOptions options, Path rootPath, Path runPath) {
        System.out.println("RocksDB file mode benchmark");
        System.out.println("rootPath=" + rootPath);
        System.out.println("runPath=" + runPath);
        System.out.println("benchmarks=" + options.benchmarks);
        System.out.println("availableProcessors=" + Runtime.getRuntime().availableProcessors());
        System.out.println("jdk=" + System.getProperty("java.version") + " " + System.getProperty("java.vm.name"));
        System.out.println("os=" + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        System.out.println("rocksdbJniVersion=" + rocksDbJniVersion());
        System.out.println("syncWrite=" + options.syncWrite);
        System.out.println("enableRangeDelete=" + options.enableRangeDelete);
        System.out.println("globalCount=" + options.globalCount);
        System.out.println("branchPerGlobal=" + options.branchPerGlobal);
        System.out.println("lockPerBranch=" + options.lockPerBranch);
        System.out.println("warmupRounds=" + options.warmupRounds);
        System.out.println("measureRounds=" + options.measureRounds);
        System.out.println("batchSize=" + options.batchSize);
        System.out.println("sampleEvery=" + options.sampleEvery);
        System.out.println("cleanup=" + options.cleanup);
        System.out.println("seed=" + options.seed);
    }

    private static String rocksDbJniVersion() {
        Package rocksDbPackage = RocksDB.class.getPackage();
        String version = rocksDbPackage == null ? null : rocksDbPackage.getImplementationVersion();
        if (version != null) {
            return version;
        }
        try {
            java.security.CodeSource codeSource =
                    RocksDB.class.getProtectionDomain().getCodeSource();
            if (codeSource == null || codeSource.getLocation() == null) {
                return "unknown";
            }
            String fileName =
                    Paths.get(codeSource.getLocation().toURI()).getFileName().toString();
            String prefix = "rocksdbjni-";
            String suffix = ".jar";
            if (fileName.startsWith(prefix) && fileName.endsWith(suffix)) {
                return fileName.substring(prefix.length(), fileName.length() - suffix.length());
            }
            return fileName;
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String configDigest(BenchmarkOptions options) {
        String raw = "syncWrite=" + options.syncWrite
                + ",enableRangeDelete=" + options.enableRangeDelete
                + ",globalCount=" + options.globalCount
                + ",branchPerGlobal=" + options.branchPerGlobal
                + ",lockPerBranch=" + options.lockPerBranch;
        int hash = raw.hashCode();
        return String.format(Locale.ROOT, "%08x", hash);
    }

    private static String millis(long nanos) {
        return String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0D);
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static void assertTrue(boolean value, String message) {
        if (!value) {
            throw new IllegalStateException(message);
        }
    }

    private static void assertEquals(long expected, long actual, String message) {
        if (expected != actual) {
            throw new IllegalStateException(message + ", expected:" + expected + ", actual:" + actual);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(path)) {
            stream.forEach(paths::add);
        }
        Collections.sort(paths, Comparator.reverseOrder());
        for (Path current : paths) {
            Files.deleteIfExists(current);
        }
    }

    @SuppressWarnings("unchecked")
    private static void restoreEnvironment(Object originalEnvironment) throws Exception {
        Field field = ObjectHolder.class.getDeclaredField("OBJECT_MAP");
        field.setAccessible(true);
        Map<String, Object> objectMap = (Map<String, Object>) field.get(ObjectHolder.INSTANCE);
        if (originalEnvironment == null) {
            objectMap.remove(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT);
        } else {
            objectMap.put(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, originalEnvironment);
        }
    }

    private interface BenchmarkTask {
        void run() throws Exception;
    }

    private static final class BenchmarkDataSet {
        private final List<GlobalSession> globalSessions;
        private final Map<String, List<BranchSession>> branchSessions;

        private BenchmarkDataSet(List<GlobalSession> globalSessions, Map<String, List<BranchSession>> branchSessions) {
            this.globalSessions = globalSessions;
            this.branchSessions = branchSessions;
        }

        private static BenchmarkDataSet create(BenchmarkOptions options, int round) {
            List<GlobalSession> globals = new ArrayList<>(options.globalCount);
            Map<String, List<BranchSession>> branches = new LinkedHashMap<>();
            long baseBeginTime =
                    System.currentTimeMillis() - options.globalCount - round - Math.floorMod(options.seed, 1000L);
            for (int i = 0; i < options.globalCount; i++) {
                GlobalSession globalSession = globalSession(i, round, options.seed, baseBeginTime + i);
                globals.add(globalSession);
                List<BranchSession> globalBranches = new ArrayList<>(options.branchPerGlobal);
                for (int branchIndex = 0; branchIndex < options.branchPerGlobal; branchIndex++) {
                    globalBranches.add(branchSession(globalSession, i, branchIndex, options.lockPerBranch));
                }
                branches.put(globalSession.getXid(), globalBranches);
            }
            return new BenchmarkDataSet(globals, branches);
        }

        private GlobalSession pick(int index) {
            return globalSessions.get(Math.floorMod(index, globalSessions.size()));
        }

        private List<BranchSession> branchesOf(GlobalSession globalSession) {
            Collection<BranchSession> branches = branchSessions.get(globalSession.getXid());
            if (branches == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(branches);
        }

        private List<BranchSession> allBranches() {
            List<BranchSession> branches = new ArrayList<>();
            for (List<BranchSession> globalBranches : branchSessions.values()) {
                branches.addAll(globalBranches);
            }
            return branches;
        }

        private int countByStatus(GlobalStatus status) {
            int count = 0;
            for (GlobalSession globalSession : globalSessions) {
                if (globalSession.getStatus() == status) {
                    count++;
                }
            }
            return count;
        }

        private BranchSession conflictBranch(BranchSession holder) {
            BranchSession branchSession = new BranchSession(BranchType.AT);
            long transactionId = holder.getTransactionId() + 10_000_000L;
            branchSession.setXid("127.0.0.1:8091:" + transactionId);
            branchSession.setTransactionId(transactionId);
            branchSession.setBranchId(holder.getBranchId() + 10_000_000L);
            branchSession.setStatus(BranchStatus.Registered);
            branchSession.setResourceId(holder.getResourceId());
            branchSession.setLockKey(holder.getLockKey());
            return branchSession;
        }

        private static GlobalSession globalSession(int index, int round, long seed, long beginTime) {
            GlobalSession globalSession = new GlobalSession(
                    "benchmark-app", "benchmark-group", "phase4-tx-" + seed + "-" + round + "-" + index, 60000);
            globalSession.setStatus(STATUSES[index % STATUSES.length]);
            globalSession.setBeginTime(beginTime);
            return globalSession;
        }

        private static BranchSession branchSession(
                GlobalSession globalSession, int globalIndex, int branchIndex, int lockPerBranch) {
            BranchSession branchSession = new BranchSession(BranchType.AT);
            branchSession.setXid(globalSession.getXid());
            branchSession.setTransactionId(globalSession.getTransactionId());
            branchSession.setBranchId(globalIndex * 1000L + branchIndex + 1L);
            branchSession.setStatus(BranchStatus.Registered);
            branchSession.setResourceId("jdbc:mysql://127.0.0.1/benchmark");
            branchSession.setLockKey(lockKey(globalIndex, branchIndex, lockPerBranch));
            return branchSession;
        }

        private static String lockKey(int globalIndex, int branchIndex, int lockPerBranch) {
            if (lockPerBranch <= 0) {
                return "";
            }
            StringBuilder builder = new StringBuilder("benchmark_table:");
            for (int i = 0; i < lockPerBranch; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append(globalIndex)
                        .append('_')
                        .append(branchIndex)
                        .append('_')
                        .append(i);
            }
            return builder.toString();
        }
    }

    private static final class OperationStats {
        private final int sampleEvery;
        private long ops;
        private long totalNanos;
        private long[] samples = new long[128];
        private int sampleCount;

        private OperationStats(int sampleEvery) {
            this.sampleEvery = Math.max(1, sampleEvery);
        }

        private void record(long nanos) {
            ops++;
            totalNanos += nanos;
            if (ops % sampleEvery == 0) {
                addSample(nanos);
            }
        }

        private long ops() {
            return ops;
        }

        private long totalNanos() {
            return totalNanos;
        }

        private double opsPerSecond() {
            if (totalNanos <= 0L) {
                return 0D;
            }
            return ops * 1_000_000_000.0D / totalNanos;
        }

        private long percentile(int percentile) {
            if (sampleCount == 0) {
                return ops == 0L ? 0L : totalNanos / ops;
            }
            long[] sortedSamples = Arrays.copyOf(samples, sampleCount);
            Arrays.sort(sortedSamples);
            int index = (int) Math.ceil(sortedSamples.length * percentile / 100.0D) - 1;
            return sortedSamples[Math.max(0, Math.min(index, sortedSamples.length - 1))];
        }

        private void addSample(long nanos) {
            if (sampleCount == samples.length) {
                samples = Arrays.copyOf(samples, samples.length * 2);
            }
            samples[sampleCount++] = nanos;
        }
    }

    private static final class DbFootprint {
        private final long sizeBytes;
        private final long fileCount;
        private final long sstFiles;
        private final long walFiles;
        private final long estimateLiveDataSizeBytes;
        private final long totalSstFilesSizeBytes;
        private final long pendingCompactionBytes;
        private final long globalEstimateKeys;
        private final long branchEstimateKeys;
        private final long lockEstimateKeys;

        private DbFootprint(
                long sizeBytes,
                long fileCount,
                long sstFiles,
                long walFiles,
                long estimateLiveDataSizeBytes,
                long totalSstFilesSizeBytes,
                long pendingCompactionBytes,
                long globalEstimateKeys,
                long branchEstimateKeys,
                long lockEstimateKeys) {
            this.sizeBytes = sizeBytes;
            this.fileCount = fileCount;
            this.sstFiles = sstFiles;
            this.walFiles = walFiles;
            this.estimateLiveDataSizeBytes = estimateLiveDataSizeBytes;
            this.totalSstFilesSizeBytes = totalSstFilesSizeBytes;
            this.pendingCompactionBytes = pendingCompactionBytes;
            this.globalEstimateKeys = globalEstimateKeys;
            this.branchEstimateKeys = branchEstimateKeys;
            this.lockEstimateKeys = lockEstimateKeys;
        }

        private static DbFootprint from(Path path) throws IOException {
            if (path == null || !Files.exists(path)) {
                return empty();
            }
            final long[] values = new long[4];
            try (Stream<Path> stream = Files.walk(path)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    values[0] += size(file);
                    values[1]++;
                    String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
                    if (name.endsWith(".sst")) {
                        values[2]++;
                    }
                    if (name.endsWith(".log")) {
                        values[3]++;
                    }
                });
            }
            RocksDBStoreDiagnostics diagnostics = diagnostics(path);
            return new DbFootprint(
                    values[0],
                    values[1],
                    values[2],
                    values[3],
                    diagnostics.getProperty(RocksDBStoreDiagnostics.ESTIMATE_LIVE_DATA_SIZE),
                    diagnostics.getProperty(RocksDBStoreDiagnostics.TOTAL_SST_FILES_SIZE),
                    diagnostics.getProperty(RocksDBStoreDiagnostics.ESTIMATE_PENDING_COMPACTION_BYTES),
                    diagnostics.getColumnFamilyProperty(
                            RocksDBColumnFamily.GLOBAL_SESSION, RocksDBStoreDiagnostics.ESTIMATE_NUM_KEYS),
                    diagnostics.getColumnFamilyProperty(
                            RocksDBColumnFamily.BRANCH_SESSION, RocksDBStoreDiagnostics.ESTIMATE_NUM_KEYS),
                    diagnostics.getColumnFamilyProperty(
                            RocksDBColumnFamily.LOCK, RocksDBStoreDiagnostics.ESTIMATE_NUM_KEYS));
        }

        private static DbFootprint empty() {
            return new DbFootprint(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        private static RocksDBStoreDiagnostics diagnostics(Path path) {
            try (RocksDBStoreEngine engine = open(path, false)) {
                return engine.diagnostics();
            } catch (Exception e) {
                return RocksDBStoreEngine.closedDiagnostics();
            }
        }

        private static long size(Path file) {
            try {
                return Files.size(file);
            } catch (IOException e) {
                return 0L;
            }
        }
    }

    private static final class BenchmarkOptions {
        private final int globalCount;
        private final int branchPerGlobal;
        private final int lockPerBranch;
        private final boolean syncWrite;
        private final boolean enableRangeDelete;
        private final boolean cleanup;
        private final int warmupRounds;
        private final int measureRounds;
        private final int batchSize;
        private final int sampleEvery;
        private final long seed;
        private final String dbPath;
        private final Set<String> benchmarks;

        private BenchmarkOptions(
                int globalCount,
                int branchPerGlobal,
                int lockPerBranch,
                boolean syncWrite,
                boolean enableRangeDelete,
                boolean cleanup,
                int warmupRounds,
                int measureRounds,
                int batchSize,
                int sampleEvery,
                long seed,
                String dbPath,
                Set<String> benchmarks) {
            this.globalCount = positive(globalCount, "globalCount");
            this.branchPerGlobal = nonNegative(branchPerGlobal, "branchPerGlobal");
            this.lockPerBranch = nonNegative(lockPerBranch, "lockPerBranch");
            this.syncWrite = syncWrite;
            this.enableRangeDelete = enableRangeDelete;
            this.cleanup = cleanup;
            this.warmupRounds = nonNegative(warmupRounds, "warmupRounds");
            this.measureRounds = positive(measureRounds, "measureRounds");
            this.batchSize = positive(batchSize, "batchSize");
            this.sampleEvery = positive(sampleEvery, "sampleEvery");
            this.seed = seed;
            this.dbPath = dbPath;
            this.benchmarks = benchmarks;
        }

        private static BenchmarkOptions parse(String[] args) {
            Map<String, String> values = parseArgs(args);
            return new BenchmarkOptions(
                    intValue(values, "globalCount", 1000),
                    intValue(values, "branchPerGlobal", 2),
                    intValue(values, "lockPerBranch", 2),
                    booleanValue(values, "syncWrite", false),
                    booleanValue(values, "enableRangeDelete", false),
                    booleanValue(values, "cleanup", false),
                    intValue(values, "warmupRounds", 1),
                    intValue(values, "measureRounds", 3),
                    intValue(values, "batchSize", 100),
                    intValue(values, "sampleEvery", 1),
                    longValue(values, "seed", 20260606L),
                    stringValue(values, "dbPath", null),
                    parseBenchmarks(stringValue(values, "benchmark", "all")));
        }

        private BenchmarkOptions withAtLeastOneLock() {
            return new BenchmarkOptions(
                    globalCount,
                    Math.max(1, branchPerGlobal),
                    Math.max(1, lockPerBranch),
                    syncWrite,
                    enableRangeDelete,
                    cleanup,
                    warmupRounds,
                    measureRounds,
                    batchSize,
                    sampleEvery,
                    seed,
                    dbPath,
                    benchmarks);
        }

        private boolean isEnabled(String benchmark) {
            return benchmarks.contains(benchmark);
        }

        private int totalRounds() {
            return warmupRounds + measureRounds;
        }

        private Path rootPath() throws IOException {
            if (dbPath == null || dbPath.trim().isEmpty()) {
                return Files.createTempDirectory("seata-rocksdb-file-mode-benchmark-");
            }
            return Paths.get(dbPath);
        }

        private static Map<String, String> parseArgs(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("--")) {
                    continue;
                }
                String keyValue = arg.substring(2);
                int equalsIndex = keyValue.indexOf('=');
                if (equalsIndex >= 0) {
                    values.put(keyValue.substring(0, equalsIndex), keyValue.substring(equalsIndex + 1));
                    continue;
                }
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    values.put(keyValue, args[++i]);
                } else {
                    values.put(keyValue, "true");
                }
            }
            return values;
        }

        private static Set<String> parseBenchmarks(String value) {
            Set<String> result = new LinkedHashSet<>();
            if (value == null || value.trim().isEmpty() || "all".equalsIgnoreCase(value.trim())) {
                result.addAll(ALL_BENCHMARKS);
                return result;
            }
            for (String item : value.split(",")) {
                String benchmark = item.trim().toLowerCase(Locale.ROOT);
                if (benchmark.isEmpty()) {
                    continue;
                }
                if (!ALL_BENCHMARKS.contains(benchmark)) {
                    throw new IllegalArgumentException("Unsupported benchmark:" + benchmark);
                }
                result.add(benchmark);
            }
            if (result.isEmpty()) {
                result.addAll(ALL_BENCHMARKS);
            }
            return result;
        }

        private static int intValue(Map<String, String> values, String key, int defaultValue) {
            return Integer.parseInt(stringValue(values, key, Integer.toString(defaultValue)));
        }

        private static long longValue(Map<String, String> values, String key, long defaultValue) {
            return Long.parseLong(stringValue(values, key, Long.toString(defaultValue)));
        }

        private static boolean booleanValue(Map<String, String> values, String key, boolean defaultValue) {
            return Boolean.parseBoolean(stringValue(values, key, Boolean.toString(defaultValue)));
        }

        private static String stringValue(Map<String, String> values, String key, String defaultValue) {
            String value = values.get(key);
            if (value == null) {
                value = System.getProperty("rocksdb.file.benchmark." + key);
            }
            if (value == null || value.trim().isEmpty()) {
                return defaultValue;
            }
            return value.trim();
        }

        private static int positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        }

        private static int nonNegative(int value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be non-negative");
            }
            return value;
        }
    }
}
