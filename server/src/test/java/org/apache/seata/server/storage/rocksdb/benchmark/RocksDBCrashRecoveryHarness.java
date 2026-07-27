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
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.server.session.GlobalSession;
import org.apache.seata.server.storage.rocksdb.RocksDBColumnFamily;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreConfig;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngine;
import org.apache.seata.server.storage.rocksdb.maintenance.RocksDBMaintenanceService;
import org.apache.seata.server.storage.rocksdb.maintenance.RocksDBVerifyReport;
import org.apache.seata.server.storage.rocksdb.store.RocksDBTransactionStoreManager;
import org.apache.seata.server.store.TransactionStoreManager.LogOperation;
import org.springframework.mock.env.MockEnvironment;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/** Manual parent/child crash-recovery harness for WAL RPO experiments. */
public final class RocksDBCrashRecoveryHarness {

    private static final String WRITER = "writer";
    private static final String PARENT = "parent";

    private RocksDBCrashRecoveryHarness() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        String mode = options.getOrDefault("mode", PARENT);
        ObjectHolder.INSTANCE.setObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, new MockEnvironment());
        ConfigurationCache.clear();
        try {
            if (WRITER.equals(mode)) {
                writeUntilKilled(options);
                return;
            }
            runParent(options);
        } finally {
            ConfigurationCache.clear();
        }
    }

    private static void runParent(Map<String, String> options) throws Exception {
        Path dbPath = requiredPath(options, "dbPath");
        Path checkpoint = requiredPath(options, "checkpoint");
        Files.deleteIfExists(checkpoint);
        Process child = new ProcessBuilder(
                        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                        "-cp",
                        System.getProperty("java.class.path"),
                        RocksDBCrashRecoveryHarness.class.getName(),
                        "--mode=writer",
                        "--dbPath=" + dbPath,
                        "--checkpoint=" + checkpoint,
                        "--count=" + options.getOrDefault("count", "10000"),
                        "--checkpointAfter=" + options.getOrDefault("checkpointAfter", "1000"),
                        "--syncWrite=" + options.getOrDefault("syncWrite", "true"))
                .inheritIO()
                .start();
        waitForCheckpoint(checkpoint, child);
        child.destroyForcibly();
        child.waitFor();
        long expected = Long.parseLong(new String(Files.readAllBytes(checkpoint), StandardCharsets.UTF_8).trim());
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(new RocksDBStoreConfig(dbPath.toString(), true))) {
            RocksDBVerifyReport report = new RocksDBMaintenanceService(engine).verifyCurrentState();
            int recovered = engine.prefixScan(RocksDBColumnFamily.GLOBAL_SESSION, new byte[0]).size();
            System.out.println("R8_RESULT expected=" + expected + " recovered=" + recovered + " lost="
                    + Math.max(0L, expected - recovered) + " clean=" + report.isClean());
            if (!report.isClean() || recovered > expected) {
                throw new IllegalStateException("crash recovery verification failed:" + report);
            }
        }
    }

    private static void writeUntilKilled(Map<String, String> options) throws Exception {
        Path dbPath = requiredPath(options, "dbPath");
        Path checkpoint = requiredPath(options, "checkpoint");
        int count = Integer.parseInt(options.getOrDefault("count", "10000"));
        int checkpointAfter = Integer.parseInt(options.getOrDefault("checkpointAfter", "1000"));
        boolean syncWrite = Boolean.parseBoolean(options.getOrDefault("syncWrite", "true"));
        try (RocksDBStoreEngine engine = RocksDBStoreEngine.open(new RocksDBStoreConfig(dbPath.toString(), syncWrite))) {
            RocksDBTransactionStoreManager store = new RocksDBTransactionStoreManager(engine);
            for (int i = 1; i <= count; i++) {
                GlobalSession session = new GlobalSession("r8", "r8", "r8-crash-" + i, 60000);
                session.setStatus(GlobalStatus.Begin);
                session.setBeginTime(System.currentTimeMillis());
                store.writeSession(LogOperation.GLOBAL_ADD, session);
                if (i == checkpointAfter) {
                    Files.write(checkpoint, Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                    Thread.sleep(Long.MAX_VALUE);
                }
            }
        }
    }

    private static void waitForCheckpoint(Path checkpoint, Process child) throws Exception {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!Files.exists(checkpoint)) {
            if (!child.isAlive()) {
                throw new IllegalStateException("writer exited before checkpoint, exit=" + child.exitValue());
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("timed out waiting for checkpoint:" + checkpoint);
            }
            Thread.sleep(10L);
        }
    }

    private static Path requiredPath(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing --" + key);
        }
        return Paths.get(value);
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                continue;
            }
            int separator = arg.indexOf('=');
            if (separator > 2) {
                values.put(arg.substring(2, separator), arg.substring(separator + 1));
            }
        }
        return values;
    }
}
