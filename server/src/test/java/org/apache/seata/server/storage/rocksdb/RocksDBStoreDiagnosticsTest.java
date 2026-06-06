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

import org.apache.seata.metrics.Measurement;
import org.apache.seata.metrics.registry.compact.CompactRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class RocksDBStoreDiagnosticsTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void afterEach() {
        RocksDBStoreEngineFactory.destroy();
        RocksDBStoreMetrics.unregister(null);
    }

    @Test
    void testDiagnosticsSnapshotContainsRocksDBProperties() {
        try (RocksDBStoreEngine engine = open("diagnostics")) {
            engine.put(
                    RocksDBColumnFamily.GLOBAL_SESSION,
                    RocksDBKeyCodec.encodeXid("xid-diagnostics"),
                    "global".getBytes(StandardCharsets.UTF_8));
            engine.put(
                    RocksDBColumnFamily.BRANCH_SESSION,
                    RocksDBKeyCodec.encodeBranch("xid-diagnostics", 1L),
                    "branch".getBytes(StandardCharsets.UTF_8));
            engine.flush();

            RocksDBStoreDiagnostics diagnostics = engine.diagnostics();

            Assertions.assertFalse(diagnostics.isClosed());
            Assertions.assertEquals(tempDir.resolve("diagnostics").toString(), diagnostics.getDbPath());
            Assertions.assertEquals(RocksDBStoreEngine.FORMAT_VERSION, diagnostics.getFormatVersion());
            Assertions.assertNotNull(diagnostics.getRocksDBVersion());
            Assertions.assertTrue(diagnostics.getProperties()
                    .containsKey(RocksDBStoreDiagnostics.ESTIMATE_LIVE_DATA_SIZE));
            Assertions.assertTrue(diagnostics.getColumnFamilyProperties()
                    .containsKey(RocksDBColumnFamily.GLOBAL_SESSION));
            Assertions.assertTrue(diagnostics
                    .getColumnFamilyProperties()
                    .get(RocksDBColumnFamily.GLOBAL_SESSION)
                    .containsKey(RocksDBStoreDiagnostics.ESTIMATE_NUM_KEYS));
        }
    }

    @Test
    void testDiagnosticsAfterCloseIsSafe() {
        RocksDBStoreEngine engine = open("closed");
        engine.close();

        RocksDBStoreDiagnostics diagnostics = engine.diagnostics();

        Assertions.assertTrue(diagnostics.isClosed());
        Assertions.assertEquals(tempDir.resolve("closed").toString(), diagnostics.getDbPath());
        Assertions.assertEquals(0L, diagnostics.getProperty(RocksDBStoreDiagnostics.ESTIMATE_LIVE_DATA_SIZE));
    }

    @Test
    void testMetricsRegistryCanCollectGaugesAndStaySafeAfterClose() {
        CompactRegistry registry = new CompactRegistry();
        try {
            RocksDBStoreEngine engine = open("metrics");
            engine.put(
                    RocksDBColumnFamily.GLOBAL_SESSION,
                    RocksDBKeyCodec.encodeXid("xid-metrics"),
                    "global".getBytes(StandardCharsets.UTF_8));
            engine.flush();

            RocksDBStoreMetrics.register(registry, engine);
            List<Measurement> measurements = measurements(registry);

            Assertions.assertFalse(measurements.isEmpty());
            Assertions.assertTrue(measurements.stream()
                    .anyMatch(measurement -> measurement.getId().toString().contains("estimateLiveDataSizeBytes")));

            engine.close();

            Assertions.assertDoesNotThrow(() -> measurements(registry));
        } finally {
            registry.clearUp();
        }
    }

    @Test
    void testMetricsRegisterWithNullRegistryIsNoop() {
        try (RocksDBStoreEngine engine = open("metrics-disabled")) {
            Assertions.assertDoesNotThrow(() -> RocksDBStoreMetrics.register(null, engine));
        }
    }

    private RocksDBStoreEngine open(String name) {
        return RocksDBStoreEngine.open(new RocksDBStoreConfig(tempDir.resolve(name).toString(), true));
    }

    private List<Measurement> measurements(CompactRegistry registry) {
        List<Measurement> measurements = new ArrayList<>();
        for (Measurement measurement : registry.measure()) {
            measurements.add(measurement);
        }
        return measurements;
    }
}
