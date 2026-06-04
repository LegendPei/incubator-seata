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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

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

    private RocksDBStoreEngine open(String name, boolean syncWrite) {
        return RocksDBStoreEngine.open(config(name, syncWrite));
    }

    private RocksDBStoreConfig config(String name, boolean syncWrite) {
        return new RocksDBStoreConfig(tempDir.resolve(name).toString(), syncWrite);
    }
}
