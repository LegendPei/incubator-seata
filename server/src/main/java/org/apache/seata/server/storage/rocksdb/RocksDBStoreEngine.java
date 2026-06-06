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
import org.apache.seata.common.util.StringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Shared RocksDB engine for file store engine.
 */
public class RocksDBStoreEngine implements AutoCloseable {

    public static final int FORMAT_VERSION = 1;

    private static final int DELETE_BATCH_SIZE = 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStoreEngine.class);
    private static final byte[] FORMAT_VERSION_KEY = "format_version".getBytes(StandardCharsets.UTF_8);

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDBStoreConfig config;
    private final Map<RocksDBColumnFamily, ColumnFamilyHandle> handles = new EnumMap<>(RocksDBColumnFamily.class);
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final Cache blockCache;
    private final Statistics statistics;
    private final RocksDB db;

    private volatile boolean closed;

    private RocksDBStoreEngine(RocksDBStoreConfig config) {
        this.config = config;
        DBOptions openedDbOptions = null;
        ColumnFamilyOptions openedColumnFamilyOptions = null;
        ReadOptions openedReadOptions = null;
        WriteOptions openedWriteOptions = null;
        BlockBasedTableConfig openedTableConfig = null;
        Cache openedBlockCache = null;
        Statistics openedStatistics = null;
        RocksDB openedDb = null;
        List<ColumnFamilyHandle> openedHandles = new ArrayList<>();
        Map<RocksDBColumnFamily, ColumnFamilyHandle> openedHandleMap = new EnumMap<>(RocksDBColumnFamily.class);
        try {
            Files.createDirectories(Paths.get(config.getDbPath()));
            openedDbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            if (config.getMaxBackgroundJobs() > 0) {
                openedDbOptions.setMaxBackgroundJobs(config.getMaxBackgroundJobs());
            }
            if (config.getMaxOpenFiles() > 0) {
                openedDbOptions.setMaxOpenFiles(config.getMaxOpenFiles());
            }
            if (config.isEnableStatistics()) {
                openedStatistics = new Statistics();
                openedDbOptions.setStatistics(openedStatistics);
            }

            openedColumnFamilyOptions = new ColumnFamilyOptions();
            if (config.getBlockCacheSize() > 0) {
                openedBlockCache = new LRUCache(config.getBlockCacheSize());
                openedTableConfig = new BlockBasedTableConfig().setBlockCache(openedBlockCache);
                openedColumnFamilyOptions.setTableFormatConfig(openedTableConfig);
            }
            applyColumnFamilyOptions(openedColumnFamilyOptions, config);
            openedReadOptions = new ReadOptions();
            openedWriteOptions = new WriteOptions().setDisableWAL(false).setSync(config.isSyncWrite());

            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (RocksDBColumnFamily columnFamily : RocksDBColumnFamily.values()) {
                descriptors.add(new ColumnFamilyDescriptor(columnFamily.getNameBytes(), openedColumnFamilyOptions));
            }

            openedDb = RocksDB.open(openedDbOptions, config.getDbPath(), descriptors, openedHandles);
            for (int i = 0; i < RocksDBColumnFamily.values().length; i++) {
                openedHandleMap.put(RocksDBColumnFamily.values()[i], openedHandles.get(i));
            }
            initMetadata(openedDb, openedHandleMap.get(RocksDBColumnFamily.METADATA), openedWriteOptions);
            dbOptions = openedDbOptions;
            columnFamilyOptions = openedColumnFamilyOptions;
            readOptions = openedReadOptions;
            writeOptions = openedWriteOptions;
            blockCache = openedBlockCache;
            statistics = openedStatistics;
            db = openedDb;
            handles.putAll(openedHandleMap);
            LOGGER.info(
                    "RocksDB file store engine opened, path:{}, columnFamilies:{}, formatVersion:{}, "
                            + "rocksDBVersion:{}, syncWrite:{}, options:{}",
                    config.getDbPath(),
                    handles.keySet(),
                    FORMAT_VERSION,
                    rocksDBVersion(),
                    config.isSyncWrite(),
                    config.tuningSummary());
        } catch (StoreException e) {
            closeQuietly(
                    openedHandles,
                    openedDb,
                    openedWriteOptions,
                    openedReadOptions,
                    openedColumnFamilyOptions,
                    openedDbOptions,
                    openedBlockCache,
                    openedStatistics);
            throw e;
        } catch (Exception e) {
            closeQuietly(
                    openedHandles,
                    openedDb,
                    openedWriteOptions,
                    openedReadOptions,
                    openedColumnFamilyOptions,
                    openedDbOptions,
                    openedBlockCache,
                    openedStatistics);
            throw new StoreException(e, "open RocksDB file store engine failed, path:" + config.getDbPath());
        }
    }

    public static RocksDBStoreEngine open(RocksDBStoreConfig config) {
        return new RocksDBStoreEngine(config);
    }

    public RocksDBStoreConfig getConfig() {
        return config;
    }

    public boolean isSyncWrite() {
        return config.isSyncWrite();
    }

    public byte[] get(RocksDBColumnFamily columnFamily, byte[] key) {
        try {
            return db.get(handle(columnFamily), key);
        } catch (RocksDBException e) {
            throw new StoreException(e, "read RocksDB failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void put(RocksDBColumnFamily columnFamily, byte[] key, byte[] value) {
        try {
            db.put(handle(columnFamily), writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new StoreException(e, "write RocksDB failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void delete(RocksDBColumnFamily columnFamily, byte[] key) {
        try {
            db.delete(handle(columnFamily), writeOptions, key);
        } catch (RocksDBException e) {
            throw new StoreException(e, "delete RocksDB failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void write(WriteBatch batch) {
        try {
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new StoreException(e, "write RocksDB batch failed");
        }
    }

    public List<RocksDBEntry> prefixScan(RocksDBColumnFamily columnFamily, byte[] prefix) {
        List<RocksDBEntry> entries = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(handle(columnFamily), readOptions)) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!RocksDBKeyCodec.startsWith(key, prefix)) {
                    break;
                }
                entries.add(new RocksDBEntry(copy(key), copy(iterator.value())));
            }
            iterator.status();
            return entries;
        } catch (RocksDBException e) {
            throw new StoreException(e, "scan RocksDB failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void scanByPrefix(RocksDBColumnFamily columnFamily, byte[] prefix, RocksDBEntryConsumer consumer) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        try (RocksIterator iterator = db.newIterator(handle(columnFamily), readOptions)) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!RocksDBKeyCodec.startsWith(key, prefix)) {
                    break;
                }
                consumer.accept(copy(key), copy(iterator.value()));
            }
            iterator.status();
        } catch (RocksDBException e) {
            throw new StoreException(e, "scan RocksDB prefix failed, columnFamily:" + columnFamily.getName());
        }
    }

    public boolean prefixExists(RocksDBColumnFamily columnFamily, byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        try (RocksIterator iterator = db.newIterator(handle(columnFamily), readOptions)) {
            iterator.seek(prefix);
            boolean exists = iterator.isValid() && RocksDBKeyCodec.startsWith(iterator.key(), prefix);
            iterator.status();
            return exists;
        } catch (RocksDBException e) {
            throw new StoreException(e, "check RocksDB prefix failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void deleteByPrefix(RocksDBColumnFamily columnFamily, byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        ColumnFamilyHandle columnFamilyHandle = handle(columnFamily);
        while (true) {
            int count = 0;
            try (RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions);
                    WriteBatch batch = new WriteBatch()) {
                for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();
                    if (!RocksDBKeyCodec.startsWith(key, prefix)) {
                        break;
                    }
                    batch.delete(columnFamilyHandle, copy(key));
                    count++;
                    if (count >= DELETE_BATCH_SIZE) {
                        break;
                    }
                }
                iterator.status();
                if (count == 0) {
                    return;
                }
                db.write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new StoreException(e, "delete RocksDB prefix failed, columnFamily:" + columnFamily.getName());
            }
        }
    }

    public void flush() {
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new StoreException(e, "flush RocksDB failed");
        }
    }

    public ColumnFamilyHandle handle(RocksDBColumnFamily columnFamily) {
        ColumnFamilyHandle handle = handles.get(columnFamily);
        if (handle == null) {
            throw new StoreException("RocksDB column family handle not found:" + columnFamily.getName());
        }
        return handle;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (ColumnFamilyHandle handle : handles.values()) {
            handle.close();
        }
        db.close();
        writeOptions.close();
        readOptions.close();
        columnFamilyOptions.close();
        dbOptions.close();
        closeQuietly(blockCache);
        closeQuietly(statistics);
    }

    private static void initMetadata(RocksDB db, ColumnFamilyHandle metadataHandle, WriteOptions writeOptions)
            throws RocksDBException {
        byte[] existing = db.get(metadataHandle, FORMAT_VERSION_KEY);
        if (existing == null) {
            db.put(
                    metadataHandle,
                    writeOptions,
                    FORMAT_VERSION_KEY,
                    Integer.toString(FORMAT_VERSION).getBytes(StandardCharsets.UTF_8));
            return;
        }
        int existingFormatVersion;
        try {
            existingFormatVersion = Integer.parseInt(new String(existing, StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            throw new StoreException(e, "invalid RocksDB format version metadata");
        }
        if (existingFormatVersion != FORMAT_VERSION) {
            throw new StoreException(
                    "unsupported RocksDB format version:" + existingFormatVersion + ", expected:" + FORMAT_VERSION);
        }
    }

    private static void closeQuietly(
            List<ColumnFamilyHandle> handles,
            RocksDB db,
            WriteOptions writeOptions,
            ReadOptions readOptions,
            ColumnFamilyOptions columnFamilyOptions,
            DBOptions dbOptions,
            Cache blockCache,
            Statistics statistics) {
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        if (db != null) {
            db.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (readOptions != null) {
            readOptions.close();
        }
        if (columnFamilyOptions != null) {
            columnFamilyOptions.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
        closeQuietly(blockCache);
        closeQuietly(statistics);
    }

    private static void applyColumnFamilyOptions(ColumnFamilyOptions options, RocksDBStoreConfig config) {
        if (config.getWriteBufferSize() > 0) {
            options.setWriteBufferSize(config.getWriteBufferSize());
        }
        if (config.getMaxWriteBufferNumber() > 0) {
            options.setMaxWriteBufferNumber(config.getMaxWriteBufferNumber());
        }
        if (config.getMinWriteBufferNumberToMerge() > 0) {
            options.setMinWriteBufferNumberToMerge(config.getMinWriteBufferNumberToMerge());
        }
        if (config.getTargetFileSizeBase() > 0) {
            options.setTargetFileSizeBase(config.getTargetFileSizeBase());
        }
        if (config.getLevel0FileNumCompactionTrigger() > 0) {
            options.setLevel0FileNumCompactionTrigger(config.getLevel0FileNumCompactionTrigger());
        }
        if (config.getLevel0SlowdownWritesTrigger() > 0) {
            options.setLevel0SlowdownWritesTrigger(config.getLevel0SlowdownWritesTrigger());
        }
        if (config.getLevel0StopWritesTrigger() > 0) {
            options.setLevel0StopWritesTrigger(config.getLevel0StopWritesTrigger());
        }
        if (config.isOptimizeFiltersForHits()) {
            options.optimizeFiltersForHits();
        }
        CompressionType compressionType = compressionType(config.getCompressionType());
        if (compressionType != null) {
            options.setCompressionType(compressionType);
        }
    }

    private static CompressionType compressionType(String configuredCompressionType) {
        if (StringUtils.isBlank(configuredCompressionType)) {
            return null;
        }
        String value = configuredCompressionType.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        if ("NONE".equals(value) || "NO".equals(value)) {
            value = "NO_COMPRESSION";
        } else if (!value.endsWith("_COMPRESSION")) {
            value = value + "_COMPRESSION";
        }
        try {
            return CompressionType.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new StoreException(e, "unsupported RocksDB compression type:" + configuredCompressionType);
        }
    }

    private static String rocksDBVersion() {
        RocksDB.Version version = RocksDB.rocksdbVersion();
        return version == null ? "unknown" : version.toString();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            LOGGER.warn("close RocksDB resource failed", e);
        }
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * RocksDB key-value entry.
     */
    public static class RocksDBEntry {
        private final byte[] key;
        private final byte[] value;

        public RocksDBEntry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return copy(key);
        }

        public byte[] getValue() {
            return copy(value);
        }
    }

    /**
     * Streaming RocksDB entry consumer.
     */
    @FunctionalInterface
    public interface RocksDBEntryConsumer {

        void accept(byte[] key, byte[] value) throws RocksDBException;
    }
}
