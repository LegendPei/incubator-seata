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
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shared RocksDB engine for file store engine.
 */
public class RocksDBStoreEngine implements AutoCloseable {

    public static final int FORMAT_VERSION = 1;

    private static final int DELETE_BATCH_SIZE = 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStoreEngine.class);
    private static final byte[] FORMAT_VERSION_KEY = "format_version".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLEAN_SHUTDOWN_KEY = "clean_shutdown".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLEAN_SHUTDOWN_TRUE = "true".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLEAN_SHUTDOWN_FALSE = "false".getBytes(StandardCharsets.UTF_8);
    private static final String[] DB_LONG_PROPERTIES = {
        RocksDBStoreDiagnostics.ESTIMATE_LIVE_DATA_SIZE,
        RocksDBStoreDiagnostics.TOTAL_SST_FILES_SIZE,
        RocksDBStoreDiagnostics.ESTIMATE_PENDING_COMPACTION_BYTES,
        RocksDBStoreDiagnostics.CUR_SIZE_ACTIVE_MEM_TABLE,
        RocksDBStoreDiagnostics.CUR_SIZE_ALL_MEM_TABLES,
        RocksDBStoreDiagnostics.NUM_LIVE_VERSIONS,
        RocksDBStoreDiagnostics.SIZE_ALL_MEM_TABLES,
        RocksDBStoreDiagnostics.ESTIMATE_TABLE_READERS_MEM,
        RocksDBStoreDiagnostics.BACKGROUND_ERRORS,
        RocksDBStoreDiagnostics.NUM_RUNNING_COMPACTIONS,
        RocksDBStoreDiagnostics.NUM_RUNNING_FLUSHES,
        RocksDBStoreDiagnostics.ACTUAL_DELAYED_WRITE_RATE,
        RocksDBStoreDiagnostics.IS_WRITE_STOPPED
    };
    private static final String[] COLUMN_FAMILY_LONG_PROPERTIES = {
        RocksDBStoreDiagnostics.ESTIMATE_NUM_KEYS,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL0,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL1,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL2,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL3,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL4,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL5,
        RocksDBStoreDiagnostics.NUM_FILES_AT_LEVEL6,
        RocksDBStoreDiagnostics.NUM_IMMUTABLE_MEM_TABLE,
        RocksDBStoreDiagnostics.MEM_TABLE_FLUSH_PENDING,
        RocksDBStoreDiagnostics.COMPACTION_PENDING
    };

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDBStoreConfig config;
    private final Map<RocksDBColumnFamily, ColumnFamilyHandle> handles = new EnumMap<>(RocksDBColumnFamily.class);
    private final DBOptions dbOptions;
    private final Map<RocksDBColumnFamily, ColumnFamilyOptions> columnFamilyOptions;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final Cache blockCache;
    private final Statistics statistics;
    private final RocksDBWalSyncController walSyncController;
    private final RocksDB db;
    private final boolean lastShutdownClean;

    private volatile boolean closed;

    private RocksDBStoreEngine(RocksDBStoreConfig config) {
        this.config = config;
        DBOptions openedDbOptions = null;
        Map<RocksDBColumnFamily, ColumnFamilyOptions> openedColumnFamilyOptions =
                new EnumMap<>(RocksDBColumnFamily.class);
        ReadOptions openedReadOptions = null;
        WriteOptions openedWriteOptions = null;
        Cache openedBlockCache = null;
        Statistics openedStatistics = null;
        RocksDBWalSyncController openedWalSyncController = RocksDBWalSyncController.disabled();
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
            if (config.getDbWriteBufferSize() > 0) {
                openedDbOptions.setDbWriteBufferSize(config.getDbWriteBufferSize());
            }
            if (config.getMaxTotalWalSize() > 0) {
                openedDbOptions.setMaxTotalWalSize(config.getMaxTotalWalSize());
            }
            if (config.isEnableStatistics()) {
                openedStatistics = new Statistics();
                openedDbOptions.setStatistics(openedStatistics);
            }

            if (config.getBlockCacheSize() > 0) {
                openedBlockCache = new LRUCache(config.getBlockCacheSize());
            }
            openedReadOptions = new ReadOptions();
            openedWriteOptions = new WriteOptions().setDisableWAL(false).setSync(config.isSyncWrite());

            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (RocksDBColumnFamily columnFamily : RocksDBColumnFamily.values()) {
                ColumnFamilyOptions options = new ColumnFamilyOptions();
                if (openedBlockCache != null) {
                    options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(openedBlockCache));
                }
                applyColumnFamilyOptions(options, config, columnFamily);
                openedColumnFamilyOptions.put(columnFamily, options);
                descriptors.add(new ColumnFamilyDescriptor(columnFamily.getNameBytes(), options));
            }

            openedDb = RocksDB.open(openedDbOptions, config.getDbPath(), descriptors, openedHandles);
            for (int i = 0; i < RocksDBColumnFamily.values().length; i++) {
                openedHandleMap.put(RocksDBColumnFamily.values()[i], openedHandles.get(i));
            }
            boolean metadataUpdated =
                    initMetadata(openedDb, openedHandleMap.get(RocksDBColumnFamily.METADATA), openedWriteOptions);
            boolean openedLastShutdownClean =
                    isCleanShutdown(openedDb, openedHandleMap.get(RocksDBColumnFamily.METADATA));
            boolean dirtyShutdownMarkerUpdated = markDirtyShutdownDurably(
                    openedDb, openedHandleMap.get(RocksDBColumnFamily.METADATA), openedWriteOptions);
            openedWalSyncController = RocksDBWalSyncController.create(openedDb, config);
            if (metadataUpdated && !dirtyShutdownMarkerUpdated) {
                openedWalSyncController.afterWrite();
            }
            dbOptions = openedDbOptions;
            columnFamilyOptions = Collections.unmodifiableMap(new EnumMap<>(openedColumnFamilyOptions));
            readOptions = openedReadOptions;
            writeOptions = openedWriteOptions;
            blockCache = openedBlockCache;
            statistics = openedStatistics;
            walSyncController = openedWalSyncController;
            db = openedDb;
            lastShutdownClean = openedLastShutdownClean;
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
            if (config.isSyncWrite() && config.getWalSyncMode().isPeriodic()) {
                LOGGER.info(
                        "RocksDB periodic WAL sync is ignored because syncWrite is enabled, path:{}",
                        config.getDbPath());
            }
            RocksDBStoreMetrics.tryRegister(this);
        } catch (StoreException e) {
            closeQuietly(openedWalSyncController);
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
            closeQuietly(openedWalSyncController);
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

    public boolean wasLastShutdownClean() {
        return lastShutdownClean;
    }

    public long getDbWriteBufferSize() {
        return dbOptions.dbWriteBufferSize();
    }

    public long getMaxTotalWalSize() {
        return dbOptions.maxTotalWalSize();
    }

    public long getColumnFamilyWriteBufferSize(RocksDBColumnFamily columnFamily) {
        ColumnFamilyOptions options = columnFamilyOptions.get(columnFamily);
        if (options == null) {
            throw new StoreException("RocksDB column family options not found:" + columnFamily);
        }
        return options.writeBufferSize();
    }

    public boolean isSyncWrite() {
        return config.isSyncWrite();
    }

    public RocksDB getDB() {
        return db;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns the database path configured for this engine.
     */
    public String getDbPath() {
        return config.getDbPath();
    }

    /**
     * Returns an unmodifiable list of column family names opened by this engine.
     */
    public List<String> getColumnFamilyNames() {
        return Collections.unmodifiableList(Arrays.stream(RocksDBColumnFamily.values())
                .map(RocksDBColumnFamily::getName)
                .collect(Collectors.toList()));
    }

    /**
     * Returns block cache memory usage in bytes, or 0 if block cache is not enabled or engine is closed.
     */
    public long getBlockCacheUsage() {
        if (blockCache == null || closed) {
            return 0L;
        }
        try {
            return blockCache.getUsage();
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * Returns block cache pinned memory usage in bytes, or 0 if block cache is not enabled or engine is closed.
     */
    public long getBlockCachePinnedUsage() {
        if (blockCache == null || closed) {
            return 0L;
        }
        try {
            return blockCache.getPinnedUsage();
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * Returns block cache configured capacity in bytes, or 0 if block cache is not enabled or engine is closed.
     */
    public long getBlockCacheCapacity() {
        if (blockCache == null || closed) {
            return 0L;
        }
        return config.getBlockCacheSize();
    }

    public RocksDBStoreDiagnostics diagnostics() {
        if (closed) {
            return closedDiagnostics(config);
        }
        Map<String, Long> properties = new LinkedHashMap<>();
        Map<RocksDBColumnFamily, Map<String, Long>> columnFamilyProperties = new EnumMap<>(RocksDBColumnFamily.class);
        List<String> errors = new ArrayList<>();
        for (String property : DB_LONG_PROPERTIES) {
            properties.put(property, readLongProperty(property, errors));
        }
        for (RocksDBColumnFamily columnFamily : RocksDBColumnFamily.values()) {
            Map<String, Long> values = new LinkedHashMap<>();
            for (String property : COLUMN_FAMILY_LONG_PROPERTIES) {
                values.put(property, readLongProperty(columnFamily, property, errors));
            }
            columnFamilyProperties.put(columnFamily, values);
        }
        return new RocksDBStoreDiagnostics(
                config.getDbPath(),
                FORMAT_VERSION,
                rocksDBVersion(),
                config.isSyncWrite(),
                false,
                config.tuningSummary(),
                properties,
                columnFamilyProperties,
                errors,
                getBlockCacheUsage(),
                getBlockCachePinnedUsage(),
                getBlockCacheCapacity(),
                walSyncController.stats());
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
            afterWrite();
        } catch (RocksDBException e) {
            throw new StoreException(e, "write RocksDB failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void delete(RocksDBColumnFamily columnFamily, byte[] key) {
        try {
            db.delete(handle(columnFamily), writeOptions, key);
            afterWrite();
        } catch (RocksDBException e) {
            throw new StoreException(e, "delete RocksDB failed, columnFamily:" + columnFamily.getName());
        }
    }

    public void write(WriteBatch batch) {
        try {
            db.write(writeOptions, batch);
            afterWrite();
        } catch (RocksDBException e) {
            throw new StoreException(e, "write RocksDB batch failed");
        }
    }

    private void afterWrite() {
        walSyncController.afterWrite();
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

    public ScanStats scanByPrefix(
            RocksDBColumnFamily columnFamily,
            byte[] seekKey,
            byte[] prefix,
            int limit,
            RocksDBEntryFilter filter,
            RocksDBEntryConsumer consumer) {
        Objects.requireNonNull(seekKey, "seekKey must not be null");
        Objects.requireNonNull(prefix, "prefix must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        int rowsScanned = 0;
        int rowsReturned = 0;
        boolean limitReached = false;
        try (RocksIterator iterator = db.newIterator(handle(columnFamily), readOptions)) {
            for (iterator.seek(seekKey); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!RocksDBKeyCodec.startsWith(key, prefix)) {
                    break;
                }
                byte[] value = iterator.value();
                rowsScanned++;
                if (filter != null && !filter.shouldContinue(key, value)) {
                    break;
                }
                consumer.accept(copy(key), copy(value));
                rowsReturned++;
                if (limit > 0 && rowsReturned >= limit) {
                    limitReached = true;
                    break;
                }
            }
            iterator.status();
            return new ScanStats(rowsScanned, rowsReturned, limitReached);
        } catch (RocksDBException e) {
            throw new StoreException(e, "scan RocksDB prefix failed, columnFamily:" + columnFamily.getName());
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
        if (config.isEnableRangeDelete() && deleteRangeByPrefix(columnFamily, prefix)) {
            return;
        }
        scanDeleteByPrefix(columnFamily, prefix);
    }

    public boolean deleteByPrefix(WriteBatch batch, RocksDBColumnFamily columnFamily, byte[] prefix)
            throws RocksDBException {
        Objects.requireNonNull(batch, "batch must not be null");
        Objects.requireNonNull(prefix, "prefix must not be null");
        if (config.isEnableRangeDelete() && deleteRangeByPrefix(batch, columnFamily, prefix)) {
            return true;
        }
        scanDeleteByPrefix(batch, columnFamily, prefix);
        return false;
    }

    /**
     * Delete all keys matching the given prefix using RocksDB deleteRange.
     * <p>
     * When {@code rangeDeleteCompactAfterDelete} is enabled, a synchronous
     * {@code compactRange} is issued immediately after the delete to reclaim
     * disk space. Note that if {@code compactRange} fails, the preceding
     * {@code deleteRange} has already been committed — the data is deleted
     * but a {@link StoreException} is still thrown to signal the compaction
     * failure.
     */
    public boolean deleteRangeByPrefix(RocksDBColumnFamily columnFamily, byte[] prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        byte[] end = RocksDBKeyCodec.prefixEnd(prefix);
        if (end == null) {
            return false;
        }
        try {
            db.deleteRange(handle(columnFamily), writeOptions, prefix, end);
            afterWrite();
            if (config.isRangeDeleteCompactAfterDelete()) {
                db.compactRange(handle(columnFamily), prefix, end);
            }
            return true;
        } catch (RocksDBException e) {
            throw new StoreException(e, "delete RocksDB range failed, columnFamily:" + columnFamily.getName());
        }
    }

    public boolean deleteRangeByPrefix(WriteBatch batch, RocksDBColumnFamily columnFamily, byte[] prefix)
            throws RocksDBException {
        Objects.requireNonNull(batch, "batch must not be null");
        Objects.requireNonNull(prefix, "prefix must not be null");
        byte[] end = RocksDBKeyCodec.prefixEnd(prefix);
        if (end == null) {
            return false;
        }
        batch.deleteRange(handle(columnFamily), prefix, end);
        return true;
    }

    private void scanDeleteByPrefix(RocksDBColumnFamily columnFamily, byte[] prefix) {
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
                afterWrite();
            } catch (RocksDBException e) {
                throw new StoreException(e, "delete RocksDB prefix failed, columnFamily:" + columnFamily.getName());
            }
        }
    }

    private void scanDeleteByPrefix(WriteBatch batch, RocksDBColumnFamily columnFamily, byte[] prefix)
            throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = handle(columnFamily);
        for (RocksDBEntry entry : prefixScan(columnFamily, prefix)) {
            batch.delete(columnFamilyHandle, entry.getKey());
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
        RocksDBStoreMetrics.unregister(this);
        RuntimeException syncFailure = null;
        try {
            markCleanShutdown(true);
            walSyncController.afterWrite();
            walSyncController.close();
        } catch (RuntimeException e) {
            syncFailure = e;
            try {
                markDirtyShutdownDurably(db, handle(RocksDBColumnFamily.METADATA), writeOptions);
            } catch (Exception markerFailure) {
                syncFailure.addSuppressed(markerFailure);
            }
        } finally {
            closeQuietly(
                    new ArrayList<>(handles.values()),
                    db,
                    writeOptions,
                    readOptions,
                    columnFamilyOptions,
                    dbOptions,
                    blockCache,
                    statistics);
        }
        if (syncFailure != null) {
            throw syncFailure;
        }
    }

    public static RocksDBStoreDiagnostics closedDiagnostics() {
        return closedDiagnostics(null);
    }

    private static RocksDBStoreDiagnostics closedDiagnostics(RocksDBStoreConfig config) {
        String dbPath = config == null ? null : config.getDbPath();
        boolean syncWrite = config != null && config.isSyncWrite();
        String tuningSummary = config == null ? null : config.tuningSummary();
        return new RocksDBStoreDiagnostics(
                dbPath,
                FORMAT_VERSION,
                rocksDBVersion(),
                syncWrite,
                true,
                tuningSummary,
                new LinkedHashMap<>(),
                new EnumMap<>(RocksDBColumnFamily.class),
                new ArrayList<>());
    }

    private static boolean initMetadata(RocksDB db, ColumnFamilyHandle metadataHandle, WriteOptions writeOptions)
            throws RocksDBException {
        byte[] existing = db.get(metadataHandle, FORMAT_VERSION_KEY);
        if (existing == null) {
            db.put(
                    metadataHandle,
                    writeOptions,
                    FORMAT_VERSION_KEY,
                    Integer.toString(FORMAT_VERSION).getBytes(StandardCharsets.UTF_8));
            return true;
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
        return false;
    }

    private void markCleanShutdown(boolean clean) {
        try {
            db.put(
                    handle(RocksDBColumnFamily.METADATA),
                    writeOptions,
                    CLEAN_SHUTDOWN_KEY,
                    clean ? CLEAN_SHUTDOWN_TRUE : CLEAN_SHUTDOWN_FALSE);
        } catch (RocksDBException e) {
            throw new StoreException(e, "write RocksDB clean shutdown marker failed");
        }
    }

    private static boolean markCleanShutdown(
            RocksDB db, ColumnFamilyHandle metadataHandle, WriteOptions writeOptions, boolean clean)
            throws RocksDBException {
        byte[] expected = clean ? CLEAN_SHUTDOWN_TRUE : CLEAN_SHUTDOWN_FALSE;
        byte[] existing = db.get(metadataHandle, CLEAN_SHUTDOWN_KEY);
        if (Arrays.equals(existing, expected)) {
            return false;
        }
        db.put(metadataHandle, writeOptions, CLEAN_SHUTDOWN_KEY, expected);
        return true;
    }

    private static boolean markDirtyShutdownDurably(
            RocksDB db, ColumnFamilyHandle metadataHandle, WriteOptions writeOptions) throws RocksDBException {
        if (markCleanShutdown(db, metadataHandle, writeOptions, false)) {
            db.flushWal(true);
            return true;
        }
        return false;
    }

    private static boolean isCleanShutdown(RocksDB db, ColumnFamilyHandle metadataHandle) throws RocksDBException {
        return Arrays.equals(db.get(metadataHandle, CLEAN_SHUTDOWN_KEY), CLEAN_SHUTDOWN_TRUE);
    }

    private static void closeQuietly(
            List<ColumnFamilyHandle> handles,
            RocksDB db,
            WriteOptions writeOptions,
            ReadOptions readOptions,
            Map<RocksDBColumnFamily, ColumnFamilyOptions> columnFamilyOptions,
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
            for (ColumnFamilyOptions options : columnFamilyOptions.values()) {
                options.close();
            }
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
        closeQuietly(blockCache);
        closeQuietly(statistics);
    }

    private static void applyColumnFamilyOptions(
            ColumnFamilyOptions options, RocksDBStoreConfig config, RocksDBColumnFamily columnFamily) {
        long writeBufferSize = config.getWriteBufferSize(columnFamily);
        if (writeBufferSize > 0) {
            options.setWriteBufferSize(writeBufferSize);
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

    /**
     * Read a DB-level long property. Returns 0 on failure and logs the error.
     */
    public long getLongProperty(String property) {
        if (closed) {
            return 0L;
        }
        try {
            return db.getAggregatedLongProperty(property);
        } catch (RocksDBException e) {
            try {
                return db.getLongProperty(property);
            } catch (RocksDBException fallback) {
                LOGGER.debug("read RocksDB property failed, property:{}, message:{}", property, fallback.getMessage());
                return 0L;
            }
        }
    }

    /**
     * Read a column-family-level long property. Returns 0 on failure and logs the error.
     */
    public long getLongProperty(RocksDBColumnFamily columnFamily, String property) {
        if (closed) {
            return 0L;
        }
        try {
            return db.getLongProperty(handle(columnFamily), property);
        } catch (RocksDBException e) {
            LOGGER.debug(
                    "read RocksDB column family property failed, columnFamily:{}, property:{}, message:{}",
                    columnFamily.getName(),
                    property,
                    e.getMessage());
            return 0L;
        }
    }

    /**
     * Read a DB-level string property. Returns null on failure.
     */
    public String getProperty(String property) {
        if (closed) {
            return null;
        }
        try {
            return db.getProperty(property);
        } catch (RocksDBException e) {
            LOGGER.debug("read RocksDB string property failed, property:{}, message:{}", property, e.getMessage());
            return null;
        }
    }

    /**
     * Read a column-family-level string property. Returns null on failure.
     */
    public String getProperty(RocksDBColumnFamily columnFamily, String property) {
        if (closed) {
            return null;
        }
        try {
            return db.getProperty(handle(columnFamily), property);
        } catch (RocksDBException e) {
            LOGGER.debug(
                    "read RocksDB column family string property failed, columnFamily:{}, property:{}, message:{}",
                    columnFamily.getName(),
                    property,
                    e.getMessage());
            return null;
        }
    }

    private long readLongProperty(String property, List<String> errors) {
        try {
            return db.getAggregatedLongProperty(property);
        } catch (RocksDBException e) {
            try {
                return db.getLongProperty(property);
            } catch (RocksDBException fallback) {
                errors.add("read RocksDB property failed, property:" + property + ", message:" + fallback.getMessage());
                return 0L;
            }
        }
    }

    private long readLongProperty(RocksDBColumnFamily columnFamily, String property, List<String> errors) {
        try {
            return db.getLongProperty(handle(columnFamily), property);
        } catch (RocksDBException e) {
            errors.add("read RocksDB column family property failed, columnFamily:"
                    + columnFamily.getName()
                    + ", property:"
                    + property
                    + ", message:"
                    + e.getMessage());
            return 0L;
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

    public static class ScanStats {
        private final int rowsScanned;
        private final int rowsReturned;
        private final boolean limitReached;

        public ScanStats(int rowsScanned, int rowsReturned, boolean limitReached) {
            this.rowsScanned = rowsScanned;
            this.rowsReturned = rowsReturned;
            this.limitReached = limitReached;
        }

        public int getRowsScanned() {
            return rowsScanned;
        }

        public int getRowsReturned() {
            return rowsReturned;
        }

        public boolean isLimitReached() {
            return limitReached;
        }
    }

    /**
     * Predicate that controls whether a bounded scan should continue.
     */
    @FunctionalInterface
    public interface RocksDBEntryFilter {

        boolean shouldContinue(byte[] key, byte[] value) throws RocksDBException;
    }

    /**
     * Streaming RocksDB entry consumer.
     */
    @FunctionalInterface
    public interface RocksDBEntryConsumer {

        void accept(byte[] key, byte[] value) throws RocksDBException;
    }
}
