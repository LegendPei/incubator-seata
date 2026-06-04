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
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
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
    private final RocksDB db;

    private volatile boolean closed;

    private RocksDBStoreEngine(RocksDBStoreConfig config) {
        this.config = config;
        DBOptions openedDbOptions = null;
        ColumnFamilyOptions openedColumnFamilyOptions = null;
        ReadOptions openedReadOptions = null;
        WriteOptions openedWriteOptions = null;
        RocksDB openedDb = null;
        List<ColumnFamilyHandle> openedHandles = new ArrayList<>();
        Map<RocksDBColumnFamily, ColumnFamilyHandle> openedHandleMap = new EnumMap<>(RocksDBColumnFamily.class);
        try {
            Files.createDirectories(Paths.get(config.getDbPath()));
            openedDbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            openedColumnFamilyOptions = new ColumnFamilyOptions();
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
            db = openedDb;
            handles.putAll(openedHandleMap);
            LOGGER.info(
                    "RocksDB file store engine opened, path:{}, columnFamilies:{}, formatVersion:{}, syncWrite:{}",
                    config.getDbPath(),
                    handles.keySet(),
                    FORMAT_VERSION,
                    config.isSyncWrite());
        } catch (StoreException e) {
            closeQuietly(
                    openedHandles,
                    openedDb,
                    openedWriteOptions,
                    openedReadOptions,
                    openedColumnFamilyOptions,
                    openedDbOptions);
            throw e;
        } catch (Exception e) {
            closeQuietly(
                    openedHandles,
                    openedDb,
                    openedWriteOptions,
                    openedReadOptions,
                    openedColumnFamilyOptions,
                    openedDbOptions);
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
            DBOptions dbOptions) {
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
}
