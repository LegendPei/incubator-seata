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
package org.apache.seata.server.storage.rocksdb.maintenance;

import org.apache.seata.common.exception.StoreException;
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.server.session.GlobalSession;
import org.apache.seata.server.storage.rocksdb.RocksDBColumnFamily;
import org.apache.seata.server.storage.rocksdb.RocksDBKeyCodec;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngine;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngineFactory;
import org.apache.seata.server.storage.rocksdb.RocksDBValueCodec;
import org.apache.seata.server.storage.rocksdb.index.RocksDBIndexManager;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

/**
 * Maintenance service for RocksDB file store engine.
 *
 * <p>Provides checkpoint creation, consistency verification and explicit index repair.
 * All operations are intended to be triggered explicitly (e.g., via admin API or benchmark);
 * none of them run automatically on the startup path.
 */
public class RocksDBMaintenanceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBMaintenanceService.class);
    private static final String CHECKPOINT_METADATA_FILE = "seata-checkpoint-metadata.txt";
    private static final byte[] EMPTY_PREFIX = new byte[0];
    private static final RocksDBColumnFamily[] VERIFY_COLUMN_FAMILIES = {
        RocksDBColumnFamily.GLOBAL_SESSION,
        RocksDBColumnFamily.BRANCH_SESSION,
        RocksDBColumnFamily.LOCK,
        RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
        RocksDBColumnFamily.GLOBAL_TIMEOUT_INDEX,
        RocksDBColumnFamily.TRANSACTION_ID_INDEX,
        RocksDBColumnFamily.LOCK_BRANCH_INDEX
    };

    private final RocksDBStoreEngine storeEngine;
    private final RocksDBIndexManager indexManager;

    public RocksDBMaintenanceService() {
        this(RocksDBStoreEngineFactory.getInstance());
    }

    public RocksDBMaintenanceService(RocksDBStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.indexManager = new RocksDBIndexManager(storeEngine);
    }

    /**
     * Create a RocksDB checkpoint at the given directory.
     *
     * <p>The target directory must not exist or must be empty. A metadata file is written
     * into the checkpoint directory after the checkpoint is created successfully.
     *
     * @param checkpointPath target directory for the checkpoint
     * @param flush          whether to flush memtables before creating the checkpoint
     */
    public void createCheckpoint(Path checkpointPath, boolean flush) {
        if (checkpointPath == null) {
            throw new StoreException("checkpoint path must not be null");
        }
        if (Files.exists(checkpointPath) && !isDirectoryEmpty(checkpointPath)) {
            throw new StoreException("checkpoint target directory already exists and is not empty:" + checkpointPath);
        }
        // Ensure parent directory exists; RocksDB createCheckpoint creates the target directory itself
        Path parent = checkpointPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                throw new StoreException(e, "create checkpoint parent directory failed:" + parent);
            }
        }
        if (flush) {
            storeEngine.flush();
        }
        storeEngine.createCheckpoint(checkpointPath.toString());
        try {
            writeCheckpointMetadata(checkpointPath);
            LOGGER.info(
                    "RocksDB checkpoint created, path:{}, flush:{}, source:{}",
                    checkpointPath,
                    flush,
                    storeEngine.getConfig().getDbPath());
        } catch (IOException e) {
            throw new StoreException(e, "write checkpoint metadata failed, path:" + checkpointPath);
        }
    }

    /**
     * Run an explicit full consistency verification.
     */
    public RocksDBVerifyReport verifyCurrentState() {
        return verifyCurrentState(RocksDBVerifyOptions.full());
    }

    /**
     * Verify the current state without retaining full database-sized reference sets.
     */
    public RocksDBVerifyReport verifyCurrentState(RocksDBVerifyOptions options) {
        RocksDBVerifyReport.Builder report = RocksDBVerifyReport.builder(options);
        verifyMetadata(report);
        switch (options.getMode()) {
            case SAMPLE:
                verifySample(options, report);
                break;
            case PAGE:
                verifyPage(options, report);
                break;
            case FULL:
            default:
                verifyFull(report);
                break;
        }
        RocksDBVerifyReport result = report.build();
        LOGGER.info("RocksDB verify completed, {}", result);
        return result;
    }

    private void verifyMetadata(RocksDBVerifyReport.Builder report) {
        byte[] versionBytes =
                storeEngine.get(RocksDBColumnFamily.METADATA, "format_version".getBytes(StandardCharsets.UTF_8));
        String expected = Integer.toString(RocksDBStoreEngine.FORMAT_VERSION);
        if (versionBytes == null) {
            report.error("missing format_version in metadata");
        } else if (!expected.equals(new String(versionBytes, StandardCharsets.UTF_8))) {
            report.error("format_version mismatch, expected:" + expected + ", found:"
                    + new String(versionBytes, StandardCharsets.UTF_8));
        }
    }

    private void verifyFull(RocksDBVerifyReport.Builder report) {
        for (RocksDBColumnFamily columnFamily : VERIFY_COLUMN_FAMILIES) {
            scanFamily(columnFamily, EMPTY_PREFIX, 0, report);
        }
        report.complete(true);
    }

    private void verifySample(RocksDBVerifyOptions options, RocksDBVerifyReport.Builder report) {
        for (RocksDBColumnFamily columnFamily : VERIFY_COLUMN_FAMILIES) {
            scanFamily(columnFamily, EMPTY_PREFIX, options.getLimit(), report);
        }
        report.complete(false);
    }

    private void verifyPage(RocksDBVerifyOptions options, RocksDBVerifyReport.Builder report) {
        RocksDBVerifyCursor cursor = options.getCursor();
        int familyIndex = cursor == null ? 0 : familyIndex(cursor.getColumnFamily());
        int remaining = options.getLimit();
        for (int i = familyIndex; i < VERIFY_COLUMN_FAMILIES.length; i++) {
            RocksDBColumnFamily columnFamily = VERIFY_COLUMN_FAMILIES[i];
            byte[] seekKey =
                    cursor != null && cursor.getColumnFamily() == columnFamily ? cursor.getSeekKey() : EMPTY_PREFIX;
            byte[][] lastKey = new byte[1][];
            RocksDBStoreEngine.ScanStats stats =
                    storeEngine.scanByPrefix(columnFamily, seekKey, EMPTY_PREFIX, remaining, null, (key, value) -> {
                        lastKey[0] = key;
                        verifyEntry(columnFamily, key, value, report);
                    });
            remaining -= stats.getRowsReturned();
            if (remaining == 0 && lastKey[0] != null) {
                report.nextCursor(new RocksDBVerifyCursor(columnFamily, nextSeekKey(lastKey[0])));
                report.complete(false);
                return;
            }
            cursor = null;
        }
        report.complete(true);
    }

    private void scanFamily(
            RocksDBColumnFamily columnFamily, byte[] seekKey, int limit, RocksDBVerifyReport.Builder report) {
        storeEngine.scanByPrefix(
                columnFamily,
                seekKey,
                EMPTY_PREFIX,
                limit,
                null,
                (key, value) -> verifyEntry(columnFamily, key, value, report));
    }

    private void verifyEntry(
            RocksDBColumnFamily columnFamily, byte[] key, byte[] value, RocksDBVerifyReport.Builder report) {
        report.checkedRecord(isIndex(columnFamily));
        switch (columnFamily) {
            case GLOBAL_SESSION:
                verifyGlobal(key, value, report);
                break;
            case BRANCH_SESSION:
                verifyBranch(key, report);
                break;
            case LOCK:
                verifyLock(key, value, report);
                break;
            case GLOBAL_STATUS_INDEX:
                verifyStatusIndex(key, value, report);
                break;
            case GLOBAL_TIMEOUT_INDEX:
                verifyTimeoutIndex(key, value, report);
                break;
            case TRANSACTION_ID_INDEX:
                verifyTransactionIdIndex(key, value, report);
                break;
            case LOCK_BRANCH_INDEX:
                verifyLockIndex(key, value, report);
                break;
            default:
                break;
        }
    }

    private void verifyGlobal(byte[] key, byte[] value, RocksDBVerifyReport.Builder report) {
        report.checkedGlobal();
        GlobalVerifyEntry global = decodeGlobal(value, report, "global session");
        if (global == null) {
            return;
        }
        if (!Arrays.equals(key, RocksDBKeyCodec.encodeXid(global.xid))) {
            report.error("global session key mismatch, xid:" + global.xid);
        }
        byte[] xidBytes = global.xid.getBytes(StandardCharsets.UTF_8);
        byte[] statusValue = storeEngine.get(
                RocksDBColumnFamily.GLOBAL_STATUS_INDEX,
                RocksDBKeyCodec.encodeGlobalStatusIndex(global.status, global.beginTime, global.xid));
        if (!Arrays.equals(xidBytes, statusValue)) {
            report.missingStatusIndex("missing global status index, xid:" + global.xid);
        }
        byte[] transactionValue = storeEngine.get(
                RocksDBColumnFamily.TRANSACTION_ID_INDEX,
                RocksDBKeyCodec.encodeTransactionIdIndex(global.transactionId));
        if (!Arrays.equals(xidBytes, transactionValue)) {
            report.missingTransactionIdIndex("missing transaction id index, xid:" + global.xid);
        }
        if (global.status == GlobalStatus.Begin) {
            byte[] timeoutValue = storeEngine.get(
                    RocksDBColumnFamily.GLOBAL_TIMEOUT_INDEX,
                    RocksDBKeyCodec.encodeGlobalTimeoutIndex(global.deadlineMillis(), global.xid));
            if (!Arrays.equals(xidBytes, timeoutValue)) {
                report.missingTimeoutIndex("missing global timeout index, xid:" + global.xid);
            }
        }
    }

    private void verifyBranch(byte[] key, RocksDBVerifyReport.Builder report) {
        report.checkedBranch();
        String xid = RocksDBKeyCodec.extractXidFromBranchKey(key);
        if (xid == null
                || storeEngine.get(RocksDBColumnFamily.GLOBAL_SESSION, RocksDBKeyCodec.encodeXid(xid)) == null) {
            report.orphanBranch("orphan branch session, xid:" + xid);
        }
    }

    private void verifyLock(byte[] key, byte[] value, RocksDBVerifyReport.Builder report) {
        report.checkedLock();
        LockVerifyEntry lock = decodeLock(value);
        if (lock == null
                || storeEngine.get(RocksDBColumnFamily.GLOBAL_SESSION, RocksDBKeyCodec.encodeXid(lock.xid)) == null) {
            report.orphanLock("orphan or invalid lock holder");
            return;
        }
        byte[] indexValue = storeEngine.get(
                RocksDBColumnFamily.LOCK_BRANCH_INDEX,
                RocksDBKeyCodec.encodeLockBranchIndex(lock.xid, lock.branchId, key));
        if (!Arrays.equals(key, indexValue)) {
            report.orphanLock("lock holder has no matching branch index, xid:" + lock.xid);
        }
    }

    private void verifyStatusIndex(byte[] key, byte[] value, RocksDBVerifyReport.Builder report) {
        try {
            String xid = RocksDBKeyCodec.extractXidFromStatusIndexKey(key);
            GlobalVerifyEntry global = readGlobal(xid);
            if (global == null
                    || !xid.equals(new String(value, StandardCharsets.UTF_8))
                    || global.status.getCode() != RocksDBKeyCodec.extractStatusCodeFromStatusIndexKey(key)
                    || global.beginTime != RocksDBKeyCodec.extractBeginTimeFromStatusIndexKey(key)) {
                report.staleStatusIndex("stale global status index, xid:" + xid);
            }
        } catch (Exception e) {
            report.staleStatusIndex("invalid global status index, message:" + e.getMessage());
        }
    }

    private void verifyTimeoutIndex(byte[] key, byte[] value, RocksDBVerifyReport.Builder report) {
        try {
            String xid = new String(value, StandardCharsets.UTF_8);
            GlobalVerifyEntry global = readGlobal(xid);
            if (global == null
                    || global.status != GlobalStatus.Begin
                    || !Arrays.equals(key, RocksDBKeyCodec.encodeGlobalTimeoutIndex(global.deadlineMillis(), xid))) {
                report.staleTimeoutIndex("stale global timeout index, xid:" + xid);
            }
        } catch (Exception e) {
            report.staleTimeoutIndex("invalid global timeout index, message:" + e.getMessage());
        }
    }

    private void verifyTransactionIdIndex(byte[] key, byte[] value, RocksDBVerifyReport.Builder report) {
        try {
            String xid = new String(value, StandardCharsets.UTF_8);
            GlobalVerifyEntry global = readGlobal(xid);
            if (global == null
                    || key.length != Long.BYTES
                    || ByteBuffer.wrap(key).getLong() != global.transactionId) {
                report.staleTransactionIdIndex("stale transaction id index, xid:" + xid);
            }
        } catch (Exception e) {
            report.staleTransactionIdIndex("invalid transaction id index, message:" + e.getMessage());
        }
    }

    private void verifyLockIndex(byte[] key, byte[] lockKey, RocksDBVerifyReport.Builder report) {
        byte[] lockValue = storeEngine.get(RocksDBColumnFamily.LOCK, lockKey);
        LockVerifyEntry lock = decodeLock(lockValue);
        if (lock == null
                || storeEngine.get(RocksDBColumnFamily.GLOBAL_SESSION, RocksDBKeyCodec.encodeXid(lock.xid)) == null
                || !Arrays.equals(key, RocksDBKeyCodec.encodeLockBranchIndex(lock.xid, lock.branchId, lockKey))) {
            report.staleLockIndex("stale lock branch index");
        }
    }

    private GlobalVerifyEntry readGlobal(String xid) {
        if (xid == null) {
            return null;
        }
        byte[] value = storeEngine.get(RocksDBColumnFamily.GLOBAL_SESSION, RocksDBKeyCodec.encodeXid(xid));
        return value == null ? null : decodeGlobal(value, null, "global session");
    }

    private static GlobalVerifyEntry decodeGlobal(
            byte[] value, RocksDBVerifyReport.Builder report, String description) {
        try {
            RocksDBValueCodec.DecodedValue decoded = RocksDBValueCodec.decode(value);
            if (decoded.getType() != RocksDBValueCodec.ValueType.GLOBAL_SESSION) {
                throw new StoreException("unexpected value type:" + decoded.getType());
            }
            GlobalSession session = new GlobalSession(null, null, null, 0, true);
            session.decode(decoded.getPayload());
            return new GlobalVerifyEntry(
                    session.getXid(),
                    session.getTransactionId(),
                    session.getStatus(),
                    session.getBeginTime(),
                    session.getTimeout());
        } catch (Exception e) {
            if (report != null) {
                report.error("failed to decode " + description + ", message:" + e.getMessage());
            }
            return null;
        }
    }

    private static LockVerifyEntry decodeLock(byte[] lockValue) {
        if (lockValue == null) {
            return null;
        }
        try {
            RocksDBValueCodec.DecodedValue decoded = RocksDBValueCodec.decode(lockValue);
            if (decoded.getType() != RocksDBValueCodec.ValueType.LOCK_HOLDER) {
                return null;
            }
            ByteBuffer buffer = ByteBuffer.wrap(decoded.getPayload());
            String xid = readString(buffer);
            buffer.getLong();
            long branchId = buffer.getLong();
            return xid == null ? null : new LockVerifyEntry(xid, branchId);
        } catch (Exception e) {
            return null;
        }
    }

    private static String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || buffer.remaining() < length) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static boolean isIndex(RocksDBColumnFamily columnFamily) {
        return columnFamily == RocksDBColumnFamily.GLOBAL_STATUS_INDEX
                || columnFamily == RocksDBColumnFamily.GLOBAL_TIMEOUT_INDEX
                || columnFamily == RocksDBColumnFamily.TRANSACTION_ID_INDEX
                || columnFamily == RocksDBColumnFamily.LOCK_BRANCH_INDEX;
    }

    private static int familyIndex(RocksDBColumnFamily columnFamily) {
        for (int i = 0; i < VERIFY_COLUMN_FAMILIES.length; i++) {
            if (VERIFY_COLUMN_FAMILIES[i] == columnFamily) {
                return i;
            }
        }
        throw new IllegalArgumentException("unsupported verify cursor column family:" + columnFamily);
    }

    private static byte[] nextSeekKey(byte[] key) {
        byte[] next = Arrays.copyOf(key, key.length + 1);
        next[key.length] = 0;
        return next;
    }

    /**
     * Rebuild secondary indexes from global session data.
     *
     * <p>This is an explicit repair action. It clears and rebuilds the
     * {@code GLOBAL_STATUS_INDEX} and {@code TRANSACTION_ID_INDEX} column families.
     */
    public void repairIndexes() {
        storeEngine.withMaintenanceLock(() -> {
            LOGGER.info("Rebuilding RocksDB secondary indexes");
            indexManager.rebuildFromGlobalSessions();
            LOGGER.info("RocksDB secondary indexes rebuilt");
            return null;
        });
    }

    private void writeCheckpointMetadata(Path checkpointPath) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("sourceDbPath=").append(storeEngine.getConfig().getDbPath()).append('\n');
        sb.append("formatVersion=").append(RocksDBStoreEngine.FORMAT_VERSION).append('\n');
        sb.append("createdAt=").append(Instant.now().toString()).append('\n');
        sb.append("columnFamilies=");
        RocksDBColumnFamily[] families = RocksDBColumnFamily.values();
        for (int i = 0; i < families.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(families[i].getName());
        }
        sb.append('\n');
        RocksDB.Version version = RocksDB.rocksdbVersion();
        sb.append("rocksdbVersion=")
                .append(version != null ? version.toString() : "unknown")
                .append('\n');
        sb.append("syncWrite=").append(storeEngine.getConfig().isSyncWrite()).append('\n');
        String seataVersion = RocksDBStoreEngine.class.getPackage() != null
                ? RocksDBStoreEngine.class.getPackage().getImplementationVersion()
                : null;
        sb.append("seataVersion=")
                .append(seataVersion != null ? seataVersion : "unknown")
                .append('\n');
        Files.write(
                checkpointPath.resolve(CHECKPOINT_METADATA_FILE), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private boolean isDirectoryEmpty(Path dir) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        } catch (IOException e) {
            throw new StoreException(e, "check directory failed:" + dir);
        }
    }

    private static final class GlobalVerifyEntry {
        final String xid;
        final long transactionId;
        final GlobalStatus status;
        final long beginTime;
        final long timeout;

        GlobalVerifyEntry(String xid, long transactionId, GlobalStatus status, long beginTime, long timeout) {
            this.xid = xid;
            this.transactionId = transactionId;
            this.status = status;
            this.beginTime = beginTime;
            this.timeout = timeout;
        }

        long deadlineMillis() {
            if (timeout > 0 && beginTime > Long.MAX_VALUE - timeout) {
                return Long.MAX_VALUE;
            }
            return beginTime + timeout;
        }
    }

    private static final class LockVerifyEntry {
        final String xid;
        final long branchId;

        LockVerifyEntry(String xid, long branchId) {
            this.xid = xid;
            this.branchId = branchId;
        }
    }
}
