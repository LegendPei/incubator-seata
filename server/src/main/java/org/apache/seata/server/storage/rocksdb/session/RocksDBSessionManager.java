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
package org.apache.seata.server.storage.rocksdb.session;

import org.apache.seata.common.loader.LoadLevel;
import org.apache.seata.common.loader.Scope;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.server.lock.LockerManagerFactory;
import org.apache.seata.server.session.AbstractSessionManager;
import org.apache.seata.server.session.GlobalSession;
import org.apache.seata.server.session.SessionCondition;
import org.apache.seata.server.storage.rocksdb.RocksDBKeyCodec;
import org.apache.seata.server.storage.rocksdb.RocksDBLocalLocks;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngine;
import org.apache.seata.server.storage.rocksdb.store.RocksDBTransactionStoreManager;

import java.util.Collection;
import java.util.List;

/**
 * RocksDB-backed session manager for file store engine.
 */
@LoadLevel(name = "rocksdb", scope = Scope.PROTOTYPE)
public class RocksDBSessionManager extends AbstractSessionManager {

    private static final GlobalStatus[] RECOVERY_STATUSES = {
        GlobalStatus.UnKnown,
        GlobalStatus.Begin,
        GlobalStatus.Committing,
        GlobalStatus.CommitRetrying,
        GlobalStatus.Rollbacking,
        GlobalStatus.RollbackRetrying,
        GlobalStatus.TimeoutRollbacking,
        GlobalStatus.TimeoutRollbackRetrying,
        GlobalStatus.AsyncCommitting,
        GlobalStatus.Committed,
        GlobalStatus.CommitFailed,
        GlobalStatus.CommitRetryTimeout,
        GlobalStatus.Rollbacked,
        GlobalStatus.RollbackFailed,
        GlobalStatus.RollbackRetryTimeout,
        GlobalStatus.TimeoutRollbacked,
        GlobalStatus.TimeoutRollbackFailed,
        GlobalStatus.Finished,
        GlobalStatus.StopRollbackOrRollbackRetry,
        GlobalStatus.StopCommitOrCommitRetry,
        GlobalStatus.Deleting
    };

    private final RocksDBLocalLocks xidLocks;

    public RocksDBSessionManager() {
        this(null);
    }

    public RocksDBSessionManager(String name) {
        super(name);
        this.xidLocks = new RocksDBLocalLocks();
        this.transactionStoreManager = new RocksDBTransactionStoreManager(xidLocks);
    }

    public RocksDBSessionManager(String name, RocksDBStoreEngine storeEngine) {
        super(name);
        this.xidLocks = new RocksDBLocalLocks();
        this.transactionStoreManager = new RocksDBTransactionStoreManager(storeEngine, xidLocks);
    }

    @Override
    public GlobalSession findGlobalSession(String xid) {
        return findGlobalSession(xid, true);
    }

    @Override
    public GlobalSession findGlobalSession(String xid, boolean withBranchSessions) {
        return transactionStoreManager.readSession(xid, withBranchSessions);
    }

    @Override
    public void removeGlobalSession(GlobalSession session) throws TransactionException {
        if (!LockerManagerFactory.getLockManager().releaseGlobalSessionLock(session)) {
            throw new TransactionException("Release RocksDB global session lock failed, xid = " + session.getXid());
        }
        super.removeGlobalSession(session);
    }

    @Override
    public Collection<GlobalSession> allSessions() {
        return findGlobalSessions(new SessionCondition(RECOVERY_STATUSES));
    }

    public RecoveryPage readStartupRecoveryPage(RecoveryCursor cursor) {
        RocksDBTransactionStoreManager.RecoveryScanPage scanPage = ((RocksDBTransactionStoreManager)
                        transactionStoreManager)
                .readRecoveryPage(RECOVERY_STATUSES, cursor.storeCursor);
        RecoveryCursor continuation = scanPage.isExhausted() ? null : new RecoveryCursor(scanPage.getContinuation());
        return new RecoveryPage(scanPage.getSessions(), continuation, scanPage.isExhausted());
    }

    @Override
    public List<GlobalSession> findGlobalSessions(SessionCondition condition) {
        return transactionStoreManager.readSession(condition);
    }

    @Override
    public <T> T lockAndExecute(GlobalSession globalSession, GlobalSession.LockCallable<T> lockCallable)
            throws TransactionException {
        try (RocksDBLocalLocks.LockScope ignored = xidLocks.lock(RocksDBKeyCodec.encodeXid(globalSession.getXid()))) {
            return lockCallable.call();
        }
    }

    @Override
    public void destroy() {
        transactionStoreManager.shutdown();
    }

    public static final class RecoveryCursor {
        private final RocksDBTransactionStoreManager.RecoveryCursor storeCursor;

        private RecoveryCursor(RocksDBTransactionStoreManager.RecoveryCursor storeCursor) {
            this.storeCursor = storeCursor;
        }

        public static RecoveryCursor initial() {
            return new RecoveryCursor(null);
        }
    }

    public static final class RecoveryPage {
        private final List<GlobalSession> sessions;
        private final RecoveryCursor continuation;
        private final boolean exhausted;

        private RecoveryPage(List<GlobalSession> sessions, RecoveryCursor continuation, boolean exhausted) {
            this.sessions = sessions;
            this.continuation = continuation;
            this.exhausted = exhausted;
        }

        public List<GlobalSession> getSessions() {
            return sessions;
        }

        public RecoveryCursor getContinuation() {
            return continuation;
        }

        public boolean isExhausted() {
            return exhausted;
        }
    }
}
