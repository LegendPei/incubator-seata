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
package org.apache.seata.server.storage.rocksdb.lock;

import org.apache.seata.common.exception.StoreException;
import org.apache.seata.common.loader.LoadLevel;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.lock.Locker;
import org.apache.seata.core.model.LockStatus;
import org.apache.seata.server.lock.AbstractLockManager;
import org.apache.seata.server.session.BranchSession;
import org.apache.seata.server.session.GlobalSession;

/**
 * The RocksDB lock manager placeholder for file store engine.
 */
@LoadLevel(name = "rocksdb")
public class RocksDBLockManager extends AbstractLockManager {

    private static final String NOT_IMPLEMENTED = "RocksDB lock manager is not implemented in Phase1";

    @Override
    public boolean acquireLock(BranchSession branchSession) throws TransactionException {
        throw notImplemented();
    }

    @Override
    public boolean acquireLock(BranchSession branchSession, boolean autoCommit, boolean skipCheckLock)
            throws TransactionException {
        throw notImplemented();
    }

    @Override
    public boolean releaseLock(BranchSession branchSession) throws TransactionException {
        throw notImplemented();
    }

    @Override
    public boolean releaseGlobalSessionLock(GlobalSession globalSession) throws TransactionException {
        throw notImplemented();
    }

    @Override
    public boolean isLockable(String xid, String resourceId, String lockKey) throws TransactionException {
        throw notImplemented();
    }

    @Override
    public void cleanAllLocks() throws TransactionException {
        throw notImplemented();
    }

    @Override
    public void updateLockStatus(String xid, LockStatus lockStatus) {
        throw notImplemented();
    }

    @Override
    protected Locker getLocker(BranchSession branchSession) {
        throw notImplemented();
    }

    private StoreException notImplemented() {
        return new StoreException(NOT_IMPLEMENTED);
    }
}
