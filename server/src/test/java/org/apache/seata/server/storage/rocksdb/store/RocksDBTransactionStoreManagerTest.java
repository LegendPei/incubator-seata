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
package org.apache.seata.server.storage.rocksdb.store;

import org.apache.seata.common.Constants;
import org.apache.seata.common.holder.ObjectHolder;
import org.apache.seata.config.ConfigurationCache;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.server.session.BranchSession;
import org.apache.seata.server.session.GlobalSession;
import org.apache.seata.server.session.SessionCondition;
import org.apache.seata.server.session.SessionHolder;
import org.apache.seata.server.session.SessionManager;
import org.apache.seata.server.storage.rocksdb.RocksDBColumnFamily;
import org.apache.seata.server.storage.rocksdb.RocksDBKeyCodec;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreConfig;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngine;
import org.apache.seata.server.storage.rocksdb.session.RocksDBSessionManager;
import org.apache.seata.server.store.TransactionStoreManager.LogOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

class RocksDBTransactionStoreManagerTest {

    @TempDir
    Path tempDir;

    private Object originalEnvironment;
    private SessionManager originalRootSessionManager;
    private Map<String, SessionManager> originalSessionManagerMap;

    @BeforeEach
    void beforeEach() throws Exception {
        originalEnvironment = ObjectHolder.INSTANCE.getObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT);
        ObjectHolder.INSTANCE.setObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, new MockEnvironment());
        ConfigurationCache.clear();
        originalRootSessionManager = getRootSessionManager();
        originalSessionManagerMap = getSessionManagerMap();
    }

    @AfterEach
    void afterEach() throws Exception {
        ConfigurationCache.clear();
        restoreSessionHolder();
        restoreEnvironment();
    }

    @Test
    void testWriteAndReadGlobalSession() {
        try (RocksDBStoreEngine engine = open("global")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession globalSession = globalSession("tx-global", GlobalStatus.Begin);

            Assertions.assertTrue(storeManager.writeSession(LogOperation.GLOBAL_ADD, globalSession));

            GlobalSession actual = storeManager.readSession(globalSession.getXid(), false);
            Assertions.assertNotNull(actual);
            Assertions.assertEquals(globalSession.getXid(), actual.getXid());
            Assertions.assertEquals(GlobalStatus.Begin, actual.getStatus());
            Assertions.assertTrue(actual.isLazyLoadBranch());
        }
    }

    @Test
    void testReadGlobalSessionWithBranches() {
        try (RocksDBStoreEngine engine = open("branches")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession globalSession = globalSession("tx-branches", GlobalStatus.Begin);
            BranchSession branchSession = branchSession(globalSession, 1L);

            storeManager.writeSession(LogOperation.GLOBAL_ADD, globalSession);
            storeManager.writeSession(LogOperation.BRANCH_ADD, branchSession);

            GlobalSession actual = storeManager.readSession(globalSession.getXid(), true);

            Assertions.assertNotNull(actual);
            Assertions.assertEquals(1, actual.getBranchSessions().size());
            Assertions.assertEquals(
                    branchSession.getBranchId(),
                    actual.getBranchSessions().get(0).getBranchId());
        }
    }

    @Test
    void testRemoveBranchAndGlobalSession() {
        try (RocksDBStoreEngine engine = open("remove")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession globalSession = globalSession("tx-remove", GlobalStatus.Begin);
            BranchSession branch1 = branchSession(globalSession, 1L);
            BranchSession branch2 = branchSession(globalSession, 2L);

            storeManager.writeSession(LogOperation.GLOBAL_ADD, globalSession);
            storeManager.writeSession(LogOperation.BRANCH_ADD, branch1);
            storeManager.writeSession(LogOperation.BRANCH_ADD, branch2);
            storeManager.writeSession(LogOperation.BRANCH_REMOVE, branch1);

            Assertions.assertEquals(
                    1,
                    storeManager
                            .readSession(globalSession.getXid(), true)
                            .getBranchSessions()
                            .size());

            storeManager.writeSession(LogOperation.GLOBAL_REMOVE, globalSession);

            Assertions.assertNull(storeManager.readSession(globalSession.getXid(), true));
            Assertions.assertTrue(engine.prefixScan(
                            RocksDBColumnFamily.BRANCH_SESSION, RocksDBKeyCodec.encodeXidPrefix(globalSession.getXid()))
                    .isEmpty());
        }
    }

    @Test
    void testReadByStatus() {
        try (RocksDBStoreEngine engine = open("status")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession begin = globalSession("tx-begin", GlobalStatus.Begin);
            GlobalSession committed = globalSession("tx-committed", GlobalStatus.Committed);

            storeManager.writeSession(LogOperation.GLOBAL_ADD, begin);
            storeManager.writeSession(LogOperation.GLOBAL_ADD, committed);

            List<GlobalSession> actual = storeManager.readSession(new GlobalStatus[] {GlobalStatus.Begin}, false);

            Assertions.assertEquals(1, actual.size());
            Assertions.assertEquals(begin.getXid(), actual.get(0).getXid());
        }
    }

    @Test
    void testLazyReadLoadsRetryBranchesOnDemand() throws Exception {
        try (RocksDBStoreEngine engine = open("lazy-retry-branches")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            setRootSessionManager(new RocksDBSessionManager("root.data", engine));
            GlobalSession committing = globalSession("tx-commit-retry", GlobalStatus.CommitRetrying);
            GlobalSession rollbacking = globalSession("tx-rollbacking", GlobalStatus.Rollbacking);
            BranchSession committingBranch = branchSession(committing, 1L);
            BranchSession rollbackingBranch = branchSession(rollbacking, 2L);

            storeManager.writeSession(LogOperation.GLOBAL_ADD, committing);
            storeManager.writeSession(LogOperation.BRANCH_ADD, committingBranch);
            storeManager.writeSession(LogOperation.GLOBAL_ADD, rollbacking);
            storeManager.writeSession(LogOperation.BRANCH_ADD, rollbackingBranch);

            SessionCondition condition = new SessionCondition(GlobalStatus.CommitRetrying, GlobalStatus.Rollbacking);
            condition.setLazyLoadBranch(true);
            List<GlobalSession> actual = storeManager.readSession(condition);

            Assertions.assertEquals(2, actual.size());
            for (GlobalSession globalSession : actual) {
                Assertions.assertTrue(globalSession.isLazyLoadBranch());
                Assertions.assertEquals(1, globalSession.getBranchSessions().size());
            }
        }
    }

    @Test
    void testReadBySessionConditionXid() {
        try (RocksDBStoreEngine engine = open("condition")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession globalSession = globalSession("tx-condition", GlobalStatus.Begin);
            storeManager.writeSession(LogOperation.GLOBAL_ADD, globalSession);

            List<GlobalSession> actual = storeManager.readSession(new SessionCondition(globalSession.getXid()));

            Assertions.assertEquals(1, actual.size());
            Assertions.assertEquals(globalSession.getXid(), actual.get(0).getXid());
        }
    }

    @Test
    void testReadBySessionConditionTransactionIdAndOvertime() {
        try (RocksDBStoreEngine engine = open("condition-filter")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession oldSession = globalSession("tx-old", GlobalStatus.Begin);
            oldSession.setBeginTime(System.currentTimeMillis() - 60000);
            GlobalSession newSession = globalSession("tx-new", GlobalStatus.Begin);

            storeManager.writeSession(LogOperation.GLOBAL_ADD, oldSession);
            storeManager.writeSession(LogOperation.GLOBAL_ADD, newSession);

            SessionCondition condition = new SessionCondition();
            condition.setTransactionId(oldSession.getTransactionId());
            condition.setOverTimeAliveMills(1000L);
            List<GlobalSession> actual = storeManager.readSession(condition);

            Assertions.assertEquals(1, actual.size());
            Assertions.assertEquals(oldSession.getXid(), actual.get(0).getXid());
        }
    }

    @Test
    void testReadByEmptySessionConditionScansAll() {
        try (RocksDBStoreEngine engine = open("condition-all")) {
            RocksDBTransactionStoreManager storeManager = new RocksDBTransactionStoreManager(engine);
            GlobalSession begin = globalSession("tx-begin-all", GlobalStatus.Begin);
            GlobalSession committed = globalSession("tx-committed-all", GlobalStatus.Committed);

            storeManager.writeSession(LogOperation.GLOBAL_ADD, begin);
            storeManager.writeSession(LogOperation.GLOBAL_ADD, committed);

            List<GlobalSession> actual = storeManager.readSession(new SessionCondition());

            Assertions.assertEquals(2, actual.size());
        }
    }

    private RocksDBStoreEngine open(String name) {
        return RocksDBStoreEngine.open(
                new RocksDBStoreConfig(tempDir.resolve(name).toString(), true));
    }

    private GlobalSession globalSession(String name, GlobalStatus status) {
        GlobalSession globalSession = new GlobalSession("app", "group", name, 60000);
        globalSession.setStatus(status);
        return globalSession;
    }

    private BranchSession branchSession(GlobalSession globalSession, long branchId) {
        BranchSession branchSession = new BranchSession(BranchType.AT);
        branchSession.setXid(globalSession.getXid());
        branchSession.setTransactionId(globalSession.getTransactionId());
        branchSession.setBranchId(branchId);
        branchSession.setStatus(BranchStatus.Registered);
        branchSession.setResourceId("jdbc:mysql://127.0.0.1/db");
        branchSession.setLockKey("t_order:1");
        return branchSession;
    }

    @SuppressWarnings("unchecked")
    private void restoreEnvironment() throws Exception {
        Field field = ObjectHolder.class.getDeclaredField("OBJECT_MAP");
        field.setAccessible(true);
        Map<String, Object> objectMap = (Map<String, Object>) field.get(ObjectHolder.INSTANCE);
        if (originalEnvironment == null) {
            objectMap.remove(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT);
        } else {
            objectMap.put(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, originalEnvironment);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, SessionManager> getSessionManagerMap() throws Exception {
        Field field = SessionHolder.class.getDeclaredField("SESSION_MANAGER_MAP");
        field.setAccessible(true);
        return (Map<String, SessionManager>) field.get(null);
    }

    private SessionManager getRootSessionManager() throws Exception {
        Field field = SessionHolder.class.getDeclaredField("ROOT_SESSION_MANAGER");
        field.setAccessible(true);
        return (SessionManager) field.get(null);
    }

    private void setRootSessionManager(SessionManager sessionManager) throws Exception {
        Field rootField = SessionHolder.class.getDeclaredField("ROOT_SESSION_MANAGER");
        rootField.setAccessible(true);
        rootField.set(null, sessionManager);
        Field mapField = SessionHolder.class.getDeclaredField("SESSION_MANAGER_MAP");
        mapField.setAccessible(true);
        mapField.set(null, null);
    }

    private void restoreSessionHolder() throws Exception {
        Field rootField = SessionHolder.class.getDeclaredField("ROOT_SESSION_MANAGER");
        rootField.setAccessible(true);
        rootField.set(null, originalRootSessionManager);
        Field mapField = SessionHolder.class.getDeclaredField("SESSION_MANAGER_MAP");
        mapField.setAccessible(true);
        mapField.set(null, originalSessionManagerMap);
    }
}
