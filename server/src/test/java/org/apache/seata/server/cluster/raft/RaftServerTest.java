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
package org.apache.seata.server.cluster.raft;

import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.XID;
import org.apache.seata.common.store.LockMode;
import org.apache.seata.common.store.SessionMode;
import org.apache.seata.common.store.StoreMode;
import org.apache.seata.config.ConfigurationCache;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.server.BaseSpringBootTest;
import org.apache.seata.server.coordinator.DefaultCoordinator;
import org.apache.seata.server.lock.LockerManagerFactory;
import org.apache.seata.server.session.SessionHolder;
import org.apache.seata.server.storage.raft.session.RaftSessionManager;
import org.apache.seata.server.storage.raft.store.RaftVGroupMappingStoreManager;
import org.apache.seata.server.store.StoreConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

import static org.apache.seata.common.ConfigurationKeys.SERVER_RAFT_SSL_CLIENT_KEYSTORE_PATH;
import static org.apache.seata.common.ConfigurationKeys.SERVER_RAFT_SSL_ENABLED;
import static org.apache.seata.common.ConfigurationKeys.SERVER_RAFT_SSL_KMF_ALGORITHM;
import static org.apache.seata.common.ConfigurationKeys.SERVER_RAFT_SSL_SERVER_KEYSTORE_PATH;
import static org.apache.seata.common.ConfigurationKeys.SERVER_RAFT_SSL_TMF_ALGORITHM;

public class RaftServerTest extends BaseSpringBootTest {

    private static StoreMode originalStoreMode;
    private static SessionMode originalSessionMode;
    private static LockMode originalLockMode;
    private static String originalRaftPort;
    private static String originalRaftServerAddr;

    @BeforeAll
    public static void setUp(ApplicationContext context) {
        originalStoreMode = (StoreMode) ReflectionTestUtils.getField(StoreConfig.class, "storeMode");
        originalSessionMode = (SessionMode) ReflectionTestUtils.getField(StoreConfig.class, "sessionMode");
        originalLockMode = (LockMode) ReflectionTestUtils.getField(StoreConfig.class, "lockMode");
        originalRaftPort = System.getProperty("server.raftPort");
        originalRaftServerAddr = System.getProperty(ConfigurationKeys.SERVER_RAFT_SERVER_ADDR);
        DefaultCoordinator.getInstance().destroy();
        LockerManagerFactory.destroy();
    }

    @BeforeEach
    public void prepareSessionDependencies() throws IOException {
        System.setProperty("server.raftPort", "0");
        System.setProperty(ConfigurationKeys.SERVER_RAFT_SERVER_ADDR, "");
        ConfigurationCache.clear();
        StoreConfig.setStartupParameter("raft", "raft", "raft");
        // Provide real state-machine dependencies while leaving RaftServerManager uninitialized.
        ReflectionTestUtils.setField(
                SessionHolder.class,
                "ROOT_SESSION_MANAGER",
                new RaftSessionManager(SessionHolder.ROOT_SESSION_MANAGER_NAME));
        ReflectionTestUtils.setField(
                SessionHolder.class, "ROOT_VGROUP_MAPPING_MANAGER", new RaftVGroupMappingStoreManager());
        LockerManagerFactory.init(LockMode.RAFT);
    }

    @AfterEach
    public void destroy() {
        try {
            SessionHolder.destroy();
        } finally {
            LockerManagerFactory.destroy();
        }
    }

    @AfterAll
    public static void restoreStore() {
        ReflectionTestUtils.setField(StoreConfig.class, "storeMode", originalStoreMode);
        ReflectionTestUtils.setField(StoreConfig.class, "sessionMode", originalSessionMode);
        ReflectionTestUtils.setField(StoreConfig.class, "lockMode", originalLockMode);
        restoreProperty("server.raftPort", originalRaftPort);
        restoreProperty(ConfigurationKeys.SERVER_RAFT_SERVER_ADDR, originalRaftServerAddr);
        ConfigurationCache.clear();
        SessionHolder.init();
        LockerManagerFactory.init();
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    @Test
    public void initRaftServerStart() {
        Assertions.assertDoesNotThrow(() -> ConfigurationFactory.getInstance().getConfig(SERVER_RAFT_SSL_ENABLED));
        Assertions.assertDoesNotThrow(
                () -> ConfigurationFactory.getInstance().getConfig(SERVER_RAFT_SSL_CLIENT_KEYSTORE_PATH));
        Assertions.assertDoesNotThrow(
                () -> ConfigurationFactory.getInstance().getConfig(SERVER_RAFT_SSL_SERVER_KEYSTORE_PATH));
        Assertions.assertDoesNotThrow(
                () -> ConfigurationFactory.getInstance().getConfig(SERVER_RAFT_SSL_KMF_ALGORITHM));
        Assertions.assertDoesNotThrow(
                () -> ConfigurationFactory.getInstance().getConfig(SERVER_RAFT_SSL_TMF_ALGORITHM));
        System.setProperty("server.raftPort", "9091");
        System.setProperty(
                ConfigurationKeys.SERVER_RAFT_SERVER_ADDR,
                XID.getIpAddress() + ":9091" + "," + XID.getIpAddress() + ":9092" + "," + XID.getIpAddress() + ":9093");
        StoreConfig.setStartupParameter("raft", "raft", "raft");
        Assertions.assertDoesNotThrow(RaftServerManager::init);
        Assertions.assertNotNull(RaftServerManager.getRaftServer("default"));
        Assertions.assertNotNull(RaftServerManager.groups());
        Assertions.assertNotNull(RaftServerManager.getCliServiceInstance());
        Assertions.assertNotNull(RaftServerManager.getCliClientServiceInstance());
        Assertions.assertFalse(RaftServerManager.isLeader("default"));
        RaftServerManager.start();
    }

    @Test
    public void initRaftServerFail() {
        StoreConfig.setStartupParameter("raft", "raft", "raft");
        Assertions.assertThrows(IllegalArgumentException.class, RaftServerManager::init);
    }

    @Test
    public void initRaftServerFailByRaftPortNull() {
        System.setProperty(
                ConfigurationKeys.SERVER_RAFT_SERVER_ADDR,
                XID.getIpAddress() + ":9091" + "," + XID.getIpAddress() + ":9092" + "," + XID.getIpAddress() + ":9093");
        StoreConfig.setStartupParameter("raft", "raft", "raft");
        Assertions.assertThrows(IllegalArgumentException.class, RaftServerManager::init);
    }

    @Test
    public void testIsRaftModeWhenNotInitialized() {
        Assertions.assertFalse(RaftServerManager.isRaftMode());
    }

    @Test
    public void testGetRaftServerWhenNotInitialized() {
        Assertions.assertNull(RaftServerManager.getRaftServer("default"));
    }

    @Test
    public void testGetRaftServersWhenNotInitialized() {
        Assertions.assertNotNull(RaftServerManager.getRaftServers());
        Assertions.assertTrue(RaftServerManager.getRaftServers().isEmpty());
    }

    @Test
    public void testGroupsWhenNotInitialized() {
        Assertions.assertNotNull(RaftServerManager.groups());
        Assertions.assertTrue(RaftServerManager.groups().isEmpty());
    }

    @Test
    public void testIsLeaderWhenNotInRaftMode() {
        StoreConfig.setStartupParameter("file", "file", "file");
        Assertions.assertTrue(RaftServerManager.isLeader("default"));
    }

    @Test
    public void testCliServiceInstance() {
        Assertions.assertNotNull(RaftServerManager.getCliServiceInstance());
    }

    @Test
    public void testCliClientServiceInstance() {
        Assertions.assertNotNull(RaftServerManager.getCliClientServiceInstance());
    }
}
