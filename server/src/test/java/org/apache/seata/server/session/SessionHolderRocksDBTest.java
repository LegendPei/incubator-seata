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
package org.apache.seata.server.session;

import org.apache.seata.common.Constants;
import org.apache.seata.common.holder.ObjectHolder;
import org.apache.seata.common.store.SessionMode;
import org.apache.seata.config.ConfigurationCache;
import org.apache.seata.core.constants.ConfigurationKeys;
import org.apache.seata.server.storage.rocksdb.RocksDBStoreEngineFactory;
import org.apache.seata.server.storage.rocksdb.session.RocksDBSessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;

class SessionHolderRocksDBTest {

    @TempDir
    Path tempDir;

    private Object originalEnvironment;

    @BeforeEach
    void beforeEach() {
        originalEnvironment = ObjectHolder.INSTANCE.getObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT);
        ObjectHolder.INSTANCE.setObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, new MockEnvironment());
        ConfigurationCache.clear();
    }

    @AfterEach
    void afterEach() throws Exception {
        System.clearProperty(ConfigurationKeys.STORE_FILE_DIR);
        System.clearProperty(ConfigurationKeys.STORE_FILE_ENGINE);
        System.clearProperty(ConfigurationKeys.STORE_FILE_ROCKSDB_DIR);
        SessionHolder.destroy();
        RocksDBStoreEngineFactory.destroy();
        ConfigurationCache.clear();
        restoreEnvironment();
    }

    @Test
    void testRocksDBFileEngineInitializesRocksDBSessionManager() {
        System.setProperty(
                ConfigurationKeys.STORE_FILE_DIR, tempDir.resolve("file").toString());
        System.setProperty(ConfigurationKeys.STORE_FILE_ENGINE, "rocksdb");
        System.setProperty(
                ConfigurationKeys.STORE_FILE_ROCKSDB_DIR,
                tempDir.resolve("rocksdb").toString());

        SessionHolder.init(SessionMode.FILE);

        Assertions.assertTrue(SessionHolder.getRootSessionManager() instanceof RocksDBSessionManager);
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
}
