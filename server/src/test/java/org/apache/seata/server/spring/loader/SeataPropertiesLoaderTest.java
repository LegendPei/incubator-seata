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
package org.apache.seata.server.spring.loader;

import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.Constants;
import org.apache.seata.common.holder.ObjectHolder;
import org.apache.seata.config.ConfigurationCache;
import org.apache.seata.server.store.StoreConfig;
import org.apache.seata.spring.boot.autoconfigure.loader.SeataPropertiesLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.lang.reflect.Field;

class SeataPropertiesLoaderTest {

    @BeforeEach
    void beforeEach() throws Exception {
        clearConfig();
    }

    @AfterEach
    void afterEach() throws Exception {
        clearConfig();
    }

    @Test
    void testLoadDefaultSessionAndLockModes() {
        new SeataPropertiesLoader().loadSessionAndLockModes();

        Assertions.assertEquals("file", System.getProperty("sessionMode"));
        Assertions.assertEquals("file", System.getProperty("lockMode"));
    }

    @Test
    void testLoadEffectiveLockModeForRocksDBFileEngine() {
        System.setProperty(ConfigurationKeys.STORE_FILE_ENGINE, "rocksdb");

        new SeataPropertiesLoader().loadSessionAndLockModes();

        Assertions.assertEquals("file", System.getProperty("sessionMode"));
        Assertions.assertEquals("rocksdb", System.getProperty("lockMode"));
    }

    private void clearConfig() throws Exception {
        System.clearProperty("sessionMode");
        System.clearProperty("lockMode");
        System.clearProperty(ConfigurationKeys.STORE_MODE);
        System.clearProperty(ConfigurationKeys.STORE_LOCK_MODE);
        System.clearProperty(ConfigurationKeys.STORE_FILE_ENGINE);
        ObjectHolder.INSTANCE.setObject(Constants.OBJECT_KEY_SPRING_CONFIGURABLE_ENVIRONMENT, new MockEnvironment());
        ConfigurationCache.clear();
        resetStoreConfig("storeMode");
        resetStoreConfig("sessionMode");
        resetStoreConfig("lockMode");
    }

    private void resetStoreConfig(String fieldName) throws Exception {
        Field field = StoreConfig.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, null);
    }
}
