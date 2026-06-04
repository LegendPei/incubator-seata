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

import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.XID;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.server.storage.file.FlushDiskMode;
import org.apache.seata.server.store.StoreConfig;

import static java.io.File.separator;
import static org.apache.seata.common.DefaultValues.DEFAULT_SESSION_STORE_FILE_DIR;

/**
 * RocksDB store configuration.
 */
public class RocksDBStoreConfig {

    private final String dbPath;
    private final boolean syncWrite;

    public RocksDBStoreConfig(String dbPath, boolean syncWrite) {
        this.dbPath = dbPath;
        this.syncWrite = syncWrite;
    }

    public static RocksDBStoreConfig fromConfiguration() {
        org.apache.seata.config.Configuration config = ConfigurationFactory.getInstance();
        String configuredDir = config.getConfig(ConfigurationKeys.STORE_FILE_ROCKSDB_DIR);
        String dbPath = configuredDir;
        if (StringUtils.isBlank(dbPath)) {
            dbPath = config.getConfig(ConfigurationKeys.STORE_FILE_DIR, DEFAULT_SESSION_STORE_FILE_DIR)
                    + separator
                    + XID.getPort()
                    + separator
                    + "rocksdb";
        }
        return new RocksDBStoreConfig(dbPath, StoreConfig.getFlushDiskMode() == FlushDiskMode.SYNC_MODEL);
    }

    public String getDbPath() {
        return dbPath;
    }

    public boolean isSyncWrite() {
        return syncWrite;
    }
}
