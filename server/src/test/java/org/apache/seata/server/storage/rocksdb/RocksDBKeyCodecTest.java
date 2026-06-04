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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RocksDBKeyCodecTest {

    @Test
    void testBranchKeyStartsWithXidPrefix() {
        byte[] branchKey = RocksDBKeyCodec.encodeBranch("127.0.0.1:8091:12345", 10L);
        byte[] xidPrefix = RocksDBKeyCodec.encodeXidPrefix("127.0.0.1:8091:12345");

        Assertions.assertTrue(RocksDBKeyCodec.startsWith(branchKey, xidPrefix));
    }

    @Test
    void testLockBranchIndexKeyStartsWithGlobalAndBranchPrefix() {
        byte[] lockKey = RocksDBKeyCodec.encodeRowLock("jdbc:mysql://127.0.0.1/db", "t_order", "id:1|name:a");
        byte[] indexKey = RocksDBKeyCodec.encodeLockBranchIndex("xid:with#separator", 11L, lockKey);

        Assertions.assertTrue(RocksDBKeyCodec.startsWith(
                indexKey, RocksDBKeyCodec.encodeLockBranchIndexGlobalPrefix("xid:with#separator")));
        Assertions.assertTrue(RocksDBKeyCodec.startsWith(
                indexKey, RocksDBKeyCodec.encodeLockBranchIndexBranchPrefix("xid:with#separator", 11L)));
        Assertions.assertFalse(RocksDBKeyCodec.startsWith(
                indexKey, RocksDBKeyCodec.encodeLockBranchIndexBranchPrefix("xid:with#separator", 12L)));
    }

    @Test
    void testStartsWithRejectsShorterKey() {
        Assertions.assertFalse(RocksDBKeyCodec.startsWith(new byte[] {1}, new byte[] {1, 2}));
    }
}
