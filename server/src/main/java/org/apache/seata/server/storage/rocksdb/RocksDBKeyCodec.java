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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Stable binary key codec for RocksDB file store engine.
 */
public final class RocksDBKeyCodec {

    private static final int LONG_BYTE_SIZE = Long.BYTES;
    private static final int INT_BYTE_SIZE = Integer.BYTES;

    private RocksDBKeyCodec() {}

    public static byte[] encodeXid(String xid) {
        return encodeComponent(xid);
    }

    public static byte[] encodeXidPrefix(String xid) {
        return encodeComponent(xid);
    }

    public static byte[] encodeBranch(String xid, long branchId) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, encodeComponent(xid));
        writeLong(out, branchId);
        return out.toByteArray();
    }

    public static byte[] encodeBranchPrefix(String xid, long branchId) {
        return encodeBranch(xid, branchId);
    }

    public static byte[] encodeRowLock(String resourceId, String tableName, String pk) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, encodeComponent(resourceId));
        write(out, encodeComponent(tableName));
        write(out, encodeComponent(pk));
        return out.toByteArray();
    }

    public static byte[] encodeLockBranchIndex(String xid, long branchId, byte[] lockKey) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, encodeBranch(xid, branchId));
        write(out, encodeComponent(lockKey));
        return out.toByteArray();
    }

    public static byte[] encodeLockBranchIndexBranchPrefix(String xid, long branchId) {
        return encodeBranch(xid, branchId);
    }

    public static byte[] encodeLockBranchIndexGlobalPrefix(String xid) {
        return encodeXidPrefix(xid);
    }

    public static boolean startsWith(byte[] key, byte[] prefix) {
        if (key == null || prefix == null || key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] encodeComponent(String value) {
        byte[] valueBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        return encodeComponent(valueBytes);
    }

    private static byte[] encodeComponent(byte[] value) {
        byte[] valueBytes = value == null ? new byte[0] : value;
        ByteBuffer buffer = ByteBuffer.allocate(INT_BYTE_SIZE + valueBytes.length);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
        return buffer.array();
    }

    private static void writeLong(ByteArrayOutputStream out, long value) {
        write(out, ByteBuffer.allocate(LONG_BYTE_SIZE).putLong(value).array());
    }

    private static void write(ByteArrayOutputStream out, byte[] bytes) {
        out.write(bytes, 0, bytes.length);
    }
}
