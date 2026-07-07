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
package org.apache.seata.rm.datasource.undo.parser;

import com.alibaba.fastjson2.JSONFactory;
import com.alibaba.fastjson2.reader.ObjectReaderProvider;
import org.apache.seata.common.loader.EnhancedServiceLoader;
import org.apache.seata.rm.datasource.sql.struct.Field;
import org.apache.seata.rm.datasource.sql.struct.Row;
import org.apache.seata.rm.datasource.sql.struct.TableRecords;
import org.apache.seata.rm.datasource.undo.BranchUndoLog;
import org.apache.seata.rm.datasource.undo.SQLUndoLog;
import org.apache.seata.rm.datasource.undo.UndoLogParser;
import org.apache.seata.sqlparser.SQLType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class Fastjson2ConcurrentRefDeserializationTest {

    private static final String ENABLE_STRESS_TEST_PROPERTY = "seata.fastjson2.concurrentRef";

    private final Fastjson2UndoLogParser parser =
            (Fastjson2UndoLogParser) EnhancedServiceLoader.load(UndoLogParser.class, Fastjson2UndoLogParser.NAME);

    @Test
    public void deserializeReferenceHeavyUndoLogDoesNotDropRefFields() {
        byte[] bytes = parser.encode(referenceHeavyUndoLog());

        assertThat(countNullRefFields(parser.decode(bytes))).isZero();
    }

    @Test
    public void concurrentDeserializeReferenceHeavyUndoLogDoesNotDropRefFields() throws Exception {
        Assumptions.assumeTrue(
                Boolean.getBoolean(ENABLE_STRESS_TEST_PROPERTY),
                "set -D" + ENABLE_STRESS_TEST_PROPERTY + "=true to run the concurrent test");

        byte[] bytes = parser.encode(referenceHeavyUndoLog());
        assertThat(countNullRefFields(parser.decode(bytes))).isZero();

        int nullTasks = runConcurrentStress(() -> countNullRefFields(parser.decode(bytes)));

        assertThat(nullTasks).isZero();
    }

    private static BranchUndoLog referenceHeavyUndoLog() {
        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid("127.0.0.1:8091:123456");
        branchUndoLog.setBranchId(123456L);

        TableRecords sharedImage = tableRecords();
        List<SQLUndoLog> sqlUndoLogs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            SQLUndoLog sqlUndoLog = new SQLUndoLog();
            sqlUndoLog.setSqlType(SQLType.UPDATE);
            sqlUndoLog.setTableName("ref_test");
            sqlUndoLog.setBeforeImage(sharedImage);
            sqlUndoLog.setAfterImage(sharedImage);
            sqlUndoLogs.add(sqlUndoLog);
        }
        branchUndoLog.setSqlUndoLogs(sqlUndoLogs);
        return branchUndoLog;
    }

    private static TableRecords tableRecords() {
        TableRecords tableRecords = new TableRecords();
        tableRecords.setTableName("ref_test");
        List<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.add(new Field("id", Types.INTEGER, 1));
        row.add(new Field("name", Types.VARCHAR, "seata"));
        rows.add(row);
        tableRecords.setRows(rows);
        return tableRecords;
    }

    private static int countNullRefFields(BranchUndoLog branchUndoLog) {
        if (branchUndoLog == null || branchUndoLog.getSqlUndoLogs() == null) {
            return 1;
        }
        int nullCount = 0;
        for (SQLUndoLog sqlUndoLog : branchUndoLog.getSqlUndoLogs()) {
            if (sqlUndoLog == null) {
                nullCount++;
                continue;
            }
            if (sqlUndoLog.getBeforeImage() == null
                    || sqlUndoLog.getBeforeImage().getRows() == null) {
                nullCount++;
            }
            if (sqlUndoLog.getAfterImage() == null || sqlUndoLog.getAfterImage().getRows() == null) {
                nullCount++;
            }
        }
        return nullCount;
    }

    private static int runConcurrentStress(NullCounter nullCounter) throws Exception {
        int rounds = Integer.getInteger("seata.fastjson2.concurrentRef.rounds", 10);
        int threadCount = Integer.getInteger("seata.fastjson2.concurrentRef.threads", 200);
        AtomicInteger totalNullTasks = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int round = 0; round < rounds; round++) {
            clearObjectReaderCache();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            CountDownLatch endLatch = new CountDownLatch(threadCount);
            AtomicInteger roundNullTasks = new AtomicInteger();
            for (int i = 0; i < threadCount; i++) {
                Thread thread = new Thread(
                        () -> {
                            try {
                                barrier.await();
                                if (nullCounter.countNulls() > 0) {
                                    roundNullTasks.incrementAndGet();
                                }
                            } catch (Throwable throwable) {
                                failure.compareAndSet(null, throwable);
                            } finally {
                                endLatch.countDown();
                            }
                        },
                        "fastjson2-undolog-ref-" + i);
                thread.start();
            }
            endLatch.await();
            if (failure.get() != null) {
                throw new AssertionError("Concurrent deserialization failed", failure.get());
            }
            totalNullTasks.addAndGet(roundNullTasks.get());
        }

        return totalNullTasks.get();
    }

    @SuppressWarnings("unchecked")
    private static void clearObjectReaderCache() throws Exception {
        ObjectReaderProvider provider = JSONFactory.getDefaultObjectReaderProvider();
        for (String fieldName : new String[] {"cache", "cacheFieldBased"}) {
            java.lang.reflect.Field field = ObjectReaderProvider.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object cache = field.get(provider);
            if (cache instanceof Map) {
                ((Map<?, ?>) cache).clear();
            }
        }

        try {
            java.lang.reflect.Field readerCacheField = ObjectReaderProvider.class.getDeclaredField("readerCache");
            readerCacheField.setAccessible(true);
            readerCacheField.set(null, null);
        } catch (NoSuchFieldException ignored) {
            // fastjson2 versions differ in internal cache fields.
        }
    }

    private interface NullCounter {
        int countNulls() throws Exception;
    }
}
