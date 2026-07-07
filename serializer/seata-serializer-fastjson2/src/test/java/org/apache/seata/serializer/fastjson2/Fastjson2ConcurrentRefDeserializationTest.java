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
package org.apache.seata.serializer.fastjson2;

import com.alibaba.fastjson2.JSONFactory;
import com.alibaba.fastjson2.reader.ObjectReaderProvider;
import org.apache.seata.core.protocol.AbstractMessage;
import org.apache.seata.core.protocol.BatchResultMessage;
import org.apache.seata.core.protocol.MergedWarpMessage;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
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

    private final Fastjson2Serializer serializer = new Fastjson2Serializer();

    @Test
    public void deserializeReferenceHeavyProtocolMessageDoesNotDropRefFields() {
        byte[] bytes = serializer.serialize(referenceHeavyMessage());

        assertThat(countNullRefFields(serializer.deserialize(bytes))).isZero();
    }

    @Test
    public void concurrentDeserializeReferenceHeavyProtocolMessageDoesNotDropRefFields() throws Exception {
        Assumptions.assumeTrue(
                Boolean.getBoolean(ENABLE_STRESS_TEST_PROPERTY),
                "set -D" + ENABLE_STRESS_TEST_PROPERTY + "=true to run the concurrent test");

        byte[] bytes = serializer.serialize(referenceHeavyMessage());
        assertThat(countNullRefFields(serializer.deserialize(bytes))).isZero();

        int nullTasks = runConcurrentStress(() -> countNullRefFields(serializer.deserialize(bytes)));

        assertThat(nullTasks).isZero();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static MergedWarpMessage referenceHeavyMessage() {
        MergedWarpMessage message = new MergedWarpMessage();
        List sharedList = new ArrayList();
        for (int i = 0; i < 20; i++) {
            BatchResultMessage resultMessage = new BatchResultMessage();
            resultMessage.setResultMessages(sharedList);
            resultMessage.setMsgIds(sharedList);
            message.msgs.add(resultMessage);
            message.msgIds.add(i);
        }
        return message;
    }

    private static int countNullRefFields(MergedWarpMessage message) {
        if (message == null || message.msgs == null) {
            return 1;
        }
        int nullCount = 0;
        for (AbstractMessage child : message.msgs) {
            if (!(child instanceof BatchResultMessage)) {
                nullCount++;
                continue;
            }
            BatchResultMessage batchResult = (BatchResultMessage) child;
            if (batchResult.getResultMessages() == null) {
                nullCount++;
            }
            if (batchResult.getMsgIds() == null) {
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
                        "fastjson2-rpc-ref-" + i);
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
            Field field = ObjectReaderProvider.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object cache = field.get(provider);
            if (cache instanceof Map) {
                ((Map<?, ?>) cache).clear();
            }
        }

        try {
            Field readerCacheField = ObjectReaderProvider.class.getDeclaredField("readerCache");
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
