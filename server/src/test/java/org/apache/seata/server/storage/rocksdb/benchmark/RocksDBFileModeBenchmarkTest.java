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
package org.apache.seata.server.storage.rocksdb.benchmark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

class RocksDBFileModeBenchmarkTest {

    @Test
    void testOptionsParseQueryAndCompareControls() throws Exception {
        Object options = parseOptions(
                "--batchSize=5",
                "--queryIterationsPerRound=7",
                "--queryLimit=3",
                "--repeatRuns=2",
                "--compare=syncWrite",
                "--compareOrder=BA");

        Assertions.assertEquals(5, intField(options, "batchSize"));
        Assertions.assertEquals(7, intField(options, "queryIterationsPerRound"));
        Assertions.assertEquals(3, intField(options, "queryLimit"));
        Assertions.assertEquals(2, intField(options, "repeatRuns"));
        Assertions.assertEquals("BA", stringField(options, "compareOrder"));
    }

    @Test
    void testComparePlanHonorsOrderAndRepeatRuns() throws Exception {
        Object options = parseOptions("--compare=syncWrite", "--compareOrder=BA", "--repeatRuns=2");
        Method method = options.getClass().getDeclaredMethod("comparisonRunLabels");
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> labels = (List<String>) method.invoke(options);

        Assertions.assertEquals(Arrays.asList("B1", "A1", "B2", "A2"), labels);
    }

    @Test
    void testCsvHeaderIncludesInterpretabilityColumns() throws Exception {
        Field field = RocksDBFileModeBenchmark.class.getDeclaredField("CSV_HEADER");
        field.setAccessible(true);
        String header = (String) field.get(null);

        Assertions.assertTrue(header.contains("queryIterationsPerRound"));
        Assertions.assertTrue(header.contains("queryLimit"));
        Assertions.assertTrue(header.contains("repeatRun"));
        Assertions.assertTrue(header.contains("compareOrder"));
        Assertions.assertTrue(header.contains("rowsScanned"));
        Assertions.assertTrue(header.contains("rowsReturned"));
        Assertions.assertTrue(header.contains("rowsUpdated"));
        Assertions.assertTrue(header.contains("innerOperations"));
    }

    private Object parseOptions(String... args) throws Exception {
        Class<?> optionsClass = Class.forName(RocksDBFileModeBenchmark.class.getName() + "$BenchmarkOptions");
        Method method = optionsClass.getDeclaredMethod("parse", String[].class);
        method.setAccessible(true);
        return method.invoke(null, (Object) args);
    }

    private int intField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private String stringField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (String) field.get(target);
    }
}
