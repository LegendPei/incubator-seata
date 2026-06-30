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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    @Test
    void testSummaryHeaderIncludesRepeatAggregationColumns() throws Exception {
        Field field = RocksDBFileModeBenchmark.class.getDeclaredField("SUMMARY_CSV_HEADER");
        field.setAccessible(true);
        String header = (String) field.get(null);

        Assertions.assertTrue(header.contains("runGroup"));
        Assertions.assertTrue(header.contains("runCount"));
        Assertions.assertTrue(header.contains("opsPerSecondMean"));
        Assertions.assertTrue(header.contains("opsPerSecondMedian"));
        Assertions.assertTrue(header.contains("opsPerSecondP95"));
        Assertions.assertTrue(header.contains("opsPerSecondP99"));
        Assertions.assertTrue(header.contains("opsPerSecondStddev"));
    }

    @Test
    void testParseOpsPerSecondUsesHeaderColumnAfterInterpretabilityFields() throws Exception {
        String line = csvLine(Map.of("scenario", "query.status", "repeatRun", "A1", "opsPerSecond", "123.4"));
        Method method = RocksDBFileModeBenchmark.class.getDeclaredMethod("parseOpsPerSecond", List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Double> actual = (Map<String, Double>) method.invoke(null, List.of(line));

        Assertions.assertEquals(123.4D, actual.get("query.status"));
    }

    @Test
    void testParseOpsPerSecondAggregatesAllRepeatRuns() throws Exception {
        List<String> lines = List.of(
                csvLine(Map.of("scenario", "query.status", "repeatRun", "A1", "opsPerSecond", "100.0")),
                csvLine(Map.of("scenario", "query.status", "repeatRun", "A2", "opsPerSecond", "300.0")));
        Method method = RocksDBFileModeBenchmark.class.getDeclaredMethod("parseOpsPerSecond", List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Double> actual = (Map<String, Double>) method.invoke(null, lines);

        Assertions.assertEquals(200.0D, actual.get("query.status"));
    }

    @Test
    void testSummarizeCsvLinesAggregatesRepeatStatistics() throws Exception {
        List<String> lines = List.of(
                csvLine(map(
                        "scenario", "query.status",
                        "repeatRun", "A1",
                        "opsPerSecond", "100.0",
                        "totalMs", "30.0",
                        "p50Ms", "3.0",
                        "p95Ms", "9.0",
                        "p99Ms", "10.0",
                        "rowsScanned", "10",
                        "rowsReturned", "5",
                        "rowsUpdated", "0",
                        "innerOperations", "1")),
                csvLine(map(
                        "scenario", "query.status",
                        "repeatRun", "A2",
                        "opsPerSecond", "300.0",
                        "totalMs", "10.0",
                        "p50Ms", "1.0",
                        "p95Ms", "4.0",
                        "p99Ms", "5.0",
                        "rowsScanned", "30",
                        "rowsReturned", "15",
                        "rowsUpdated", "2",
                        "innerOperations", "3")));
        Method method = RocksDBFileModeBenchmark.class.getDeclaredMethod("summarizeCsvLines", List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> actual = (List<String>) method.invoke(null, lines);

        Assertions.assertEquals(1, actual.size());
        Map<String, String> summary = parseSummaryLine(actual.get(0));
        Assertions.assertEquals("query.status", summary.get("scenario"));
        Assertions.assertEquals("A", summary.get("runGroup"));
        Assertions.assertEquals("2", summary.get("runCount"));
        Assertions.assertEquals("200.000", summary.get("opsPerSecondMean"));
        Assertions.assertEquals("200.000", summary.get("opsPerSecondMedian"));
        Assertions.assertEquals("300.000", summary.get("opsPerSecondP95"));
        Assertions.assertEquals("300.000", summary.get("opsPerSecondP99"));
        Assertions.assertEquals("100.000", summary.get("opsPerSecondStddev"));
        Assertions.assertEquals("20.000", summary.get("rowsScannedMean"));
        Assertions.assertEquals("10.000", summary.get("rowsReturnedMean"));
        Assertions.assertEquals("1.000", summary.get("rowsUpdatedMean"));
        Assertions.assertEquals("2.000", summary.get("innerOperationsMean"));
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

    private String csvLine(Map<String, String> valuesByColumn) throws Exception {
        Field field = RocksDBFileModeBenchmark.class.getDeclaredField("CSV_HEADER");
        field.setAccessible(true);
        String[] columns = ((String) field.get(null)).split(",");
        Map<String, String> values = new LinkedHashMap<>(valuesByColumn);
        String[] parts = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            parts[i] = values.getOrDefault(columns[i], "");
        }
        return String.join(",", parts);
    }

    private Map<String, String> map(String... values) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(values[i], values[i + 1]);
        }
        return result;
    }

    private Map<String, String> parseSummaryLine(String line) throws Exception {
        Field field = RocksDBFileModeBenchmark.class.getDeclaredField("SUMMARY_CSV_HEADER");
        field.setAccessible(true);
        String[] columns = ((String) field.get(null)).split(",");
        String[] values = line.split(",", -1);
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < columns.length; i++) {
            result.put(columns[i], values[i]);
        }
        return result;
    }
}
