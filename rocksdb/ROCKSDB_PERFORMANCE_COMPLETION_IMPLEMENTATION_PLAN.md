# RocksDB Remaining Performance Work Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development task-by-task. Do not run remote or large-scale benchmarks while the benchmark server is unavailable.

**Goal:** Complete the locally verifiable R4/R6 performance work, strengthen R3 concurrency semantics, and publish an evidence-based progress snapshot without claiming unavailable benchmark results.

**Architecture:** Preserve current defaults. Add an opt-in DB-wide memtable budget and per-column-family write-buffer overrides, then make verification streaming and cursor-bounded through point reads instead of full in-memory cross-reference sets. Keep the existing full verification entry point as a compatibility wrapper.

**Tech Stack:** Java 8-compatible source, RocksDB JNI 10.10.1.1, JUnit 5, Maven.

## Global Constraints

- Existing RocksDB configuration remains behaviorally unchanged when new options are unset.
- Configuration values use the existing Seata size parser and reject negative values.
- `verifyCurrentState()` remains available and performs an explicit full verification.
- Sample/page verification must have bounded retained memory and cap diagnostic samples.
- No benchmark conclusion is recorded without an actual benchmark run.

---

### Task 1: Global Memtable Budget And CF Profiles

**Files:**
- Modify: `common/src/main/java/org/apache/seata/common/ConfigurationKeys.java`
- Modify: `server/src/main/java/org/apache/seata/server/storage/rocksdb/RocksDBStoreConfig.java`
- Create: `server/src/main/java/org/apache/seata/server/storage/rocksdb/RocksDBColumnFamilyProfile.java`
- Modify: `server/src/main/java/org/apache/seata/server/storage/rocksdb/RocksDBStoreEngine.java`
- Modify: `server/src/main/resources/application.example.yml`
- Test: `server/src/test/java/org/apache/seata/server/storage/rocksdb/RocksDBStoreConfigTest.java`
- Test: `server/src/test/java/org/apache/seata/server/storage/rocksdb/RocksDBStoreEngineTest.java`

**Interfaces:**
- Consumes: existing `RocksDBStoreConfig` defaults and `RocksDBColumnFamily` values.
- Produces: `dbWriteBufferSize`, profile-specific write-buffer sizes, `writeBufferSizeFor(RocksDBColumnFamily)`, and one independently owned `ColumnFamilyOptions` per descriptor.

- [x] Add failing configuration tests for the DB-wide budget, profile overrides, fallback behavior, and negative-value rejection.
- [x] Run the focused config tests and confirm failure because the new keys/accessors do not exist.
- [x] Add failing engine tests that open a DB with profile overrides and assert each CF option has the expected write-buffer size.
- [x] Run the focused engine tests and confirm failure because per-CF options are not exposed/applied.
- [x] Add configuration keys, profile mapping, parsing, accessors, tuning summary output, and equality/hash-code coverage.
- [x] Build descriptors with separate `ColumnFamilyOptions`, apply the DB-wide budget through `DBOptions.setDbWriteBufferSize`, and close all options on every success/failure path.
- [x] Run the focused config and engine tests and confirm they pass.

### Task 2: Bounded Streaming Verify

**Files:**
- Create: `server/src/main/java/org/apache/seata/server/storage/rocksdb/maintenance/RocksDBVerifyMode.java`
- Create: `server/src/main/java/org/apache/seata/server/storage/rocksdb/maintenance/RocksDBVerifyOptions.java`
- Create: `server/src/main/java/org/apache/seata/server/storage/rocksdb/maintenance/RocksDBVerifyCursor.java`
- Modify: `server/src/main/java/org/apache/seata/server/storage/rocksdb/maintenance/RocksDBVerifyReport.java`
- Modify: `server/src/main/java/org/apache/seata/server/storage/rocksdb/maintenance/RocksDBMaintenanceService.java`
- Test: `server/src/test/java/org/apache/seata/server/storage/rocksdb/maintenance/RocksDBMaintenanceServiceTest.java`

**Interfaces:**
- Consumes: `RocksDBStoreEngine.scanByPrefix`, primary record codecs, and exact secondary-index key codecs.
- Produces: `verifyCurrentState(RocksDBVerifyOptions)`, resumable page cursors, completion state, bounded error samples, and missing/stale index counts.

- [x] Add failing tests for page limit/cursor continuation, sample per-family bounds, capped error samples, and missing status/transaction/timeout/lock indexes.
- [x] Run the focused maintenance tests and confirm failure because the bounded verify API does not exist.
- [x] Add immutable mode/options/cursor/report types with defensive byte-array copies and input validation.
- [x] Replace full maps/sets and materialized prefix lists with streaming scans plus exact point reads; preserve `verifyCurrentState()` as `FULL` mode.
- [x] Run the focused maintenance tests and confirm they pass.

### Task 3: R3 Concurrency Semantics Regression

**Files:**
- Test: `server/src/test/java/org/apache/seata/server/storage/rocksdb/lock/RocksDBLockManagerTest.java`

**Interfaces:**
- Consumes: existing batched global release and lock acquisition APIs.
- Produces: deterministic regression coverage proving a competing xid cannot acquire a partially released multi-key transaction as an all-or-nothing lock set.

- [x] Add a concurrency test coordinated with a barrier around batched release/acquire.
- [x] Run the focused lock test and confirm current semantics or expose a real race.
- [x] The test confirmed current all-or-nothing semantics, so no production lock change was required.
- [x] Run the full RocksDB lock-manager test class as part of the 103-test focused suite.

### Task 4: Documentation, Review, Verification, And Commit

**Files:**
- Modify: `rocksdb/ROCKSDB_REVIEW_RISKS_AND_TUNING_PLAN.md`
- Create: `rocksdb/ROCKSDB_LATEST_PROGRESS.md`

**Interfaces:**
- Consumes: verified code/test results from Tasks 1-3.
- Produces: a current completion matrix separating implemented work, local evidence, server-blocked benchmarks, and remaining reliability/observability work.

- [x] Update the risk/tuning plan with exact R4/R6/R3 implementation status and no unrun benchmark claims.
- [x] Write `ROCKSDB_LATEST_PROGRESS.md` with completed, locally verified, server-blocked, and remaining work sections.
- [x] Run focused RocksDB tests and server test compilation.
- [x] Review the final diff for behavior regressions, resource ownership, cursor correctness, and documentation consistency.
- [x] Run final `git diff --check`, stage the scoped files, and commit with `feat(rocksdb): complete local performance tuning`.
