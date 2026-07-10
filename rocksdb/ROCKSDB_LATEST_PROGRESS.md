# RocksDB File Mode 最新进度

## 快照

- 更新时间：2026-07-11
- 当前分支：`feat/rocksdb-phase4-benchmark-and-ops`
- 本轮原则：性能优先；服务器不可连接期间只提交可由本地单测和编译验证的优化，不生成或推测大规模 benchmark 结论。
- 当前阶段：核心 O(N) 风险已完成 bounded/streaming 化；本轮进一步完成 R4 内存预算/per-CF profile、R6 bounded verify，并补 R3 并发语义回归。

## 本轮完成

### R4 全局内存预算与 per-CF profile

- 新增 `store.file.rocksdb.dbWriteBufferSize`。值大于 0 时使用 RocksDB `DBOptions.setDbWriteBufferSize` 控制同一 DB 所有 CF 的 memtable 总预算；默认 0，不改变现有行为。
- 新增 `globalWriteBufferSize`、`branchWriteBufferSize`、`lockWriteBufferSize`、`indexWriteBufferSize`、`metadataWriteBufferSize`。
- profile 未设置时回退到共享 `writeBufferSize`；共享值也为 0 时保留 RocksDB 默认。
- 每个 CF 使用独立 `ColumnFamilyOptions`，避免 per-CF 参数互相覆盖；block cache 仍为共享对象。
- tuning summary 与 `application.example.yml` 已同步新参数。

### R6 sample/page/full streaming verify

- 保留 `verifyCurrentState()`，其语义为显式 full verify。
- 新增 sample/page/full 三种模式；page 支持跨 CF cursor/limit，sample 对每个业务 CF 限量扫描。
- 移除 verify 中数据库规模的 global `HashMap`、valid-lock `HashSet` 和 `prefixScan` 全量 `List`。
- global、branch、lock、status index、timeout index、transaction-id index、lock-branch index 统一使用 streaming iterator 和 exact point read 交叉校验。
- report 新增 checked record/index、inconsistent、missing/stale/orphan、complete/nextCursor 和 first N error samples；错误总数不会被样本上限截断。

### R3 fanout 并发语义

- 已有 release/update streaming 分批实现保持不变。
- 新增 `batchSize=1` 下 batched global release 与整组 acquire 的 20 轮并发回归。
- 回归覆盖 acquire 在竞态中失败后可重试成功，以及 acquire 成功后不会被旧 xid 的后续 release 批次误删。

## 本地验证证据

- 最终 focused suite 覆盖配置、引擎、索引、事务查询、锁和 maintenance：103 tests，0 failures，0 errors，0 skipped。
- `server test-compile`：BUILD SUCCESS。
- 其中 `RocksDBMaintenanceServiceTest` 为 20 tests，`RocksDBLockManagerTest` 为 18 tests，均包含本轮新增回归。
- 测试日志仍包含仓库既有的 SpringBootConfigurationProvider 与 SLF4J binding 噪声，不影响上述命令退出码和 surefire 结果。

## 已实现但等待服务器 benchmark

- R2：1M+ session 下 timeout/end-state/retry bounded scan 的耗时是否主要随到期数量增长。
- R3：10K/100K locks per xid 的 release/update p95/p99、堆内存、batch size 对比。
- R4：baseline、memory-budget、lock-hot、index-scan 的 RSS、memtable、table reader、flush/compaction stall、吞吐和 p99。
- R6：sample/page/full verify 对前台写入 p95/p99、iterator/point-read 和 IO 的影响。
- R7：跨 repeat summary 已具备；仍需把 RocksDB 内部 perf counter/stats snapshot 与 scenario 关联后重新采数。

## 尚未完成

- R1：orphan cleanup 后台任务的进度持久化、中断恢复、统一限速、定时/admin 调度和告警。
- R8：crash-injection harness、sync/periodic/none RPO 矩阵、严格 backpressure 和生产告警阈值。
- R3/R7：生产级 fanout/stall/perf counter 指标。本阶段按“观测性后置”暂不施工。
- R6：自动巡检调度、token-bucket/sleep 限速；当前 page/sample API 已提供上层调度所需的 cursor/limit。
- R5/R9：checkpoint `flush=false` 与 range delete/compact 恢复曲线的运维文档最终收口。

## 环境恢复后的执行顺序

1. 先跑 R2 1M bounded scan 与 R3 10K/100K fanout，确认本轮代码路径的实际 tail latency。
2. 再跑 R4 memory-budget/profile 矩阵，根据 RSS、stall、p99 决定是否给出非零推荐值；在此之前保持全部新参数默认 0。
3. 跑 R6 sample/page/full 前台干扰测试，确定巡检 page size 和限速策略。
4. 最后补 R8 crash-injection/RPO 证据，再决定 periodic WAL sync 的生产建议。
