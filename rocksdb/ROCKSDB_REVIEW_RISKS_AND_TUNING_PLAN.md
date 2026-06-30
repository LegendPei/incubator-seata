# RocksDB File Mode 架构审查与调优计划

## 文档状态

- 日期：2026-06-20
- 范围：Seata file mode 的 RocksDB 存储引擎设计、实现风险、benchmark 暴露的问题，以及后续调优路线。
- 目标：把当前 review 结论沉淀为可以拆分 issue / PR / benchmark 任务的计划。
- 非目标：不在本文替代 Phase1-4 的设计文档，也不直接给出所有参数的最终推荐值。RocksDB 参数需要结合机器、数据规模、事务模型和压测证据逐步收敛。
- 最近同步：2026-06-30 已同步剩余优化队列。R7/R2/R1 的首批补强和 review 修复已完成；R2/R1/R8/R7/R3/R4/R6/R5/R9 的剩余项已拆成后续工作包，重点从“修明显 O(N) 风险”转为“后台任务产品化、crash-injection/RPO 证据、benchmark 汇总和运维能力收口”。

## 本次实现同步

本轮按“先降低复杂度，再调 RocksDB 参数”的顺序，优先完成了几类高收益、低歧义的改动：

- R2：新增 `RocksDBStoreEngine.scanByPrefix(seekKey, prefix, limit, filter, consumer)` 和 `ScanStats`；`RocksDBIndexManager.scanXidsByStatus(status, minBeginTimeInclusive, maxBeginTimeInclusive, cursor, limit)` 已使用 status | beginTime lower-bound seek 和 cursor 续扫；`RocksDBTransactionStoreManager.readByStatuses` 在 `overTimeAliveMills` 存在时按 cursor 分页读取，在 status-only 且 `SessionCondition.limit > 0` 时也走 paged status index scan，避免先全量扫描再在 JVM 层截断。
- R1：新增 `RocksDBLockManager.cleanOrphanLocks(int limit)` / `cleanOrphanLocks(byte[] seekKey, int limit)` / `cleanOrphanLocksBatches(...)` / `RocksDBLocker.cleanOrphanLocks(...)` 的结构化结果，返回 cleaned/scanned/batches/limitReached/nextSeekKey；启动期 orphan lock cleanup 改为最多扫描 1024 条，并在上次 clean shutdown 时跳过启动清理，在 scan limitReached 时输出 warning，避免启动关键路径无界清理且可靠暴露剩余风险；打开 DB 时会读取上次 clean marker 后立即 durable sync dirty marker，正常 close 时写回 clean marker；维护任务可用 nextSeekKey 做 bounded 续扫。
- R3：`RocksDBLocker.updateLockStatus` 和按 xid/branch release 改为按 `LOCK_BRANCH_INDEX` 分批扫描、分批 `WriteBatch`，并对已扫描到的 index key 做精确删除，避免一次性物化大 fanout 锁索引和 live prefix delete。
- R5：`RocksDBMaintenanceService.createCheckpoint(path, flush=true)` 已在 checkpoint 前显式调用 `storeEngine.flush()`；`flush=false` 表示跳过显式 flush，但不承诺 RocksDB JNI 内部不会做自己的 checkpoint 行为。
- R7：benchmark 增强了 tuning profile、WAL sync 对比、RocksDB 配置摘要和 WAL sync 统计输出；本轮新增 `queryIterationsPerRound`、`queryLimit`、`repeatRuns`、`compareOrder`，CSV 增加 repeat/order/query limit 与 `rowsScanned` / `rowsReturned` / `rowsUpdated` / `innerOperations` 解释性列，query status 场景已接入 `queryLimit`；A/B comparison 的 ops/s 解析已改为按 CSV header 定位列，避免新增解释性列后读错下标。
- R8：新增 periodic WAL sync 配置、控制器、统计快照和 metrics。默认仍为 `none`，仅在 `flush-disk-mode=async` 且 `walSyncMode=periodic` 时启用；后台 periodic sync 仍按 best-effort 语义管理，`walSyncOnShutdown=true` 的 final sync 已改为 strict failure observable，且 `RocksDBStoreEngine.close()` 会在 final sync 失败时仍关闭 CF handle、DB、options、cache、statistics 后再抛出；关闭竞态下的调度拒绝不会改变已成功写入的返回语义；crash-injection 验证仍未完成。

已验证的 focused suite：

```powershell
.\mvnw.cmd -pl server "-DskipITs=true" "-Dcheckstyle.skip=true" "-Dlicense.skip=true" "-Dspotless.check.skip=true" "-Dtest=RocksDBStoreEngineTest,RocksDBIndexManagerTest,RocksDBTransactionStoreManagerTest,RocksDBLockManagerTest,RocksDBFileModeBenchmarkTest" test
```

结果：exit 0；67 tests, 0 failures, 0 errors, 0 skipped。测试日志仍有既有的 SpringBootConfigurationProvider / SLF4J 噪声。

## 2026-06-30 当前状态快照

- 架构主线：RocksDB file mode 的 CF 切分、主记录 + 二级索引模型、lock branch index、migration/index rebuild、maintenance/checkpoint/diagnostics 的方向仍然成立。
- 恢复复杂度：R1/R2/R3 已把最危险的启动全量 orphan cleanup、status 全量扫描和大 fanout lock release/update 做了首轮 bounded/streaming 化；当前风险从“单次无界阻塞”下降为“后台任务调度、进度、限速、观测还未产品化”。
- 可靠性：R8 的 periodic WAL sync 仍是默认关闭的 best-effort 能力；shutdown final sync 已 strict failure observable，close 失败后资源释放已补；R1 clean shutdown marker 已能区分 clean/dirty restart，且启动 dirty marker 已 durable sync，避免崩溃窗口误跳过 orphan cleanup。
- benchmark：R7 已能输出 repeat/order/query limit、rows scanned/returned/updated、innerOperations 和 WAL sync 指标；A/B ops/s 解析已随 CSV header 对齐，跨 repeat summary CSV 已输出 mean/median/p95/p99/min/max/stddev。剩余不足是 summary JSON、statusDistribution/lockWorkload 配置和 rowsUpdated 精细口径还没有完成。
- 当前进入阶段：已从“验证可行性和修明显 O(N) 风险”进入“后台任务产品化 + 运维调参矩阵 + crash-injection/RPO 验证”的阶段。

## 剩余优化工作包

这些工作包用于把剩余优化拆成可以独立提交、review 和压测的 issue/PR。排序原则是：先收敛恢复和后台任务复杂度，再补可靠性证据和 benchmark 解释力，最后补运维调参能力和边界文档。

### WP1：R2 后台任务 batch limit 和多状态有序读取

- 目标：让 timeout / retry committing / async committing / end-state 后台任务的扫描成本与“本轮到期数量”绑定，而不是与状态全集绑定。
- 当前进展：retry rollbacking、retry committing、async committing、rollbacking/committing scheduled、end-state scheduled 已显式传入 `SessionCondition.limit`；coordinator 侧多状态任务先拆成单状态 bounded 查询，再按 beginTime 排序并截断到本轮全局 batch；RocksDB store 层 `readByStatuses` 已支持多状态 k-way merge，按 beginTime 全局有序读取并校验 stale status/beginTime index，不再按 status 顺序拼接导致单个状态占满本轮结果。
- 主要任务：
  - 已完成：为 retry/end-state 相关后台路径显式传入 batch limit，新增 `server.session.backgroundTaskQueryLimit`，默认 1024。
  - 复用 `SessionCondition.limit` 和 status beginTime cursor scan，避免后台任务无界读取。
  - 暂不直接 limit `timeoutCheck`：该路径按每个事务自己的 timeout 判断，直接限定最早 beginTime 的前 N 条可能漏掉“后创建但 timeout 更短”的事务，需要单独的 deadline-aware 索引或算法。
  - 记录每轮 rowsScanned、rowsReturned、pointReads、耗时和 limitReached。
  - 已完成：store 层多状态 k-way merge，复用每个 status 的 cursor/page scan，并按 beginTime/status/xid 稳定合并。
  - 待完成：跨轮 cursor merge、后台任务 scan stats/耗时指标和专项 benchmark。
- 验收：
  - 构造 1M+ sessions、每轮只到期 1K/10K 的数据集，后台任务耗时主要随到期数量增长。
  - 单测覆盖 retry/end-state limit、空结果、相同 beginTime、多状态、分页续扫。
  - benchmark 或专项压测输出后台任务 batch limit 前后对比。

### WP2：R1 orphan cleanup 后台任务产品化

- 目标：把 orphan lock 清理从“启动期有限补救 + 手动维护入口”推进为可观测、可中断、可恢复的后台维护任务。
- 主要任务：
  - 定义 orphan cleanup job 状态：cursor、cleaned、scanned、batches、lastError、startedAt、updatedAt。
  - 持久化进度，支持进程重启后从 last cursor 继续。
  - 增加限速参数：batchLimit、maxBatchesPerRun、sleepMillis/token bucket、最大连续运行时长。
  - 增加调度入口：启动后 dirty marker 触发、定时维护触发、显式 admin 触发。
  - 增加告警：limitReached 持续存在、失败次数、清理滞后时间、疑似 orphan 数量持续增长。
- 验收：
  - 非 clean shutdown 后服务能先恢复可用，再后台分批清理 orphan locks。
  - 中断/重启后不会从头无界扫描，能根据持久化 cursor 继续。
  - 压测 1M/5M lock index 时，清理任务不显著拉高 lock acquire/release p95/p99。

### WP3：R8 crash-injection 和 RPO 矩阵

- 目标：用故障注入证据说明 `syncWrite=true`、periodic WAL sync、walSyncMode=none 等策略的实际恢复边界，避免把 best-effort periodic sync 误读成严格 RPO。
- 主要任务：
  - 新增独立 crash-injection harness，不放入普通单测路径。
  - 支持写入 N 条后 kill 进程、按时间窗口 kill、按 WAL sync stats 阈值 kill。
  - 输出 lostWrites、lostDurationMs、lastSyncedSequenceNumber、latestSequenceNumber、recoveredSessions。
  - 形成 RPO/吞吐/p99 延迟矩阵：syncWrite=true、periodic low-latency、periodic throughput、none。
  - 评估强可靠模式：unsyncedWrites/unsyncedMs 超过硬阈值时前台等待 sync 或拒绝继续扩大窗口。
- 验收：
  - 每种 WAL 策略至少有多轮 crash-injection 数据。
  - 文档明确 periodic sync 是 best-effort，并给出推荐告警阈值。
  - 如果引入 backpressure，测试覆盖 sync 失败、超阈值等待和关闭竞态。

### WP4：R7 benchmark repeat 汇总和 workload 参数

- 目标：让多轮 benchmark 结果可以直接用于结论判断，减少人工拼 CSV 和人工解释 workload 偏差。
- 主要任务：
  - 已完成：对 repeatRuns 输出跨 repeat summary CSV：mean、median、p95、p99、min、max、stddev。
  - 已完成：A/B comparison 不只比较第一组 A/B，而是比较所有 repeat 聚合结果。
  - 增加 workload 参数：statusDistribution、expiredRatio、lockWorkload、lockConflictRatio、xidFanoutDistribution。
  - 细化 rowsUpdated、pointReads、iteratorNext、writeBatchBytes 等解释性指标，至少先覆盖 query/status、orphan clean、lock release/update。
  - 输出汇总 CSV/JSON，保留 raw CSV 方便复查。
- 验收：
  - `repeatRuns >= 3` 时自动输出 raw + summary，两者 scenario/runLabel 可互相追踪。
  - AB/BA/ABBA 顺序下 summary 不再受单次运行顺序明显误导。
  - benchmark 文档更新所有新增参数和字段含义。

### WP5：R3 fanout 指标、并发语义和大 fanout benchmark

- 目标：证明 streaming/分批 lock release/update 在超大 fanout 和并发场景下不仅降低内存风险，也保持语义正确。
- 主要任务：
  - 增加 release/update 指标：rowsScanned、rowsUpdated、batchCount、maxBatchSize、elapsedMs。
  - 增加并发语义测试：release xid 时并发 acquire 相同 xid/不同 branch/相同 lockKey 的边界。
  - 增加 10K/100K locks per xid benchmark，记录 p95/p99 和堆内存。
  - 评估是否需要 xid lock summary，用于大 fanout 告警和维护任务拆批。
- 验收：
  - 大 fanout release/update 不一次性物化全部 index entries。
  - 并发测试覆盖不会误删扫描后新写入的 index key。
  - benchmark 给出 batch size 对 tail latency 的影响。

### WP6：R4/R6 内存预算、per-CF profile 和 verify 模式

- 目标：把 RocksDB 调参从单参数试错推进为“全局预算 + per-CF profile + 可控 verify”的运维模型。
- 主要任务：
  - 引入全局 memtable/write buffer 预算，明确与 block cache 预算的关系。
  - 设计 per-CF profile：global、branch、lock、index、metadata 分别给出默认和调优方向。
  - 暴露 memtable、block cache、table reader、flush/compaction pending、stall 等指标。
  - verify 增加 sample/page/full 三档模式，page 模式支持 cursor/limit，full 模式只允许显式 admin 触发。
  - verify 输出 checked count、inconsistent count、orphan index/record count 和 first N samples。
- 验收：
  - 小内存配置下不会因多 CF memtable 无感放大导致持续 write stall。
  - sample/page verify 可在大库运行，不一次性构建全量内存集合。
  - full verify 的成本、风险和触发方式在运维文档中明确。

### WP7：R5/R9 运维文档和 range delete 边界收口

- 目标：补齐已经实现或已有 benchmark 结论的文档边界，避免用户误用 checkpoint flush 或 range delete。
- 主要任务：
  - 明确 checkpoint `flush=false` 仅表示跳过显式 `storeEngine.flush()`，不承诺禁止 RocksDB JNI 内部行为。
  - 明确 `flush=true` 的一致性和 IO 成本预期。
  - range delete 默认保持关闭，不进入 lock hot path。
  - 增加 range tombstone 后效应验证建议：range delete 后继续跑 lock acquire/release 和 prefix scan，观察 compact 前后恢复曲线。
- 验收：
  - 运维文档中 checkpoint 和 range delete 语义与实现一致。
  - 默认配置不启用 range delete。
  - 若未来启用 range delete，必须有维护场景、compact 策略和回归 benchmark 证据。

## 总体判断

当前 RocksDB file mode 的主架构方向是成立的：

- 通过 column family 拆分 `GLOBAL_SESSION`、`BRANCH_SESSION`、`LOCK`、`LOCK_BRANCH_INDEX`、`GLOBAL_STATUS_INDEX`、`TRANSACTION_ID_INDEX`，能把会话主记录、锁记录和二级索引的生命周期隔离开。
- `GLOBAL_SESSION` 主记录和 `GLOBAL_STATUS_INDEX` / `TRANSACTION_ID_INDEX` 索引更新放在同一个 `WriteBatch` 中，方向正确，有利于保证索引一致性。
- benchmark 已经覆盖导入、查询、锁路径、维护、WAL sync、range delete 等关键点，足以支撑下一轮优化判断。

但现在最值得优先处理的问题不是单纯调 RocksDB 参数，而是访问路径、后台任务边界和运维动作的复杂度控制。尤其是全量扫描、全量物化、无分页的状态查询和启动期清理，会在百万级以上数据规模下直接成为延迟和可用性风险。参数调优应放在这些路径优化之后，否则容易用缓存、memtable 或压缩配置掩盖模型问题。

## 风险总览

| ID | 优先级 | 问题 | 主要影响 | 建议产出 |
| --- | --- | --- | --- | --- |
| R1 | P1 | 启动期 orphan lock 清理是全量扫描和全量物化 | 大库启动慢，可能阻塞恢复路径 | 分页/限速/异步清理方案，异常关闭标记，启动耗时指标 |
| R2 | P1 | 状态与 beginTime 查询没有利用有序索引边界 | timeout/retry 清理任务随全量数据线性变慢 | bounded iterator、分页 cursor、按 deadline 扫描的 API |
| R3 | P2 | xid 维度锁释放/状态更新会扫描并物化全部锁索引 | 大事务锁数量高时释放成本不可控 | streaming update/delete、fanout 指标、并发语义测试 |
| R4 | P2 | RocksDB options 还是单一 CF 模板，缺少全局内存预算 | 多 CF memtable 放大，调参不可控 | per-CF profile、WriteBufferManager/全局预算、监控项 |
| R5 | P2 | checkpoint API 的 `flush` 语义和实际行为不一致 | 备份/恢复预期不清晰 | 明确 flush 语义、补测试、文档修正 |
| R6 | P3 | 维护校验会全量构建内存集合 | 大库 verify 内存和耗时不可控 | sample/page/full 三档校验模式，进度和限速 |
| R7 | P3 | benchmark 部分指标口径还不足以指导参数调优 | 优化结论可能被工作负载偏差影响 | repeat/order/rows scanned/percentile 等指标补齐 |
| R8 | P3 | WAL periodic sync 是 best-effort，不是严格丢失窗口 | 容灾语义容易被误读 | crash-injection 验证，配置说明，max unsynced 指标告警 |
| R9 | P3 | range delete 对锁热路径伤害明显 | prefix scan 受 range tombstone 影响 | 默认关闭，限制使用范围，改为维护路径专项评估 |

## R1 启动期 orphan lock 全量清理

### 问题

当前启动流程会在 RocksDB 初始化后执行 orphan lock 清理。清理实现会扫描 `LOCK_BRANCH_INDEX`，把符合条件的索引值收集到内存列表，再批量处理锁记录。

benchmark 中 `lock.clean_orphan` 在 1M/4M 锁规模下耗时约 54s，说明该路径已经是 O(N) 级恢复成本。

### 当前落地状态

- 已完成：`cleanOrphanLocks(int limit)` 支持单批限量清理，并返回 cleaned/scanned/limitReached/nextSeekKey；测试覆盖 `limit=1` 时只清理一条并保留剩余条目给下一批。
- 已完成：补充“前缀为有效锁、后续仍存在 orphan lock”的场景，确保扫描达到 limit 但 cleaned=0 时也能通过 limitReached 暴露剩余风险。
- 已完成：新增 `cleanOrphanLocks(byte[] seekKey, int limit)` 续扫入口，maintenance task 可消费上次返回的 nextSeekKey 继续 bounded 清理。
- 已完成：新增 `cleanOrphanLocksBatches(...)` 维护入口，可按 batchLimit / maxBatches 聚合 cleaned/scanned/batches，并返回 nextSeekKey 供后续任务继续。
- 已完成：新增 clean shutdown marker；RocksDB 打开时读取上次 marker 后 durable sync dirty marker，正常 close 时写回 clean；启动期 orphan cleanup 在上次 clean shutdown 时跳过。
- 已完成：`SessionHolder` 启动期 orphan lock cleanup 改为 `ROCKSDB_STARTUP_ORPHAN_LOCK_CLEAN_LIMIT=1024`，并按 scan limitReached 输出 warning，不再依赖 cleaned 数量推断风险。
- 已完成：原 `cleanOrphanLocks()` 入口仍保留，用于显式维护或 benchmark 全量清理。
- 待完成：后台异步清理仍未完成；底层 nextSeekKey 续扫和 bounded maintenance loop 已具备，但后台任务的进度持久化、中断恢复、限速、调度入口和告警口径还没有产品化。

### 为什么

orphan lock 清理本质上是维护动作，不应该无条件成为启动关键路径。对服务端来说，大库场景下启动时间会直接影响故障恢复时间；如果清理过程中还持有锁管理器内部互斥，可能进一步影响并发 lock/unlock 路径。

另外，全量扫描并把结果一次性放进 `List` 会把 RocksDB 的迭代成本转化为 JVM 堆内存压力。数据越大，风险越明显。

### 怎么做

1. 把 orphan lock 清理改成分页迭代：
   - 每批扫描固定数量的 `LOCK_BRANCH_INDEX`。
   - 每批生成一个小 `WriteBatch`。
   - 批次之间释放锁、记录 cursor、允许限速。

2. 将启动期清理改为条件触发：
   - 正常关闭时写入 clean shutdown marker。
   - 非正常关闭、迁移后、显式管理员命令时才触发清理。
   - 默认启动只做轻量一致性检查和指标上报。

3. 提供异步后台清理模式：
   - 服务先恢复可用。
   - 后台以低优先级扫描 orphan locks。
   - 暴露进度、剩余估计、删除数量、耗时、失败次数。

4. 保留显式维护入口：
   - 管理员可以主动执行 full clean。
   - full clean 必须明确提示这是重维护操作。

### 预期产出

- 大库启动耗时不再和锁总数强绑定。
- orphan lock 清理能观察、能暂停、能重试。
- 清理任务不会一次性把全部索引值加载到 JVM 堆。

### 验证方式

- 构造 1M / 5M / 10M lock index 数据集，分别验证冷启动耗时。
- 对比同步清理、异步清理、跳过清理三种模式。
- 增加异常关闭恢复测试，验证 orphan lock 最终会被清理，且不影响正常 lock/unlock。

## R2 状态与 beginTime 查询没有利用有序索引边界

### 问题

`GLOBAL_STATUS_INDEX` 的 key 结构是 `status | beginTime | xid`，已经具备按状态和 beginTime 有序扫描的条件。但当前查询只按 status prefix 扫出 xid，再回表读取 `GLOBAL_SESSION`，最后在 Java 层过滤 beginTime 或排序。

这会导致：

- timeout check 每轮可能读取所有 `Begin` 状态事务。
- committing / async committing / end states 的后台任务可能读取全部相关状态事务，再排序或过滤。
- benchmark 中 `query.status`、`query.begin_sorted`、`query.full_scan_filter` 在百万级数据下都落到秒级。

### 当前落地状态

- 已完成：`RocksDBKeyCodec.encodeGlobalStatusSeekKey(status, beginTime)` 已提供底层 seek key 编码能力，当前 paged status scan 路径已用它承载 lower-bound/cursor。
- 已完成：`RocksDBStoreEngine.scanByPrefix` 支持 `seekKey`、`prefix`、`limit`、提前停止 filter 和 `ScanStats`。
- 已完成：`RocksDBIndexManager.scanXidsByStatus(status, minBeginTimeInclusive, maxBeginTimeInclusive, cursor, limit)` 使用 lower-bound seek、upper-bound stop 和 cursor 续扫。
- 已完成：`RocksDBTransactionStoreManager.readByStatuses` 在 `SessionCondition.overTimeAliveMills > 0` 时按 cursor 分页读取，并支持 `SessionCondition.limit` 限制本轮返回数量。
- 已完成：status-only 且 `SessionCondition.limit > 0` 时也走 paged status index scan，不再先扫描状态全集再在 JVM 层截断；query benchmark 的 `queryLimit` 因此可以触发真实限量 scan 路径。
- 已完成：retry/end-state 相关后台任务已显式传入 `SessionCondition.limit`，并通过 coordinator 侧单状态 bounded fan-in 避免多状态任务被当前按 status 顺序拼接的实现占满本轮 batch。
- 已完成：RocksDB store 层多状态 `readByStatuses` 已使用 k-way merge，按 beginTime 全局有序返回，并跳过 stale status/beginTime index 对排序和去重的影响。
- 待完成：后台任务每轮 scan stats/耗时指标、专项 benchmark 对比、timeoutCheck 的 deadline-aware bounded scan，以及跨轮 cursor merge 还未全面接入。

### 为什么

后台恢复任务需要的是“到期的一小批事务”，不是“某状态下的全部事务”。如果每次任务都从状态全集开始扫描，复杂度会跟历史数据总量绑定，而不是跟本轮需要处理的过期事务数量绑定。

RocksDB 的优势在于有序 key 和顺序迭代。当前 key 设计已经给了 `status + beginTime` 的排序能力，但 API 层没有把 upper bound、limit、cursor 暴露出来。

### 怎么做

1. 增加 bounded scan API：
   - 输入：`status`、`beginTime <= deadline`、`limit`、`cursor`。
   - 输出：`xid` 列表或轻量 session projection。
   - 内部使用 iterator seek 到 `status | minBeginTime`，到 `status | deadline` 停止。

2. 改造后台任务：
   - timeout check 只扫描 `Begin` 且 `beginTime <= now - timeout` 的前 N 条。
   - retry committing / async committing 只扫描到期状态。
   - end states 清理任务按 beginTime 分页处理。

3. 多状态查询使用 k-way merge：
   - 对每个 status 维护一个有序 iterator。
   - 按 beginTime 合并前 N 条。
   - 避免先取全量再排序。

4. 增加扫描过程指标：
   - `rowsScanned`
   - `rowsReturned`
   - `pointReads`
   - `iteratorSeekCount`
   - `iteratorNextCount`

### 预期产出

- timeout/retry/end-state 后台任务的成本从“状态全集大小”下降到“本轮到期数量 + 少量边界扫描”。
- beginTime 查询和状态查询可以支持分页，避免单次任务不可控。
- benchmark 能清楚地区分 RocksDB 读取成本、回表成本和 Java 过滤成本。

### 验证方式

- 在 1M / 5M / 10M global sessions 下压测 timeout check。
- 固定每轮只过期 1K / 10K 条，验证耗时是否主要随过期数量增长。
- 增加 beginTime 边界正确性测试，覆盖相同 beginTime、多状态、空结果、分页续扫。

## R3 xid 维度锁释放和状态更新的 fanout 风险

### 问题

锁释放和锁状态更新依赖 `LOCK_BRANCH_INDEX` 按 xid 前缀扫描，再逐个更新或删除锁记录。当前实现会把该 xid 下的索引项扫描出来并物化。

普通事务 fanout 小时问题不明显，但大事务、批量分支、热点 xid 或异常恢复场景下，一个 xid 可能关联大量 lock key，释放成本会突然变高。

### 当前落地状态

- 已完成：`updateLockStatus(xid, lockStatus)` 改为按 `LOCK_BRANCH_INDEX` 分批扫描、分批更新。
- 已完成：`releaseLock(xid)` / `releaseLock(xid, branchId)` 的内部 release-by-index 路径改为分批扫描并精确删除已扫描 index key。
- 已完成：测试通过 package-private 小 batch size 构造 `batchSize=1`，覆盖多批次 release 和多批次 status update。
- 待完成：生产级 fanout 指标、并发 release + acquire 语义测试和超大 fanout benchmark 仍需补齐。

### 为什么

锁路径是 file mode 的核心热路径之一。即便单个 lock key 的读写很快，只要释放流程需要扫描和处理大量索引项，就会把尾延迟拉高。更重要的是，`deleteByPrefix` 如果在 live prefix 上执行，需要明确和并发加锁路径之间的上层互斥关系，否则容易出现“扫描开始后新写入的索引是否会被误删或漏删”的语义疑问。

### 怎么做

1. 明确并发语义：
   - 记录 xid 级锁释放时是否有全局会话锁保护。
   - 增加并发测试：释放 xid 锁时并发写入相同 xid、不同 branchId、相同 lockKey。

2. 改为 streaming update/delete：
   - iterator 逐批读取索引项。
   - 每批生成小 `WriteBatch`。
   - 不再一次性把所有 index entries 放入内存。

3. 尽量使用精确删除：
   - 对已经扫描到的 index key 精确删除。
   - 谨慎使用 live prefix delete，避免删除扫描后新写入的 key。

4. 增加 fanout 指标：
   - 每个 xid 的 lock count。
   - 单次 release/update 的 rows scanned、rows updated、batch size、耗时。
   - 大 fanout 事务日志采样。

5. 评估 xid lock summary：
   - 对超大 fanout 事务，可以维护轻量计数或状态摘要。
   - summary 不替代真实锁记录，但可用于限流、告警和任务拆批。

### 预期产出

- 大事务锁释放不再一次性消耗大量堆内存。
- release/update 的尾延迟可观测。
- 并发语义有测试保护，不依赖隐含假设。

### 验证方式

- 构造单 xid 下 1K / 10K / 100K locks 的释放压测。
- 增加并发 release + acquire 测试。
- 对比一次性 batch、分页 batch、精确删除三种实现。

## R4 RocksDB options 缺少全局内存预算和 per-CF profile

### 问题

当前 RocksDB 初始化使用同一套 `ColumnFamilyOptions` 创建所有 CF。虽然 block cache 可以是共享对象，但 memtable、level、压缩、bloom/filter 等配置缺少按 CF 区分的 profile，也缺少全局 write buffer 预算。

多 CF 下，如果每个 CF 都按相同 `writeBufferSize * maxWriteBufferNumber` 增长，实际内存上限会被 CF 数量放大。

### 为什么

Pika/PikiwiDB 这类基于 RocksDB 的中间件通常会把内存拆成几个关键部分观察和控制：

- memtable 使用量。
- table reader / index/filter 使用量。
- block cache 使用量。
- write buffer 和 flush/compaction 压力。

如果没有全局预算，参数调优会变成“某一项指标变好，但进程 RSS 或写放大变差”。Seata server 的默认配置还必须保守，因为部署环境和事务规模差异很大。

### 怎么做

1. 引入 per-CF profile：
   - `global`：主记录，读多写中等，关注点查和状态回表。
   - `branch`：按 xid/branch 查，关注事务生命周期读写。
   - `lock`：热路径，关注点查、prefix 查和 release。
   - `index`：有序扫描，关注 bloom/filter、prefix extractor、iterator 成本。

2. 增加全局内存预算：
   - 使用 RocksDB `WriteBufferManager` 或等价机制限制所有 CF 的 memtable 总量。
   - 明确 block cache 是否共享，默认共享。
   - 暴露 `dbWriteBufferSize` / `maxWriteBufferSize` 类配置时，要说明是全局预算还是单 CF 预算。

3. 优先补 metrics 再调参：
   - `rocksdb.cur-size-all-mem-tables`
   - `rocksdb.block-cache-usage`
   - `rocksdb.estimate-table-readers-mem`
   - flush pending / compaction pending
   - stall count / stall micros
   - bytes written/read、write amplification 相关指标

4. 参数实验按 profile 分组：
   - baseline：当前默认。
   - memory-budget：共享 cache + 全局 memtable 预算。
   - lock-hot：锁 CF 更小 memtable、更强 bloom/prefix 配置。
   - index-scan：索引 CF 优化 block size、cache index/filter、prefix extractor。

### 预期产出

- 调参从“单参数试错”变为“预算 + profile + 指标”的可解释过程。
- 多 CF 内存不会随 CF 数量无感放大。
- 可以借鉴 Pika 的内存拆解方式，但保留 Seata 自己对事务一致性和恢复语义的约束。

### 验证方式

- 在相同 workload 下记录 RSS、memtable、block cache、table reader、flush/compaction 指标。
- 对比不同 CF profile 的 p50/p95/p99、吞吐和写放大。
- 验证默认配置在小内存机器上不会产生持续 write stall。

## R5 checkpoint `flush` 参数语义不清晰

### 问题

`RocksDBMaintenanceService.createCheckpoint(Path, boolean flush)` 的 API 和注释暗示可以控制是否 flush memtable，但当前实现使用 RocksDB JNI checkpoint API 时没有真正使用该参数。

### 当前落地状态

- 已完成：`flush=true` 时先显式调用 `storeEngine.flush()`，再创建 checkpoint。
- 已完成：新增测试验证 `flush=false` 不触发显式 flush、`flush=true` 会触发显式 flush。
- 待完成：运维文档仍需明确 `flush=false` 只是不做显式 flush，不代表阻止 RocksDB JNI 内部 checkpoint 行为。

### 为什么

checkpoint 属于备份和恢复边界。调用方会根据 `flush=true/false` 推断一致性、耗时和 IO 行为。如果参数只是日志字段，而不是实际行为，会让运维预期和实现不一致。

### 怎么做

1. 明确 RocksDB JNI 当前能力：
   - 如果只能创建默认 checkpoint，则文档中说明 checkpoint 会使用 RocksDB JNI 默认行为。
   - 不要暴露无法兑现的 `flush=false` 语义。

2. 二选一修正 API：
   - 方案 A：移除或废弃 `flush` 参数，改为 `createCheckpoint(Path)`。
   - 方案 B：当 `flush=true` 时先显式调用 `storeEngine.flush()`，再 checkpoint；当 `flush=false` 时清楚说明只是跳过显式 flush，不能阻止 JNI 内部行为。

3. 补测试：
   - 验证调用路径是否执行显式 flush。
   - 验证 checkpoint 后能正常打开并读取核心 CF。

### 预期产出

- 备份 API 语义和实际行为一致。
- 用户不会基于错误的 flush 预期做容量或耗时规划。

### 验证方式

- 单测覆盖 `flush=true` 和 `flush=false`。
- 集成测试在有未 flush 写入时创建 checkpoint，并验证恢复后的数据完整性。

## R6 维护校验全量构建内存集合

### 问题

当前 verify 会扫描 global、status index、transaction id index、branch、lock index、lock records，并构建多个内存集合来检查一致性。

这适合显式 admin full verify，但不适合启动期、常规巡检或大库高峰期运行。

### 为什么

维护校验的价值是发现索引漂移和孤儿记录，但如果校验动作自身不可控，就可能影响正常服务。全量构建 HashMap/HashSet 会把数据规模压力转移到 JVM 堆，而且难以提供进度、暂停和恢复。

### 怎么做

1. 增加三档模式：
   - `sample`：抽样校验，适合周期巡检。
   - `page`：分页校验，带 cursor 和 limit。
   - `full`：完整校验，仅显式 admin 触发。

2. 输出结构化结果：
   - checked count。
   - inconsistent count。
   - orphan index count。
   - orphan record count。
   - first N samples。

3. 加入限速和进度：
   - 每批 scan 数量。
   - 每批 sleep 或 token bucket。
   - 记录 last cursor，支持继续执行。

### 预期产出

- 校验能力可用于生产巡检，而不仅是开发期工具。
- full verify 的风险和成本明确可控。

### 验证方式

- 注入缺失索引、孤儿索引、孤儿锁记录，验证三种模式都能发现或按采样概率发现。
- 在大数据集下观察 verify 对正常写入延迟的影响。

## R7 benchmark 口径需要继续增强

### 问题

Phase4 benchmark 已经有价值，但部分参数口径仍会影响优化结论：

- `batchSize` 对查询类 workload 的含义不够直观。
- A/B 对比缺少 repeat runs 和执行顺序控制。
- status 分布、lock 冲突模型、超大 fanout 事务覆盖不足。
- ops/s 对长耗时维护任务不如 latency、rows scanned、rows updated 更有解释力。

### 当前落地状态

- 已完成：benchmark CSV 增加 WAL sync 统计列，包括 sync count/failure/cost、unsynced writes/ms、sequence number 等。
- 已完成：benchmark options 增加 `tuningProfile`、`walSyncMode`、WAL sync interval/write threshold/on-shutdown/warn threshold 等参数。
- 已完成：`RocksDBStoreEngine.scanByPrefix` 可返回 `rowsScanned` / `rowsReturned` / `limitReached`，核心 scan API 已具备指标口径。
- 已完成：benchmark 主流程 CSV 已输出 `queryIterationsPerRound`、`queryLimit`、`repeatRun`、`compareOrder`、`rowsScanned`、`rowsReturned`、`rowsUpdated`、`innerOperations`；compare 支持 `repeatRuns` 和 `compareOrder=AB|BA|ABBA` 等顺序控制。
- 已完成：A/B comparison 的 `opsPerSecond` 解析改为 header-driven column lookup，新增解释性列后不会再误读 `repeatRun` 等旧下标位置。
- 已完成：repeatRuns 会输出 summary CSV，按 scenario + runGroup 聚合 `opsPerSecond` 的 mean/median/p95/p99/min/max/stddev，并汇总 totalMs、latency 和 rows 指标；A/B comparison 使用所有 repeat 的聚合 ops/s。
- 待完成：statusDistribution、lockWorkload、rowsUpdated 更细粒度口径和 summary JSON 仍需继续增强。

### 为什么

RocksDB 调优很容易出现局部胜利：某个 workload 变快，但内存、写放大、compaction、尾延迟变差。benchmark 必须把“处理了多少数据”和“内部扫描了多少数据”说清楚，才能判断优化是否真的降低复杂度。

### 怎么做

1. 参数命名拆分：
   - `batchSize`：写入或批处理大小。
   - `queryIterationsPerRound`：查询迭代次数。
   - `queryLimit` / `queryPageSize`：分页返回数量。

2. 增加 repeat 和顺序控制：
   - `repeatRuns`
   - `warmupRuns`
   - `compareOrder`
   - 输出均值、p50、p95、p99、min、max。

3. 增加 workload 维度：
   - status distribution。
   - lock conflict ratio。
   - xid fanout distribution。
   - expired ratio。
   - background task batch limit。

4. 增加解释性指标：
   - `rowsScanned`
   - `rowsReturned`
   - `rowsUpdated`
   - `pointReads`
   - `iteratorNext`
   - `writeBatchBytes`
   - `rocksdbStatsSnapshot`

### 预期产出

- benchmark 能直接说明优化来自访问路径改变、缓存命中提升、还是写入批量变化。
- 参数调优结论可以复现和比较。

### 验证方式

- 每个关键优化至少跑 3 次 repeat，并交换 A/B 顺序。
- 对同一份数据输出原始 CSV/JSON 结果和汇总报告。

## R8 WAL periodic sync 的可靠性边界

### 问题

WAL periodic sync 已经能降低频繁 sync 的成本，但它是 best-effort 机制。benchmark 中 500ms/5000 writes 场景出现过较大的 `maxUnsyncedWrites` 和 `maxUnsyncedMs`，说明实际未同步窗口可能超过配置直觉。

### 当前落地状态

- 已完成：新增 `RocksDBWalSyncMode`、`RocksDBWalSyncController`、`RocksDBWalSyncStats`。
- 已完成：配置项已接入 `ConfigurationKeys`、`RocksDBStoreConfig`、Spring Boot `StoreFileProperties` 和 `application.example.yml`。
- 已完成：`RocksDBStoreEngine` 在 write/delete/batch write/range delete/metadata init 后调用 WAL sync controller；关闭时在 DB/handle close 前执行 shutdown sync。
- 已完成：metrics 暴露 WAL sync count/failure/cost、unsynced writes/ms、latest/last synced sequence number。
- 已完成：`walSyncOnShutdown=true` 的 final sync 使用 strict 路径，失败会抛出 `StoreException` 并保留 failure stats，不再只在日志中吞掉。
- 已完成：`RocksDBStoreEngine.close()` 在 final sync 失败时仍通过 finally 释放 CF handle、DB、options、cache、statistics，再重新抛出 sync failure；测试覆盖失败后可重新打开同一路径。
- 已完成：关闭竞态下 executor 拒绝 WAL sync 调度时，若 executor 已关闭则降级为 debug，不改变 RocksDB write 已成功后的返回语义。
- 待完成：crash-injection、严格 backpressure、RPO 矩阵和生产告警阈值仍需专项验证。

### 为什么

Seata 的事务日志属于恢复关键数据。任何“异步 sync”都必须明确说明可靠性边界。配置项如果被理解成严格上限，但实现只是后台调度，就会在故障恢复时产生争议。

### 怎么做

1. 文档明确语义：
   - `sync=true`：强一致持久化优先。
   - periodic sync：吞吐优先，允许扩大故障窗口。
   - `disableWAL`：不建议用于生产事务日志。

2. 增加 backpressure 选项：
   - 当 unsynced writes 或 unsynced ms 超过硬阈值时，前台写入短暂等待 sync。
   - 默认可不开启，但需要给强可靠场景使用。

3. 增加 crash-injection 测试：
   - 写入后立即 kill 进程。
   - 不同 sync 策略下验证恢复数据丢失范围。
   - 输出实际 lost writes 和 lost duration。

4. 暴露指标和告警建议：
   - `currentUnsyncedWrites`
   - `maxUnsyncedWrites`
   - `currentUnsyncedMs`
   - `maxUnsyncedMs`
   - `lastSyncDurationMs`
   - `syncFailureCount`

### 预期产出

- 用户能基于可靠性需求选择 WAL 策略。
- periodic sync 不会被误解为严格 RPO 保证。
- 后续调优可以同时比较吞吐和故障恢复代价。

### 验证方式

- 在不同 sync 策略下进行 crash-injection。
- 对比吞吐、p99 写入延迟、恢复完整性。

## R9 range delete 不适合作为锁热路径默认优化

### 问题

benchmark 显示启用 range delete 后，锁状态更新和全局锁释放明显变慢。这通常和 range tombstone 影响 prefix scan / iterator 行为有关。

### 为什么

range delete 适合删除大段冷数据或维护型清理，但锁路径需要频繁 prefix scan、point lookup 和 delete。如果在热 CF 或热 key range 上制造大量 range tombstone，后续读取和扫描可能持续付出额外成本。

### 怎么做

1. 默认保持关闭。
2. 不在 lock hot path 使用 range delete。
3. 仅在明确的维护场景评估：
   - 冷数据批量删除。
   - 删除后立即 compact range。
   - 独立 CF 或低频路径。
4. benchmark 增加 tombstone 后效应：
   - range delete 后继续跑 lock acquire/release。
   - 观察 compaction 前后性能恢复情况。

### 预期产出

- 避免把维护优化引入锁热路径。
- range delete 的使用边界清晰。

### 验证方式

- range delete 前后分别跑锁读写和 prefix scan。
- 记录 range tombstone 数量、compaction 耗时和恢复曲线。

## 调优路线

### Stage 0：先修正 benchmark 口径

先补齐 repeat、percentile、rows scanned、rows returned、rocksdb stats 等指标。没有这些指标，后面的 RocksDB 参数实验很难解释。

产出：

- 新 benchmark 参数说明。
- 原始结果 JSON/CSV。
- 每组实验的机器、JVM、RocksDB 配置快照。

### Stage 1：优先优化访问路径

优先处理 R1、R2、R3：

- orphan lock 清理分页化。
- 状态查询 bounded scan。
- 锁 release/update streaming 化。

当前状态：

- 已完成首批实现：R1/R2/R3 的核心无界扫描和全量物化风险已经收敛到 bounded/分批路径。
- 待继续：后台任务 cursor、异步 orphan clean、fanout 指标、并发语义测试和大规模 benchmark 对比仍需补齐。

这些优化会降低算法复杂度，收益比盲目调 block cache 或 write buffer 更稳定。

产出：

- 新的 scan API。
- 后台任务分页实现。
- 大数据集下的前后对比报告。

### Stage 2：建立 RocksDB 内存预算

在访问路径稳定后，再调 RocksDB options：

- 共享 block cache。
- 全局 memtable 预算。
- per-CF profile。
- index/filter cache 策略。
- prefix extractor 和 bloom/filter 针对 lock/index CF 调整。

产出：

- 默认保守 profile。
- 高吞吐 profile。
- 低内存 profile。
- 参数与指标的对照表。

### Stage 3：验证 WAL 与恢复语义

针对 `sync`、periodic sync、disable WAL 建立可靠性矩阵。

当前状态：

- 已完成首批实现：periodic WAL sync 的配置、控制器、统计、metrics 和 benchmark 输出已经落地。
- 待继续：crash-injection、RPO 矩阵、backpressure/强可靠模式和生产告警阈值仍需验证。

产出：

- crash-injection 测试。
- RPO/吞吐/延迟对照。
- 配置文档中的明确风险说明。

### Stage 4：运维能力产品化

将 checkpoint、verify、orphan clean、stats dump 做成可观测、可限速、可分页的维护能力。

产出：

- admin API 或命令入口。
- 维护任务进度模型。
- 失败重试和中断恢复说明。

## 借鉴 Pika/PikiwiDB 与 RocksDB 官方调优思路

Pika/PikiwiDB 的经验对 Seata 有参考价值，但不能照搬。Pika 的核心关注点是 Redis-like KV 服务的吞吐和内存成本；Seata 的核心关注点还包括事务恢复、一致性边界和锁路径尾延迟。

可以学习的部分：

- 把内存拆成 memtable、table reader/index/filter、block cache 几部分观察。
- 使用共享 block cache，而不是每个 CF 自己膨胀。
- 控制全局 write buffer/memtable 预算，避免多 CF 放大。
- 通过 metrics 先定位瓶颈，再改变参数。
- 将高吞吐配置和保守可靠配置分开。

需要谨慎的部分：

- 不应为了吞吐默认关闭 WAL。
- 不应只看 QPS，不看恢复完整性和尾延迟。
- 不应把 range delete 放到锁热路径默认启用。
- 不应把大内存机器上的最佳参数作为默认参数。

## 推荐实施顺序

1. R2：status + beginTime bounded scan 已完成 lower-bound/cursor API，`overTimeAliveMills` 读取路径已按 cursor 分页并支持 limit，status-only + limit 也已走 paged status index scan；retry/end-state 后台任务已显式传入 batch limit，coordinator 侧保留单状态 bounded fan-in，RocksDB store 层已支持多状态 k-way merge 并跳过 stale status/beginTime index；下一步补后台任务 scan stats/benchmark、timeoutCheck deadline-aware bounded scan 和跨轮 cursor merge。
2. R1：orphan lock 清理已支持限量扫描、cleaned/scanned/batches/limitReached 结果、nextSeekKey 续扫入口和 bounded maintenance loop；启动路径已从全量清理改为 clean shutdown 跳过、非 clean shutdown 最多扫描 1024 条，启动 dirty marker 已 durable sync；下一步做异步任务调度、进度持久化、中断恢复、限速和告警。
3. R8：periodic WAL sync 已落地为默认关闭的 best-effort 能力，shutdown final sync 已 strict failure observable，engine close 已补资源释放异常安全；下一步补 crash-injection、RPO 矩阵、backpressure/强可靠模式和生产告警策略。
4. R7：benchmark 已增强 tuning/WAL sync 指标、repeat/order/query limit、rows 解释性列、header-driven A/B 解析和跨 repeat summary CSV；下一步补 statusDistribution、lockWorkload、rowsUpdated 精细统计和 summary JSON。
5. R3：锁 release/update 已 streaming/分批化；下一步补 fanout 指标、并发语义测试和 10K/100K fanout benchmark。
6. R4/R6：加入全局内存预算、per-CF profile、RocksDB 内存/flush/compaction/stall 指标，并把 verify 拆成 sample/page/full 三档。
7. R5/R9：checkpoint `flush` 语义和 range delete 使用边界进入运维文档收口；range delete 默认继续关闭，不进入锁热路径。

这个顺序的原则是：先让后台任务成本可控，再让异常恢复和 WAL 可靠性有证据；先让 benchmark 能自动给出结论，再推进 fanout、内存预算和 verify；最后收口 checkpoint/range delete 这类运维边界说明。

## 验收标准

### 架构验收

- 已部分满足：`overTimeAliveMills` 场景不再无界扫描状态全集；retry/end-state 后台任务必须继续接入 batch limit。
- 已满足首批目标：启动路径不再默认执行不可控的全量 orphan lock 清理，而是限量 1024 条并告警。
- 已满足首批目标：大 fanout lock release/update 不再一次性物化全部索引项。
- 待完成：orphan cleanup、verify、end-state 清理等维护任务都有分页、进度、限速、中断恢复和失败语义。
- 已完成：多状态 `readByStatuses` 已提供 store 层 k-way merge；待补跨轮 cursor merge 和后台任务 scan stats/耗时指标。

### 性能验收

- 待验证：1M 级数据下 timeout check 和 end-state scan 的耗时随过期数据量增长，而不是随总数据量增长。
- 已具备基础能力：orphan clean 支持每批处理上限，仍需补大规模 benchmark。
- 待验证：lock release/update 在 10K+ fanout 下仍能稳定输出 p95/p99。
- 已完成：benchmark repeat summary 自动输出 mean/median/p95/p99/min/max/stddev，不再依赖人工拼多轮 CSV；summary JSON 仍待补。
- 待完成：statusDistribution、lockWorkload、expiredRatio、xidFanoutDistribution 等 workload 参数可配置。
- 已部分满足：RocksDB stats/WAL sync stats 已增强，仍需继续补 rows scanned/updated、pointReads、iteratorNext、writeBatchBytes 等 workload 级指标。
- 待完成：R4 的全局内存预算和 per-CF profile 有 RSS、memtable、block cache、table reader、flush/compaction/stall 指标支撑。

### 可靠性验收

- 待验证：`sync=true`、periodic sync、disable WAL 的恢复差异有 crash-injection 证据。
- 待完成：RPO 矩阵同时给出 lostWrites、lostDurationMs、吞吐和 p99 写入延迟。
- 待完成：periodic WAL sync 的生产告警阈值和强可靠/backpressure 策略有测试和文档说明。
- 已满足：checkpoint 的 flush 语义和实现一致，`flush=true` 会显式调用 `storeEngine.flush()`。
- 待文档收口：`flush=false` 只表示不做显式 flush，不承诺阻止 RocksDB JNI checkpoint 内部行为。
- 待完成：索引一致性校验能在 sample/page/full 模式下发现主记录缺失、索引缺失、孤儿索引、孤儿锁记录。
- 待文档收口：range delete 默认关闭，不进入 lock hot path；任何启用建议都必须绑定维护场景、compact 策略和回归 benchmark。

## 参考资料

- RocksDB Tuning Guide: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
- RocksDB Setup Options and Basic Tuning: https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
- RocksDB Block Cache: https://github.com/facebook/rocksdb/wiki/Block-Cache
- Pika/PikiwiDB Memory Usage: https://github.com/OpenAtomFoundation/pikiwidb/wiki/Pika-Memory-Usage
- Pika/PikiwiDB configuration discussion: https://github.com/OpenAtomFoundation/pikiwidb/issues/1047
- Tencent Cloud Pika RocksDB tuning case: https://developer.cloud.tencent.com/article/2008629?policyId=1004
