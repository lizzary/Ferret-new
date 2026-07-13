# Index-node M3 开发故障复盘（含直接相关的 M2 问题）

> 记录日期：2026-07-13  
> 范围：本开发线程中实际发现、复现或通过对抗性审计确认的问题。重点是 M3 authoritative reconciliation，同时包含直接影响 M3 的 M2 watcher/debounce 问题，以及测试和 Windows 开发环境干扰。M4 之后的功能不在本文范围内。  
> 状态：本文列出的产品缺陷均已修复；文末另列尚未补齐的一对一测试和已知性能边界。

## 1. 完成基线

M3 最终门禁如下：

- <code>go test -buildvcs=false -count=1 ./...</code> 通过。
- <code>go test -buildvcs=false -race -count=1 ./...</code> 通过。
- <code>go vet ./...</code> 通过。
- <code>go build -buildvcs=false ./cmd/indexnode</code> 通过。
- 覆盖率：<code>store 81.2%</code>、<code>debounce 90.6%</code>、<code>errclass 97.2%</code>、<code>scheduler 91.7%</code>、<code>reconcile 81.7%</code>。
- 真实 filecat E2E 最终为 <code>move_fast_path=2</code>、<code>extract=3</code>；文件移动与目录子文件移动均保持原 <code>file_id</code>，旧路径无 catalog 残留。

这里的“任务”不是 watcher goroutine，而是 SQLite <code>tasks</code> 表中的持久化工作单元：

- <code>upsert</code>：读取当前路径并建立或更新索引。
- <code>remove</code>：删除 catalog 和索引投影。
- <code>relocate</code>：把已有 <code>file_id</code> 从 <code>old_path</code> 移到 <code>path</code>，内容没变时走零抽取快路径。

<code>in_flight</code> 表示任务已被 scheduler 原子 claim，正在 worker 中执行。它既不是“已经完成”，也不是“可以忽略后续变化”；它只能证明某个 generation 的工作当前有执行者。

## 2. 最终并发模型

~~~mermaid
flowchart LR
    W["filecat 事件"] --> D["debounce / 目录分类"]
    D --> E["watcher 增量入队"]
    S["startup / dirty / periodic scanner"] --> C["条件对账入队"]
    E --> Q["SQLite durable tasks"]
    C --> Q
    Q --> SCH["scheduler claim"]
    SCH --> F["in_flight worker"]
    F --> P["Tantivy + catalog 提交"]
    P --> G["generation fence"]
    C -- "同路径已有 in_flight" --> R["标记 deferred rescan"]
    R -- "RetryBase 到 RetryCap" --> S
~~~

最终规则是：

1. watcher 和 scanner 都只在 store 的事务边界分配 generation 和入队。
2. pending、retry_wait、waiting_dep 的等价工作可作为 durable coverage。
3. 同路径直接 <code>in_flight</code> 本轮视为覆盖，不能立刻制造更高 generation 与它竞争。
4. 但 scanner 必须安排延迟权威复扫；旧任务退出后仍有差异，才创建 <code>generation+1</code>。
5. 最后还有提交 generation fence，陈旧结果不能覆盖新状态。

## 3. 全 root 扫描的最终触发条件

| 条件 | 是否扫描 | 原因 |
|---|---:|---|
| 节点启动 | 是 | 启动期间事件可能缺失，必须建立权威基线 |
| 新增 watch root | 是 | 新 root 尚无权威基线 |
| watcher overflow | 是 | 有明确丢事件证据 |
| watcher 到 debounce 的非阻塞投递失败 | 是 | 下游背压导致事件被主动丢弃 |
| watcher 打开、运行失败或重开 | 是 | 失败窗口内事件可能缺失 |
| 默认 24 小时周期巡检 | 是 | 修复无法被即时观测的长期漂移 |
| 普通目录 Created/Modified | 否 | 目录本身不可索引；后代事件提供增量信息 |
| 普通目录 Move/Removed | 否 | 使用 old/new path 和 catalog prefix 原子展开 |
| 普通文件 Created/Modified/Move/Removed | 否 | 直接生成增量任务 |

一次 deliberate full scan 仍是 <code>O(磁盘文件数 + catalog 行数)</code>。当前实现通过最多四个并发 root round、每 root 四个 stat worker、有界 channel 和 metadata-only 遍历控制资源，但无法消除大型冷 root 的 metadata I/O。

## 4. 主事故：目录移动被重复扫描破坏

### 4.1 如何发现

真实 watcher E2E 对一个普通文件和一个包含子文件的目录分别做 rename。预期两个 move 都走快路径，因此应为：

- <code>move_fast_path=2</code>
- <code>extract=3</code>，仅包含最初创建的三个文件

失败现场却是：

- <code>move_fast_path=1</code>
- <code>extract=5</code>
- 目标路径最终可能已 indexed，所以只等待最终状态会误以为系统正确

增强测试诊断后，dump 出了完整 task/catalog 快照，确认不是 filecat 没有报告事件，而是本节点内部 generation 和任务顺序互相覆盖。

### 4.2 最小复现

1. 创建 <code>old-directory/nested.txt</code> 并等待 indexed。
2. 将目录 rename 为 <code>new-directory</code>。
3. 让 scanner 或目标子项 Created 事件在父目录 relocate expansion 前后交错。
4. 同时观察 <code>tasks</code>、<code>files</code>、<code>move_fast_path</code> 和 <code>extract</code>。

失败时的持久化顺序可归纳为：

1. watcher 的旧子文件 upsert，generation 1，priority 5，进入 <code>in_flight</code>。
2. scanner 又为旧子文件创建 generation 2、priority 8 的工作。
3. 父目录 relocate 因 prefix 冲突等待。
4. 旧路径任务先清理或改写 catalog。
5. 父目录任务展开出 anchored child relocate，generation 3。
6. scanner 在 child relocate 执行期间再创建 generation 4。
7. 合法 relocate 被更高 generation 判陈旧，触发额外抽取，严重时出现 path ownership/Fatal。

### 4.3 原因推断

证据表明问题不在 filecat-go：

- 父目录 Move 的 old/new path 已到达。
- 目标后代事件也已到达。
- 系统最终能看到新路径。
- 错误发生在 watcher 任务与 scanner 任务汇合之后。

真正错误是同时存在三个不成立的假设：

- 假设父目录 relocate 一定先于目标子项事件执行。
- 假设 scanner 看见 catalog 暂无目标路径，就一定是新文件。
- 假设 <code>in_flight</code> 可以简单地“永不覆盖”或“永远覆盖”扫描观察。

### 4.4 失败的中间方案

#### 方案 A：所有 direct in-flight 都不算 coverage

目的原本是避免 worker 启动后发生的修改被吞掉。结果 scanner 会立即制造 successor，合法 relocate 在完成前就被更高 generation 淘汰，重复抽取更严重。

#### 方案 B：目录 expansion 完成后再 MarkDirty

这比收到目录事件立刻扫描更晚，但 scanner 仍会与刚展开、尚未完成的 child relocate 重叠；只是改变了竞态窗口，没有消除竞态。

#### 方案 C：每秒固定复扫

正确性可以收敛，但一个长任务会让大型 root 每秒执行一次 metadata walk，形成 I/O 放大。

### 4.5 最终修复

- 普通目录事件不再触发整 root 扫描。
- scanner 对新路径先用唯一 <code>(size, mtime_ns, inode)</code> 反查旧 catalog；唯一命中时生成 anchored relocate。
- pending anchored relocate 与目标 Created/upsert 合并时保留 <code>op、old_path、file_id</code>。
- relocate 的 <code>old_path</code> 只覆盖 remove，不覆盖后来重新创建源路径的 upsert。
- 同路径 direct in-flight 本轮返回 <code>ReconcileCovered</code>，不立即 bump generation。
- scanner 记录 per-root deferred rescan，并按 <code>RetryBase → RetryCap</code> 指数退避。
- worker 退出后再次权威观察；仍不一致才入队更高 generation。
- prepare、relocate、最终 Tantivy/catalog commit 均受 generation 和 path ownership fence 保护。

### 4.6 回归

- <code>internal/lifecycle/lifecycle_test.go::TestRealWatchEventsConvergeAndMovesDoNotExtract</code>
- <code>internal/reconcile/scanner_test.go::TestDirectInFlightCoverageSchedulesDelayedAuthoritativeRescan</code>
- <code>internal/reconcile/scanner_test.go::TestScanRecognizesUniqueFileIdentityAsRelocate</code>
- <code>internal/store/taskqueue_test.go::TestDestinationUpsertPreservesAndAdvancesPendingRelocateAnchor</code>
- <code>internal/store/reconcile_enqueue_test.go::TestRelocateOldPathOnlyCoversRemovalNotRecreatedSource</code>

## 5. 逐项故障记录

### B-01：Move 后复用源路径会被旧 move/remove 吞掉

**发现**

在 M2 debounce 合并表做对抗序列检查时，序列 <code>Move(A→B), Created(A), Removed(B)</code> 暴露出错误：新创建的 A 可能被当成原对象的旧源继续折叠。

**复现**

1. 提交 A→B。
2. settle window 内重新创建 A。
3. 再提交 B removed。
4. 检查 pending 状态，旧实现可能删除新 A 或破坏 move chain。

**原因与根因**

pending map 同时用路径表达 move 的源和目标，却没有区分“旧对象的源”与“后来占用同一路径的新对象”；合并规则对 source reuse 不完整。

**修复**

pending state 分开维护 source/destination 索引，使用显式 move fold table。源路径重新出现时建立独立工作，不再被旧 move 的后续 remove 覆盖。

**回归**

- <code>internal/debounce/debounce_test.go::TestMoveRemovalDoesNotOverwriteReusedSource</code>
- <code>internal/debounce/debounce_test.go::TestMoveSourceFoldTable</code>
- <code>internal/debounce/debounce_test.go::TestMergeRulesTable</code>
- <code>internal/debounce/debounce_test.go::TestPendingStateMoveChainAndDeadlineReset</code>

### B-02：flapping watcher 收到一个事件就重置重开退避

**发现**

fake watcher 连续失败测试中发现，只要某次打开后短暂收到一个事件，失败计数就被清零；随后再次崩溃仍从 5 秒基础延迟开始。

**复现**

让 backend 反复执行“打开成功 → 发一个事件 → 立即失败”，记录每次 reopen delay。

**原因与根因**

一个事件只能证明瞬时可用，不能证明 watcher 已恢复健康。旧实现把“收到事件”等同于“稳定恢复”。

**修复**

只有 watcher 连续存活达到 <code>HealthyReset</code> 才重置 failure count；单个事件不再清空指数退避历史。每个 root 的失败和 backoff 相互隔离。

**回归**

- <code>internal/watch/manager_test.go::TestManagerDoesNotResetBackoffAfterOneEvent</code>
- <code>internal/watch/manager_test.go::TestManagerReopensWithExponentialBackoffAndIsolatesRoots</code>

### B-03：普通目录事件错误触发整 root 扫描

**发现**

最初存在 <code>DirectoryHint/DirectoryExpanded → MarkDirty(root)</code>。用户指出 filecat 已报告后代变化，Move 也提供 old/new path；真实目录移动调试进一步证明额外 scan 会扩大竞态面。

**复现**

在大 root 下创建、修改或移动目录，观察 dirty generation/reconcile round。旧实现会在增量任务之外再启动一次全 root metadata walk。

**原因与根因**

把“目录需要特殊处理”错误等同于“事件流已丢失”。目录 move/remove 需要 catalog prefix expansion，不需要重新遍历整个 root。

**修复**

- 目录 Created/Modified 本身不可索引，跳过目录自身，依赖后代事件。
- 目录 Move/Removed 保留目录级 durable task，按 catalog prefix 在事务中展开。
- 移除生产路径中的 <code>DirectoryHint</code> 和成功 expansion 后的 <code>MarkDirty</code>。
- 只在第 3 节列出的真实 loss/startup/periodic 条件下全扫描。

**回归**

- <code>internal/debounce/debounce_test.go::TestDirectoryClassificationRepresentationAndSkip</code>
- <code>internal/debounce/debounce_test.go::TestVanishedDirectoryUsesCatalogPrefix</code>
- <code>internal/lifecycle/lifecycle_test.go::TestRealWatchEventsConvergeAndMovesDoNotExtract</code>

### B-04：catalog 返回短页时 scanner 提前结束

**发现**

对 keyset pagination 使用一个每次最多只返回一行的 fake store；scanner 请求十行却漏掉第二条及后续 catalog row。

**复现**

1. fake store 准备两个磁盘上不存在的 catalog row。
2. 请求 page size 10，但 fake store 每次只返回 1。
3. 旧 scanner 因 <code>len(page) &lt; pageSize</code> 直接停止，只生成一个 remove。

**原因与根因**

“短页”不等于 EOF。Store 可以内部限流，接口也没有承诺非末页必定填满。

**修复**

持续推进 keyset cursor，只有返回空页才结束；每页都验证 root identity。

**回归**

- <code>internal/reconcile/scanner_test.go::TestCatalogReconciliationContinuesAcrossShortStorePages</code>
- <code>internal/store/catalog_test.go::TestListFilesByPrefixPageNoGapsOrDuplicates</code>
- <code>internal/store/catalog_test.go::TestListFilesByPrefixPageSeparatorsAndLiteralCharacters</code>

### B-05：scanner 与 watcher 的先读后写 TOCTOU

**发现**

scanner 先读取 generation N，watcher 随后 bump 到 N+1；旧 scanner 仍可根据旧快照再次 bump 或插入任务。

**复现**

先保存 scanner 的 <code>file_id/generation</code> 观察值，再执行 watcher enqueue，最后使用旧观察调用 reconcile enqueue。

**原因与根因**

catalog 校验、active-work coverage、generation 分配和 task insert 原先不在同一个 SQLite 写事务中。

**修复**

<code>EnqueueReconcileIfCurrent</code> 在单一事务内完成：

1. 校验 observed file ID 和 generation。
2. 校验 relocate destination ownership。
3. 查询 active coverage。
4. 分配 generation。
5. 插入或合并 task。

竞争失败返回 <code>ReconcileStale</code>，不留下 catalog bump 或半个 task。

**回归**

- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileWatchRaceIsStale</code>
- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileStaleObservations</code>
- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileRollsBackCatalogBump</code>
- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileRelocateIsConditionalAndPreservesIdentity</code>

### B-06：重复扫描重复 bump generation 和 diff

**发现**

startup scan 已创建任务但尚未消费时，紧接着 dirty scan 会再次报告相同 diff，早期实现还会重复 bump。

**复现**

第一轮扫描只入队不执行，立即对同一 root 再扫描，比较 task 数、generation 和 reconcile diff。

**原因与根因**

scanner 没有把 pending、retry_wait、waiting_dep 的等价 durable work 视为“差异已经被系统接纳”。

**修复**

条件入队返回 <code>ReconcileCovered</code>；不 bump catalog、不新增 task，也不重复计入 diff。

**回归**

- <code>internal/reconcile/scanner_test.go::TestStartupAndDirtyRoundsConvergeWithoutDoubleCountingCoveredTasks</code>
- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileRepeatedScanIsCoveredWithoutBump</code>

### B-07：in-flight 不覆盖会制造并发 successor

**发现**

这就是第 4 节主事故中的 generation 竞争。旧任务还在处理，scanner 立即创建更高代，导致 relocate/commit 自己变陈旧。

**复现**

claim generation N 的任务，在它仍 <code>in_flight</code> 时运行 scanner；观察是否立刻出现 N+1。

**原因与根因**

把 <code>in_flight</code> 理解成“没有 durable coverage”，忽略了它已拥有执行 lease 和路径冲突槽。

**修复**

同路径 direct in-flight 当前轮视为 covered，不立即创建 successor；同时交给 deferred authoritative rescan 处理 worker 启动后的真实新变化。

**回归**

- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileInFlightWorkCoversWithoutSuccessor</code>
- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileNewPathInFlightWorkIsCovered</code>
- <code>internal/reconcile/scanner_test.go::TestDirectInFlightCoverageSchedulesDelayedAuthoritativeRescan</code>

### B-08：简单覆盖 in-flight 会永久吞掉后发生的修改

**发现**

修复 B-07 后继续做对抗序列：worker 已读取旧内容，磁盘随后改变，scanner 看见新状态却因 in-flight covered 把 root 确认为 clean。

**复现**

1. claim generation N，让 worker 开始。
2. 修改同一文件。
3. worker 未退出时触发 scan。
4. 等 N 完成；若没有后续复扫，变化永久无任务承接。

**原因与根因**

in-flight 只能证明任务正在执行，不能证明 worker 的输入快照包含 scanner 后看到的变化。

**修复**

covered 结果携带 deferred 含义。scanner 保存 per-root 延迟复扫状态；任务退出后仍不一致才创建 N+1。

**回归**

- <code>internal/reconcile/scanner_test.go::TestDirectInFlightCoverageSchedulesDelayedAuthoritativeRescan</code>

### B-09：固定每秒 deferred rescan 造成 I/O 放大

**发现**

正确性修复最初按固定 RetryBase（1 秒）复扫。性能审计发现，长任务期间大 root 会每秒被完整 walk。

**复现**

让一个任务长时间保持 in-flight，用 fake clock 推进多个 RetryBase，统计 scan 次数。

**原因与根因**

把任务级重试周期直接用于 root 级 O(N) 操作，没有退避状态。

**修复**

每 root 使用指数序列 <code>RetryBase → ... → RetryCap</code>；没有 deferred work、root 被 fence 或删除时清除状态。fake-clock 回归验证触发点为 T+1、T+3、T+7，而不是 busy loop。

**回归**

- <code>internal/reconcile/scanner_test.go::TestDirectInFlightCoverageSchedulesDelayedAuthoritativeRescan</code>

### B-10：relocate 的 old_path 没覆盖 scanner remove

**发现**

relocate 已 in-flight，磁盘旧路径当然不存在；scanner 却只按 task.path 查覆盖，于是为 old_path 创建更高代 remove。

**复现**

catalog 准备旧路径，claim <code>old→new</code> relocate，再对旧路径运行 catalog removal scan。

**原因与根因**

active-work coverage 忽略了 relocate 的 source 语义。对旧源来说，relocate 已经承担 remove。

**修复**

desired op 为 remove 时，同时匹配 active relocate 的 <code>old_path_key</code>，覆盖 pending、in_flight、retry_wait、waiting_dep。

**回归**

- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileActiveStatesAndOldPathCover</code>

### B-11：old_path coverage 过宽会吞掉重新创建的源文件

**发现**

修复 B-10 后做反向审计：如果移动完成前旧源路径被一个新对象重新占用，仅按 old_path 相同判断 coverage，会把新对象的 upsert 也视为已处理。

**复现**

排入 <code>old→new</code> relocate，然后在 old 创建新文件并让 scanner 请求 upsert。

**原因与根因**

“relocate 会删除旧源”只与 remove 等价，与“索引一个后来创建的新对象”不等价。

**修复**

relocate 的 <code>old_path_key</code> 仅覆盖 desired remove；upsert 必须正常入队。

**回归**

- <code>internal/store/reconcile_enqueue_test.go::TestRelocateOldPathOnlyCoversRemovalNotRecreatedSource</code>

### B-12：无 catalog 路径会复用旧 generation

**发现**

路径曾作为 relocate old_path 留下高 generation，后来重新创建时，单看 catalog 会从 generation 1 重新开始。

**复现**

先持久化 generation 5、old_path 指向目标路径的历史任务，再在该路径扫描一个新文件。

**原因与根因**

generation 分配只查询 catalog；pre-catalog path、已完成 task 和 task.old_path 中的历史都被忽略。

**修复**

新路径 generation 取该 <code>path_key</code> 在 task.path 和 task.old_path 中的最大值加一。任务 release/coalesce 也保持单调。

**回归**

- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileNewPathUsesMaximumTaskGeneration</code>
- <code>internal/store/taskqueue_states_test.go::TestPreCatalogEventsStillAdvanceGeneration</code>
- <code>internal/store/taskqueue_states_test.go::TestTaskReleaseCoalescesGenerations</code>

### B-13：目标 upsert 会破坏 pending anchored relocate

**发现**

missed-rename 识别加入后，目标路径已能收敛，但目录子文件仍不走 move fast path。检查 pending task 发现 <code>old_path/file_id</code> 被后续 Created 事件覆盖。

**复现**

1. catalog 中旧路径 generation 2。
2. 先排入携带 file ID 的 <code>old→new</code> relocate。
3. 再模拟目标路径 Created，执行普通 upsert enqueue。
4. 旧实现把同槽任务改成无锚点 upsert。

**原因与根因**

目标 catalog 行尚不存在，普通 <code>BumpGeneration(destination)</code> 得到 NotFound；通用 pending coalesce 只看 destination，覆盖了 relocate 语义。

**修复**

NotFound 分支先检查目标 path_key 上的 pending anchored task；若有 file ID，按 ID 同时 bump catalog 和 task generation。incoming upsert 只代表目标仍需处理，不得覆盖 relocate 的 op、old_path、file ID。

**回归**

- <code>internal/store/taskqueue_test.go::TestDestinationUpsertPreservesAndAdvancesPendingRelocateAnchor</code>

### B-14：relocate 期间内容改变会创建第二个 file_id

**发现**

relocate 因 tuple/hash 不同不能走快路径，进入完整 prepare 后可能在 destination 插入新 catalog row，产生重复身份或 ownership Fatal。

**复现**

旧路径已有 anchored file，排入 relocate；claim 后让目标 size/mtime/content 与原文件不同，再执行 prepare。

**原因与根因**

<code>PrepareFileForTask</code> 对 anchored relocate 仍走普通 path upsert；目标路径暂时没有 row 时就分配了新 ID。

**修复**

anchored task 按 <code>file_id + generation</code> UPDATE 原 catalog row，原子更新 path/path_key 和 metadata。目标若已由另一 file ID 占用，则返回 <code>ErrPathOwnership</code>，绝不静默覆盖。

**回归**

- <code>internal/store/catalog_test.go::TestPrepareAnchoredRelocateWithChangedContentPreservesFileID</code>

### B-15：正常陈旧 worker 结果被升级成 Fatal

**发现**

scanner/watcher 竞争后，旧 worker 在 prepare/relocate 遇到 <code>ErrStaleGeneration</code> 或 <code>ErrPathOwnership</code>。早期 processor 会停止整个节点。

**复现**

claim generation N，随后持久化 N+1 successor，再让 N 提交或占用目标路径。

**原因与根因**

不能简单把 stale 标 done，因为这可能掩盖真正丢失的工作；也不能一律 Fatal，因为正常 generation 竞争必然会产生 loser。缺少“已有 durable successor”的事务化证明。

**修复**

新增 <code>RetireTaskIfSuperseded</code>。仅当 catalog 或同路径任务存在严格更高 generation 时退休旧 in-flight；没有 successor 仍返回 false 并保留 Fatal。退休过程不清除已有 dead-letter 证据。

**回归**

- <code>internal/store/supersede_test.go::TestRetireTaskRequiresDurableSuccessorAndPreservesDeadLetter</code>
- <code>internal/pipeline/worker/processor_test.go::TestProcessorRetiresStaleGenerationWithoutStoppingSuccessor</code>

### B-16：missed rename 被当成新文件，file_id 改变

**发现**

scanner 看见新路径存在、旧路径消失，但 destination 没有 catalog row，早期只会生成无锚点 upsert，随后又为旧路径 remove。

**复现**

停掉或丢弃 rename 事件，只在磁盘上把文件 A rename 为 B，再运行 scanner。

**原因与根因**

算法只按 path 对账，没有利用内容外的稳定文件身份；它无法区分“新文件 B”和“旧文件 A 改名为 B”。

**修复**

用 <code>(size, mtime_ns, inode)</code> 查询 live catalog。只有唯一匹配时才推导 anchored relocate；零匹配走新 upsert，多匹配返回歧义并安全降级，绝不猜测。

**回归**

- <code>internal/store/catalog_identity_test.go::TestFindFileByIdentityRequiresUniqueLiveMatch</code>
- <code>internal/reconcile/scanner_test.go::TestScanRecognizesUniqueFileIdentityAsRelocate</code>
- <code>internal/store/reconcile_enqueue_test.go::TestEnqueueReconcileRelocateIsConditionalAndPreservesIdentity</code>

### B-17：Windows 没有取到跨 rename 稳定的文件 ID

**发现**

B-16 在 Windows 真实 E2E 中仍会退化成 create+remove。rename 前后的 inode 字段为空或不稳定。

**复现**

Windows 上对同一文件执行 Lstat、rename、再次 Lstat；只读取 <code>FileInfo.Sys()</code> 无法得到 NTFS FileIndex。

**原因与根因**

Windows 的 <code>os.FileInfo.Sys()</code> 通常是 <code>Win32FileAttributeData</code>，不包含 <code>FileIndexHigh/FileIndexLow</code>。

**修复**

新增平台实现 <code>fsmeta.InodeAt</code>。Windows 以共享 read/write/delete 打开 handle，调用 <code>GetFileInformationByHandle</code>，组合 64 位 FileIndex；scanner 和 IO stage 统一使用该入口。非 Windows 保留 syscall inode。

**回归**

- <code>internal/fsmeta/inode_test.go::TestInodeAtSurvivesRename</code>
- <code>internal/fsmeta/inode_test.go::TestInodeRecognizesUnixAndWindowsShapes</code>
- <code>internal/lifecycle/lifecycle_test.go::TestRealWatchEventsConvergeAndMovesDoNotExtract</code>

### B-18：Windows case-only 路径产生重复 catalog/task

**发现**

SQLite raw path 比较大小写敏感，而 Windows 文件系统通常不敏感。<code>C:\Root\File.txt</code> 和 <code>c:\root\FILE.TXT</code> 可占用两个 row；仅 casing 变化还会被“metadata unchanged”提前返回。

**复现**

以不同 casing 对同一路径执行 upsert/get/bump/enqueue/claim，或在磁盘做 case-only rename。

**原因与根因**

展示字符串同时被当成持久身份键，数据库和宿主文件系统的路径等价关系不一致。

**修复**

- migration 0002 增加 <code>files.path_key</code>、<code>tasks.path_key</code>、<code>tasks.old_path_key</code>。
- Windows key 为 Clean 后大小写折叠；其他平台保留精确 Clean。
- 唯一槽、claim、recovery、coalesce、prefix paging 和 conditional enqueue 都使用 key。
- raw path 继续保存最近观察到的展示 casing。
- scanner 的 unchanged 判断同时要求 raw path 相同，使 case-only rename 能更新展示路径。

**回归**

- <code>internal/store/pathkey_test.go::TestWindowsCaseOnlyCatalogAndReconcileUsePathKey</code>
- <code>internal/store/pathkey_test.go::TestWindowsCaseOnlyTaskSlotsClaimAndRecovery</code>
- <code>internal/store/pathkey_test.go::TestWindowsCaseOnlyPrefixPagingUsesPathKeyCursor</code>
- <code>internal/reconcile/scanner_test.go::TestScanPersistsWindowsCaseOnlyPathChange</code>
- <code>internal/store/directory_test.go::TestEnqueuePrefixRemovalsSupersedesCaseFoldedDoneSlot</code>

### B-19：path_key 迁移可能破坏旧库冲突数据

**发现**

构造 legacy 数据库时发现，它可能已同时包含仅大小写不同的两个 files 或同 state tasks。直接 lower 回填会产生顺序相关冲突。

**复现**

创建 v1 schema，插入大小写折叠后相同的 rows，再用新版本打开。

**原因与根因**

迁移前没有全量 collision audit；若边回填边建唯一约束，失败时可能只完成一部分。

**修复**

迁移先加载并验证所有规范化 key；冲突返回 <code>ErrPathKeyCollision</code>。drop/rebuild index、回填、trigger 和算法版本写入位于同一个事务，失败时原数据不变。

**回归**

- <code>internal/store/pathkey_test.go::TestPathKeyMigrationBackfillsLegacyRowsAndIndexes</code>
- <code>internal/store/pathkey_test.go::TestPathKeyMigrationRejectsWindowsCaseCollisionsWithoutDataLoss</code>

### B-20：Windows prefix 分页的排序键与 cursor 不一致

**发现**

引入 path_key 后继续审计分页：结果按 path_key 排序，却可能使用 raw path 作为 after cursor，导致跳行或循环。

**复现**

插入不同 casing 的 <code>a.txt/B.txt/c.txt</code>，用 limit 1 和不同 casing 的 root/cursor 逐页读取。

**原因与根因**

ORDER BY、范围过滤和 cursor 不在同一个 durable ordering domain。

**修复**

prefix filter、<code>&gt; after</code> 和 ORDER BY 全部使用 path_key；返回值仍保留 raw path。

**回归**

- <code>internal/store/pathkey_test.go::TestWindowsCaseOnlyPrefixPagingUsesPathKeyCursor</code>
- <code>internal/store/catalog_test.go::TestListFilesByPrefixPageSeparatorsAndLiteralCharacters</code>

### B-21：root 暂时不可用被解释成全部文件已删除

**发现**

对 unavailable root 做启动扫描时，catalog 下所有 row 可能被批量 enqueue remove，dirty 还可能被错误 acknowledge。

**复现**

catalog 预置 root 下文件，但让 root 不存在、无权访问或临时卸载，再触发 scan。

**原因与根因**

空 walk 结果无法区分“权威目录为空”和“根本无法取得权威视图”。

**修复**

扫描前 Lstat root，要求真实目录且不是 symlink/reparse；不可用返回 <code>ErrRootUnavailable</code>，不生成 remove、不确认 dirty、不结束 warming，并按退避重试。

**回归**

- <code>internal/reconcile/scanner_test.go::TestUnavailableRootDoesNotGenerateMassRemovalOrAcknowledge</code>

### B-22：扫描过程中 root 被替换仍会删除旧 catalog

**发现**

同一路径的 mount/directory 在 walk 前后可能已不是同一对象；仅检查路径存在会把新空目录当作旧目录的权威结果。

**复现**

fake FS 第一次返回 root A 的 FileInfo，walk/removal 阶段改为 root B，catalog 中保留 A 的文件。

**原因与根因**

路径字符串不等于 root identity，尤其在卸载重挂、目录删除重建时。

**修复**

保存 scan 前 root identity；walk 后及每个 removal page 提交前再次 <code>SameFile</code>。身份变化使整轮失败，不产生 removals。

**回归**

- <code>internal/reconcile/scanner_test.go::TestRootIdentityChangeDuringScanCannotGenerateRemovals</code>

### B-23：watcher degraded 反而禁用了 scanner

**发现**

审查 lifecycle 到 scanner 的 root 状态映射时发现，旧逻辑只有 <code>RootActive</code> 才设置 Available。watch backend 永久失败时，可读 root 也不扫描。

**复现**

使用始终返回 backend unavailable 的 watcher factory，但 root 真实存在并含 <code>scan-only.txt</code>。

**原因与根因**

混淆“事件后端是否健康”和“文件系统是否可权威读取”。watcher 越不健康，scanner 兜底反而越重要。

**修复**

root 仅在 <code>RootStopped</code> 时对 scanner 不可用；pending/active/degraded 均允许扫描。是否权威由 scanner 自己的 Lstat/SameFile 判断。

**回归**

- <code>internal/reconcile/watch_integration_test.go::TestDegradedWatcherDoesNotGateAuthoritativeScan</code>
- <code>internal/reconcile/scanner_test.go::TestUnavailableRootDoesNotGenerateMassRemovalOrAcknowledge</code>

### B-24：扫描期间再次丢事件，旧扫描会错误清除 dirty

**发现**

对 dirty acknowledge 做并发审计：scanner 读取 dirty=true 后开始扫描；期间 watcher 又 overflow 或投递失败，旧扫描结束时仍可能把 root 标 clean。

**复现**

1. 记录 dirty generation N 并启动扫描。
2. 扫描期间再次 MarkDirty，变为 N+1。
3. 用旧扫描结果执行 acknowledge。
4. 若状态只是 bool，N+1 会被错误清除。

**原因与根因**

dirty 只有布尔值，缺少 compare-and-clear 版本；不能区分“本轮开始前的丢失”和“本轮进行中的新丢失”。

**修复**

每次 loss 单调增加 DirtyGeneration；ack 只有 root epoch 和 observed dirty generation 都匹配才清除，否则保留 dirty 并重新排队。

**回归**

- <code>internal/watch/manager_test.go::TestManagerBackpressureAndOverflowMarkDirtyIdempotently</code>
- <code>internal/reconcile/watch_integration_test.go::TestDroppedWatchSubmissionMarksDirtyAndScannerConverges</code>

### B-25：remove/re-add 同路径 root 存在 ABA

**发现**

旧 root 的扫描可能在 remove 之后完成，并把刚刚重新 add 的同路径新 root 确认为 clean。

**复现**

add path 得到 epoch 1，remove，再 add 同 path 得到 epoch 2；让 epoch 1 的旧扫描最后 acknowledge。

**原因与根因**

root identity 只使用 path；remove/re-add 后字符串相同，但它们是两个生命周期。

**修复**

每次 Add 分配单调 epoch；scanner queue、active、ack、fence 都携带 <code>(path, epoch)</code>。旧 epoch 结果不能修改新 epoch 状态。

**回归**

- <code>internal/watch/manager_test.go::TestRootEpochPreventsDirtyAcknowledgeABA</code>

### B-26：FenceRoot 在 takeReady 与 registerActivity 间有空窗

**发现**

work 已从 pending 取出、尚未登记 active 时调用 FenceRoot；fence 看不到它并立即返回，旧扫描随后仍注册和运行。

**复现**

手动把旧 epoch work 执行到 takeReady 后暂停，调用 FenceRoot，再继续 registerActivity。

**原因与根因**

状态模型只有 pending 和 active，没有“已经 dispatch、尚未 register”的可见阶段。

**修复**

增加 per-root <code>dispatching</code> 计数和 blocked epoch tombstone。Fence 删除 pending 并写 tombstone；register 必须检查 tombstone 并拒绝旧 work。

**回归**

- <code>internal/reconcile/scanner_test.go::TestFenceRootCancelsAndJoinsActiveScan</code>
- <code>internal/reconcile/scanner_test.go::TestFenceTombstonesSurviveDispatchGapAndArePrunedAcrossEpochs</code>

### B-27：root fence tombstone 在动态 churn 下无界增长

**发现**

B-26 引入 tombstone 后做 64 次 remove/re-add 压测，若不回收，每个 epoch 都永久留在 blocked map。

**复现**

同路径连续 fence、re-add 64 个 epoch，观察 blocked/dispatching/active 状态大小。

**原因与根因**

为关闭 dispatch gap 引入了保守墓碑，但最初没有定义安全删除条件。

**修复**

仅当旧 epoch 已不在 provider，且 <code>dispatching=0</code>、无 active round 时 prune。这样既不能在 register 空窗提前删，也不会长期泄漏。

**回归**

- <code>internal/reconcile/scanner_test.go::TestFenceTombstonesSurviveDispatchGapAndArePrunedAcrossEpochs</code>

### B-28：PreserveIndex 删除 root 时跳过 scanner/debounce fence

**发现**

动态 root removal 的 keep-index 分支不应删除 catalog，但旧扫描或已接纳 debounce work 仍可能在 removal 返回后入队。

**复现**

分别执行 destructive removal 和 preserve-index removal，记录 scanner fence、debounce flush、prefix removal hook 的调用顺序。

**原因与根因**

fence 与 destructive prefix deletion 被绑成同一个条件分支；keep-index 因不删除 catalog而跳过了整个并发隔离。

**修复**

拆成两个 hook：

- <code>RootFenceHook</code>：两种 removal 都同步执行，先 fence scanner，再 flush debounce prefix。
- <code>PrefixRemovalHook</code>：只在 destructive removal 执行。

**回归**

- <code>internal/watch/manager_test.go::TestManagerDynamicRemoveSynchronizesPrefixExpansion</code>
- <code>internal/debounce/debounce_test.go::TestFlushPrefixFencesAcceptedChanges</code>

### B-29：destructive root removal 漏掉 pre-catalog 活跃任务

**发现**

root catalog 可能为空，但其下已有 pending、in_flight、retry_wait 或 waiting_dep 的无 file ID upsert。只枚举 catalog children 时，这些任务会在删除 root 后重新建立索引。

**复现**

为 root 子路径分别准备四种 active state、且不建 catalog row，再调用 <code>EnqueuePrefixRemovals</code>。

**原因与根因**

目录/根删除逻辑只把 catalog 当作待删除集合，忽略 durable task 本身也是未来状态来源。

**修复**

同一事务中同时扫描 catalog 和 prefix active tasks：

- pending 原地 coalesce 成更高代 remove；
- in-flight 保留当前 lease并建立 successor remove；
- retry_wait/waiting_dep 行政性 supersede；
- catalog child 统一 bump/remove；
- 处理 path boundary 和 case-folded done slot。

**回归**

- <code>internal/store/directory_test.go::TestEnqueuePrefixRemovalsFencesPreCatalogActiveTasks</code>
- <code>internal/store/directory_test.go::TestEnqueuePrefixRemovalsSupersedesCaseFoldedDoneSlot</code>
- <code>internal/store/directory_test.go::TestEnqueuePrefixRemovalsIsAtomicAndParentless</code>

### B-30：Manager shutdown 未等待已接纳的 removal hook

**发现**

并发审计发现 RemoveRoot 的 fence/prefix hook 在调用方 goroutine 执行，不属于 root watcher errgroup。Manager.Run 只等待 consumer，可能先返回。

**复现**

启动 RemoveRoot 并让 PrefixRemovalHook 阻塞，同时 cancel Manager.Run。旧实现可能在 hook 仍使用 scanner/debounce/store 时返回。

**原因与根因**

动态管理调用未纳入 Manager 生命周期所有权树；还必须避免 <code>WaitGroup.Wait</code> 开始后从零计数 Add。

**修复**

Manager 增加 removal WaitGroup。RemoveRoot 在 manager mutex 下检查状态并 Add；Run 在同一锁下先切为 stopping、拒绝新 removal，再等待 root consumers 和全部 admitted removal hooks。

**回归**

- <code>internal/watch/manager_test.go::TestManagerShutdownJoinsAdmittedRemovalHooks</code>

### B-31：shutdown deadline 后关闭仍被活组件使用的资源

**发现**

对关闭流程做对抗性测试时，scheduler 收到 cancel 后延迟发送，processor 收到 cancel 后故意不退出。旧 lifecycle 把“已 cancel”近似成“已停止”。

**复现**

1. scheduler 取消后继续 20 ms 再向 leases 发送，shutdown timeout 设为 2 ms。
2. 旧实现先关闭 leases，会触发 <code>send on closed channel</code>。
3. 另一用例让 processor 等待人工 release，timeout 20 ms。
4. 旧实现可能关闭其下的 Tantivy projection/store，或最终无界 group.Wait。

**原因与根因**

CancelFunc 只表达停止请求，不是 goroutine exit 回执。channel、projection、SQLite 的释放必须由组件真实退出决定。无条件最终 Wait 还会使全局 deadline 失效。

**修复**

- 每个组件提供明确 exit 回执。
- 严格按 watch → reconcile → debounce → scheduler → processor → reliability → writer → projection → metrics 关闭。
- scheduler 确认退出后才能关闭 leases。
- processor/writer 都退出后才能关闭 projection。
- deadline 到期且仍有组件存活时返回 <code>ErrComponentsLive</code>，不再无界 Wait。
- 顶层设置 abandon-resources，不在 live goroutine 下关闭 native/store/logger；clean marker 保持 false，留给下一次 crash recovery。

**回归**

- <code>internal/lifecycle/lifecycle_test.go::TestSchedulerTimeoutDoesNotCloseLiveOutput</code>
- <code>internal/lifecycle/lifecycle_test.go::TestShutdownTimeoutReturnsWithoutClosingProjectionUnderLiveProcessor</code>
- <code>internal/lifecycle/lifecycle_test.go::TestComponentTreeUsesStrictShutdownOrder</code>
- <code>internal/lifecycle/lifecycle_test.go::TestComponentErrorLeavesShutdownMarkerFalse</code>

### B-32：每 root 的 stat “小池”实际上仍是串行

**发现**

对照规格 §6.10 审计：早期把 stat 直接放在 <code>filepath.WalkDir</code> callback 中。callback 本身串行，所以即使外围有限流器，也没有并发。

**复现**

创建至少 12 个文件，注入阻塞 Lstat，期望默认有四个同时进入；旧实现只能观察到一个。

**原因与根因**

遍历、metadata 获取和 store visitor 没拆成流水线；性能上不并发，错误/取消路径也缺少统一 cancel/join。

**修复**

- WalkDir 只生产 path。
- 使用容量等于并发数的 paths/results channel。
- 默认四个 stat worker。
- 单一串行 visitor 执行 store 条件入队。
- walk/stat/visit 任一错误取消 scoped context。
- 返回前按顺序关闭 channel 并 join worker/visitor，FenceRoot 能真正等待完整 round。
- root 外层用 limit 4 控制并发 root rounds。

**回归**

- <code>internal/reconcile/osfs_test.go::TestOSFileSystemWalkUsesBoundedParallelStatAndSerialVisit</code>
- <code>internal/reconcile/osfs_test.go::TestOSFileSystemWalkVisitErrorJoinsWorkers</code>
- <code>internal/reconcile/osfs_test.go::TestOSFileSystemWalkCancellationJoinsWorkers</code>

## 6. 测试与诊断问题

这些问题影响开发判断或门禁稳定性，但不等同于生产逻辑缺陷。

### T-01：1000 文件 race 验收的 30 秒 deadline 过紧

**发现与复现**

<code>go test -buildvcs=false -race -count=1 ./internal/pipeline/worker</code> 在 30 秒时约完成 905/1000。done 数持续增长，scheduler、worker、writer 都在前进；单独重复通常约 39–48 秒，完整 race suite 在负载下可接近 70 秒。

**判断**

这是 race detector 对 SQLite/Tantivy CGO durable path 的吞吐放大，不是死锁。

**修复**

只把首轮等待放宽到有界 90 秒；保留 1000 文件、1000 search hit、幂等重复、move 零抽取等全部行为断言。真实吞吐继续由 <code>BenchmarkTextPipeline</code> 负责测量。

**回归**

- <code>internal/pipeline/worker/processor_test.go::TestTextPipelineIndexesThousandFilesAndShortCircuitsRepeat</code>

### T-02：外层工具 60 秒超时制造假失败

完整 race 命令曾在工具层以 exit 124 超时，尽管各 package 仍持续输出通过。将命令预算提高到 180 秒后完成；这不对应任何代码修复。

### T-03：Lifecycle E2E 吞掉后台真实错误

**发现**

目录移动失败时，表面只报告 <code>waitCatalogStatus</code> 或 metric 20 秒超时；lifecycle goroutine 可能早已因 store/worker Fatal 退出。

**根因**

测试只轮询目标条件，没有及时读取 lifecycle 返回值；timeout 也没有 durable state。

**修复**

cleanup 必须报告 lifecycle error。等待超时附带：

- task_id、path、op、generation、state、old_path、priority；
- file_id、path、generation、status、inode、size、mtime、hash/indexed state；
- extract 和 move_fast_path 指标。

这项诊断改进直接暴露了第 4 节的 generation 覆盖链。

### T-04：新目录递归 watch 挂接存在测试时序

真实 E2E 在 Mkdir 后立即写第一个 child 时，第三方 backend 可能尚未完成新子目录 watch 注册。测试在 Mkdir 后等待 200 ms 再写 child，以稳定验证目录增量推导；这没有被认定为 filecat 功能 Bug。生产正确性仍由明确 watcher-loss 和周期扫描兜底。

### T-05：新增代码使 store 覆盖率短暂降到 79.7%

新增 <code>RetireTaskIfSuperseded</code> 后，store 覆盖率曾跌破 80% 门禁。没有写无意义行覆盖，而是增加“必须存在 durable successor、且保留 dead-letter”的真实事务回归，最终为 81.2%。

### T-06：PowerShell coverprofile 参数被错误解析

一次 coverage 命令把输出名解析成了额外 package，出现类似 <code>FAIL .out</code>。改用带引号的 <code>-coverprofile 'store.cover'</code> 后恢复。属于命令行解析问题。

## 7. 开发环境问题

### E-01：异常父级 .git 导致 Go VCS stamping 失败

开发时父级 <code>.git</code> 为空或不可用，Go 自动读取 VCS build info 失败。与业务代码无关。本轮 build/test/race 使用 <code>-buildvcs=false</code>；当前仓库状态变化后未必还能复现。

### E-02：Windows/sandbox 默认 Go cache 和 temp 不可写

受限环境中默认 GOCACHE/GOTMPDIR 位于 writable root 外；Windows 对仍打开的 exe、SQLite 和 Tantivy/native 文件也有更严格的替换/删除锁。

本轮稳定运行约定：

~~~powershell
$env:GOCACHE=(Resolve-Path '.gocache').Path
$env:GOTMPDIR=(Resolve-Path '.gotmp').Path
go test -buildvcs=false -race -count=1 ./...
~~~

测试使用唯一 <code>t.TempDir()</code>，并在清理前严格关闭 writer、Tantivy engine 和 SQLite store。

## 8. 为什么这些问题不是 filecat-go 的主责

本轮没有证据证明 filecat-go 错误表达了目录语义。已观察到的核心事实是：

- 目录 move 提供 old/new path。
- 后代 create/modify/remove 事件可用于增量推导。
- 最终错误 task 是 watcher 和 scanner 在本节点 SQLite 队列汇合后产生的。
- 目录移动失败时，新路径通常已经可见，只是 file ID、generation 或快路径被内部任务覆盖。

与 filecat 直接相关的唯一测试时序是“新建目录后递归 watch 注册不是瞬时完成”；测试等待 200 ms 只是稳定测试前置。生产系统不依赖所有事件必达，明确的 overflow、admission loss、watcher failure/reopen 和周期 scan 仍承担恢复职责。

## 9. 当前仍存在的边界与测试缺口

以下不是已知未修复数据错误，但应在后续里程碑继续跟踪：

1. **全扫描仍为 O(N)**：并发有界但大冷 root 仍消耗 metadata bandwidth。
2. **50,000 文件/write-storm stress 尚在 M9**：当前功能/race 测试不能替代长时间内存峰值验证。
3. **显式 TriggerRescan 在 M8**：当前只有内部 startup/dirty/periodic 触发。
4. **“正常目录事件绝不触发 full scan”缺少一对一 round-count 断言**：现由 manager 不 MarkDirty、目录分类测试和真实 E2E 间接覆盖。
5. **最多四个并发 root round 缺少独立命名测试**：单 root 内四 stat worker 已有直接测试。
6. **symlink/reparse root 拒绝缺少独立测试名**：root identity swap 已覆盖，但实际 symlink/reparse 应补专项。

## 10. 可复用的排障方法

这次最有效的不是只看“最终是否 indexed”，而是同时验证身份、代际和工作量：

1. 先用真实 watcher E2E 复现，不先假设第三方事件丢失。
2. 条件超时时 dump durable tasks 和 catalog，而不是只输出 timeout。
3. 对每条 task 追踪 <code>task_id、file_id、op、old_path、path、generation、state</code>。
4. 同时看 <code>extract</code> 和 <code>move_fast_path</code>；最终路径正确不代表执行路径正确。
5. 把每个修复做反向序列：
   - in-flight 不覆盖会怎样；
   - in-flight 全覆盖又会怎样；
   - old_path 覆盖 remove 后，源路径重新创建会怎样。
6. 竞争判断放进一个 SQLite 写事务，不在 Go 层做“先查再写”。
7. 关闭流程用 exit 回执证明 goroutine 已退出，不能用 cancel 调用代替。
8. 性能修复使用 fake clock 和并发计数器，避免用 sleep 猜测。

## 11. 最终结论

M3 的主要问题不是 scanner 或 watcher 单独失效，而是两条正确但异步的观察路径，在 durable queue 汇合时缺少完整的 generation、identity、coverage 和 lifecycle 协议。

最终方案把正常目录操作保持为增量路径；只有真实事件不可靠证据才提升为全 root scan。即使扫描与 watcher 再次并发，也由事务化条件入队、direct in-flight coverage、指数退避权威复扫、path ownership 和最终 generation fence 保证收敛。因此“收紧全扫描触发面”不会重新引入原来的并发错误，反而减少了发生频率；正确性不再依赖扫描和 watcher 恰好错开。
