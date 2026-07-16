结论：当前 M0–M5 不能认定为 “P0/P1 clean”。整体方向扎实、测试基线全绿，但仍存在多处跨组件一致性和资源所有权缺口；其中一些确实是“补丁压住已知复现，但没有消除根因”。

本次严格只读，没有修改、格式化、暂存任何文件。

## P1 级核心问题

### 1. B07 只约束响应顺序，没有约束向量落盘顺序

证据集中在 [batcher.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/embed/batcher.go:474>)、[manager.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/reliability/manager.go:279>)、[model_upgrade.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/model_upgrade.go:153>)、[processor.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/worker/processor.go:442>) 和 [vectors.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/vectors.go:179>)。

复现：

1. epoch 1 的 v1 响应先通过 `observeModel`，暂停在 worker 调用 `VectorProjector.Replace` 之前。
2. epoch 2 的 v2 响应完成 adoption，并执行一轮迁移扫描。
3. v1 文件此时已被 Prepare 为 `pending`，不满足迁移查询的 `status=indexed AND indexed_at IS NOT NULL`，因此被漏过。
4. 放行 v1 worker，v1 truth 在 active=v2 后提交，HNSW 可重新切回 v1。
5. 后续 v2 响应被同模型快速去重，不再启动扫描。

现补丁有效，是因为它能拒绝“先 dispatch、后返回”的旧响应；但它管不到响应离开 batcher 之后的持久化乱序。

根治应让 embedding 携带 durable model/adoption epoch，并在向量 truth 事务中核对当前 active model/epoch。过期写必须在删除旧 truth 之前被拒绝，并以“superseded、零尝试计费”重新嵌入。多副本 compute 还需要服务端 rollout epoch；客户端 dispatch epoch 无法区分正式回滚与负载均衡命中旧副本。

### 2. Tantivy 提交存在双 owner 和 batch-wide 污染

相关代码是 [writer.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/writer.go:153>)、[writer.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/writer.go:308>)、[taskqueue.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/taskqueue.go:368>) 和 [processor.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/worker/processor.go:602>)。

有两个独立问题：

- 请求进入 writer channel 后，writer 已拥有操作，但调用方仍可因 `CommitTimeout` 返回并执行 `MarkRetry/MarkDead`；writer 随后仍可能提交并 `MarkDone`。
- batch 中一个任务在 generation 预读后变旧，会令整个 SQLite receipt 事务回滚，错误再广播给所有 batchmate。无辜任务没有 successor 时会 fatal。

复现第二项：批次含 A/gen1、B/gen1；generation 预读后把 A 推进 gen2；Tantivy 提交成功，SQLite 在 A 上发现 stale，整批 receipt 回滚；B 收到相同 stale 错误，却没有 successor，于是节点停止。

B-15 的“只有存在 durable successor 才退休 stale task”对单任务竞争是正确的，但无法解释 batch-wide 错误。

根治：

- context 只控制 admission；一旦 accepted，只有 writer 能转换完成状态。
- receipt 必须重新核对权威 generation 并返回逐任务结果，而非一个错误广播整批。
- 引入 durable projection intent/receipt；提交结果不确定时由投影恢复状态机处理，processor 不得并发转换任务。
- [tantivy.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/tantivy.go:264>) 在 native commit 成功后再次返回 `ctx.Err()`，也应纳入同一所有权重构。

### 3. `files` 同时承担对象身份和当前位置

[0001_core.sql](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/migrations/0001_core.sql:1>) 将 path 设为唯一，[pathkey.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/pathkey.go:101>) 又令 `path_key` 唯一；deleted row 仍占有该路径。[RelocateFile](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/catalog.go:461>) 则拒绝任何其他 owner。

复现：

1. 索引 A、B。
2. 删除 B，B 的 tombstone 仍占有 path B。
3. 执行覆盖式 rename A→B。
4. A 的 relocate 遇到 B owner，返回 `ErrPathOwnership`。
5. 没有更高 generation successor，[processor.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/worker/processor.go:211>) 将其升级为 fatal。

现有 ownership fence 能防止两个对象静默合并，但无法表达合法的目标替换；同路径删除后重建还会复用旧 `file_id`，导致未来 notes 错绑。

根治需要通过新迁移和 ADR 拆分：

- `files`：稳定对象身份。
- `file_locations`：当前活动路径，只对 active location 建唯一约束。
- 覆盖式 relocate 在一个事务中撤销目标位置、迁移源位置，并产生两个对象的 projection intent。

### 4. M3 文件身份缺少设备/卷命名空间；root fence 仍有 TOCTOU

Unix 只保存 inode，Windows 只保存 FileIndex，见 [inode.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/fsmeta/inode.go:29>) 和 [inode_at_windows.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/fsmeta/inode_at_windows.go:31>)；[FindFileByIdentity](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/catalog_identity.go:14>) 却在全 catalog 查询。

因此不同卷中相同 `(size,mtime,inode/FileIndex)` 会被误判为 missed rename，并错误继承 `file_id`。B-16/B-17 补上 Windows FileIndex 后解决了同卷 rename，但没有补 identity 的作用域。

此外，[scanner.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/reconcile/scanner.go:713>) 在 post-walk root 验证前已经逐文件入队；removal 页也只是点检查，verify 后到 enqueue 前仍可替换 root。

根治：

- Unix 持久化 `st_dev + st_ino`。
- Windows 持久化 volume serial + 文件 ID，优先 `FILE_ID_INFO`。
- scan task 携带 durable `root_id/root_epoch`。
- 尽可能持有 root directory handle 做相对遍历；否则先 staging diff，最终验证通过后再发布任务。

### 5. B01 与 B02 都只完成了表层闭环

B01：failed 文件崩溃窗口仍可由旧正文触发命中。

`MarkDead` 先写 catalog failed，之后才通过 [ensureDeadLetterProjection](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/lifecycle/lifecycle.go:551>) 清理 Tantivy 正文。查询虽然取到了 `KeywordHit.Generation/Status`，但 [rankKeywordHits](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/search.go:540>) 完全忽略它们，只清空 snippet。

复现：索引包含唯一秘密词的正文；下一代失败；在 `MarkDead` 后、最小 failed 文档提交前崩溃；按秘密词搜索仍会返回该文件，只是不显示 snippet。

补丁解决了输出泄漏，却没有证明“命中来自 filename/path”。根治应要求 hit 的 projection generation/status 与 committed failed receipt 一致，或支持字段来源并让 failed route 只检索 filename/path。

B02：几何扩窗能解决“过滤后不足 TopK”，但不能证明 hybrid RRF 排名正确。

[search.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/search.go:194>) 只要已有 TopK 就立即返回。`TopK=1` 时初始窗口为 4；若 keyword、semantic 前四名互不重叠，而共同的第五名存在，则共同第五名 RRF 为 `2/(60+5)`，高于单路第一名 `1/(60+1)`，当前实现却不会扩窗。

根治应使用 backend offset/cursor，或计算严格的 unseen-score upper bound；无法证明排名稳定时，即使已有 TopK 也必须继续扩窗或标记 `Incomplete`。

### 6. B04 的 keyword 可用性仍依赖时序

模型迁移入队时保留 indexed 是正确改进，但 worker 在 RPC 前通过 [PrepareFileForTask](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/worker/processor.go:442>) 把文件改为 pending。compute 宕机后，controller 只把 task 改成 `waiting_dep`，而 [keyword eligibility](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/search.go:590>) 排除 pending。

因此迁移图片会在整个 compute outage 期间从 keyword/hybrid 消失。这是 bug log 中失败方案 E 的“缩短窗口版”。

根治是把以下状态拆开：

| 状态 | 唯一含义 |
|---|---|
| work state | pending/in-flight/waiting |
| keyword projection | committed generation/status |
| vector truth | committed generation/model |
| ANN projection | model/dims/revision |
| desired model | rollout epoch/version/dims |

model-only reembed 不应破坏已提交的 keyword receipt。

### 7. B05 只修了 Replace；vector projection 故障又被当作文件失败

[applyRequest](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/index/vector.go:542>) 对 Replace gap 使用 `request.model`，但 Delete gap 调用 `rebuildFromStore("")`，再由 [LatestVectorModelVersion](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/store/vectors.go:477>) 猜测。在迁移期，旧 indexed v1 可以压过当前 v2，随后代码仍确认最新 revision。

此外，truth 已经提交后若 replay/rebuild 失败，错误返回 worker，未知错误按 transient 处理，于是重复 decode、RPC 和 truth write，最终甚至把有效文件 dead-letter。

根治：

- Delete request 必须携带精确 active projection model；非空图禁止 heuristic。
- truth 提交与 ANN 恢复分离。truth 成功后，由 vector component 根据 durable watermark 自行重建。
- HNSW invariant、change-log corruption、rebuild/store failure 应是组件级 fatal；输入契约才是 permanent；generation/model supersede 零计费。
- failed 终态也必须事务化产生 vector-delete intent，避免旧向量长期占满 ANN 前 1000 名。

### 8. lifecycle 的所有权保护遗漏了新消费者

[lifecycle.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/lifecycle/lifecycle.go:819>) 忽略 `await(componentReliability)` 的结果；关闭 projection 的条件也不包含 reliability。但 reliability 会在 [manager.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/reliability/manager.go:467>) 调用 CommitWriter/Tantivy。

复现：让 reliability 在 projection hook 中忽略取消并阻塞，writer/vector 已退出；shutdown deadline 到达后，projection 仍可能在 live reliability 下被关闭。

这是 M3“必须等 exit receipt”的修复在 M4 新增 consumer 后没有更新依赖列表。同步 `closeEmbedding/closeProjection` 又位于 deadline 外，仍可无限阻塞。

根治是显式组件依赖 DAG：

- 每个组件声明 consumes/owns。
- 只有全部 consumer 的 exit receipt 到齐才关闭 dependency。
- closer 也作为 managed phase 受同一 deadline 管理。
- deadline 后返回 `ErrComponentsLive` 并 abandon，不在活组件下关闭资源。

[lifecycle.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/lifecycle/lifecycle.go:647>) 为旧测试把缺失 M5 组件静默替换成 idle，也是明显的测试补丁；生产 tree 应 fail-fast，测试 helper 显式注入 fake。

### 9. 两个资源边界没有真正实现

- `resources.min_free_bytes` 只定义和校验，生产代码零引用。低于阈值仍继续 Claim，ENOSPC 后则在 [processor.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/worker/processor.go:226>) 直接终止节点，违反任务书要求的 pause/degraded/自动恢复。
- B15 只按源图 decoded pixels 计费；[image.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/media/image.go:368>) 的 `image_size²×4` 目标 RGBA 不计入 semaphore。[config.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/config/config.go:320>) 允许 `image_size=200000,image_max_pixels=1,image_bytes_inflight=4`，处理 1×1 PNG 会尝试约 160GB 分配。

B15 的像素预检和 panic boundary 对压缩炸弹有效，但 Go runtime OOM 不是可 recover 的普通 panic。

根治分别是带滞回的磁盘资源监督器 + Scheduler Pause/Resume，以及对源图、目标 RGBA、编码缓冲同时存活峰值做溢出安全预算。

### 10. audit、backfill 和 compute 分类仍有可靠性缺口

- [audit.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/obs/audit.go:141>) 发生 partial write 后会留下残缺 JSON 前缀；outbox 重试会把完整 JSON 接到残片后面。根治需 write-all、失败回滚到写前 offset，并在启动时修复不完整尾帧，或改用可恢复 framing。
- [media_backfill.go](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/lifecycle/media_backfill.go:45>) 对 sniff open/read error 静默返回 nil，仍推进 cursor 并写 complete marker。无扩展名图片在一次临时 AccessDenied 后可永久漏过。根治需 durable per-candidate 状态或至少不确认全局完成。
- compute classifier 在 lifecycle/maintenance 复制两份；`InvalidArgument/Unimplemented` 最终会按 unknown→Transient 重试八次，`ResourceExhausted` 又不会进入 waiting_dep。应有唯一 typed transport taxonomy，search degradation 只消费 availability 类型。

## 任务书架构审计

任务书要求每阶段独立资源池和有界 channel，见 [开发任务书](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/Index-Node 开发任务书.md:373>)。当前 [Processor.Run](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/internal/pipeline/worker/processor.go:163>) 的一个 worker 从 IO 一直同步执行到 embed/vector/Tantivy。

这意味着一批慢 compute 图片可占满所有 processor worker，文本任务即使 CPU/IO 空闲也无法推进。它资源有界，但不是任务书规定的 SEDA 隔离，也没有对应 ADR。根治时应保留规定的 package/组件骨架，把 stage ownership 和 bounded handoff 真正拆开，而不是继续向单体 worker 增加分支。

## M5 bug 逐项结论

| 项目 | 审计判断 |
|---|---|
| B01 failed eligibility | 部分有效；仍有旧正文假命中 |
| B02 overfetch | 不完整；过滤数量正确性改善，但 RRF 排名不可证明 |
| B03 冻结启动模型 | 基本优雅；runtime accessor 正确 |
| B04 durable reembed | 不完整；复用全局 pending 破坏 keyword 可用性 |
| B05 runtime gap | 部分有效；Replace 修好，Delete 未修 |
| B06 dims drift | 优雅；durable `(version,dims)` contract 在 truth 前校验 |
| B07 late response | 部分有效；缺 downstream commit fence 和服务端 rollout epoch |
| B08 delta delete recovery | 架构正确；POSIX rename 后缺 parent-dir fsync |
| B09 vector delete ordering | 正常 remove/type-change 路径正确；terminal failed vector 未清理 |
| B10 atomic parking | 批次事务正确；parking store failure 缺 Fatal 类型 |
| B11 double transition | 优雅；controller 独占 waiting_dep 转换 |
| B12 legacy backfill | 部分有效；sniff error 可被 complete marker 永久吞掉 |
| B13 degradation | 分层方向正确；gRPC status taxonomy 不完整且实现重复 |
| B14 response validation | 优雅；验证和 L2 normalization 边界清楚 |
| B15 decoded memory | 部分有效；源图受控，目标/输出缓冲未计费 |

因此 [PROGRESS.md](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/PROGRESS.md:171>) 和 [M5 bug log](</D:/GoLand 2026.1.2/PROJECT/Ferret-new/packages/index-node/docs/m5-development-bug-log.md:20>) 的 “P0/P1 clean” 结论应撤回。

## 第三方内部问题

除 filecat-go 外，本轮确认：

- `coder/hnsw v0.6.1`：同 key `Add` 会触发内部长度 invariant panic。当前 Delete→Add + adapter recover 合理。
- `coder/hnsw v0.6.1`：Windows 编译路径引用 upstream renameio 不提供的 `TempFile`，属于真实内部构建缺陷。
- `coder/hnsw v0.6.1`：`Import(io.Reader)` 的实际运行契约要求 `io.ByteReader`，签名未表达要求。
- hnsw 没有独立 `efConstruction` 是 API 能力限制，不算内部 bug；当前锁内临时切换 `EfSearch` 合理。
- `tantivy-go v1.0.6` 在 [lib.rs](</C:/Users/29623/go/pkg/mod/github.com/anyproto/tantivy-go@v1.0.6/rust/src/lib.rs:319>) 明确因上游 Tantivy rollback bug 禁用了 commit-failure rollback。index-node 不能把 commit error 后的 writer 当成普通干净 transient 状态继续使用。
- `tantivy-go v1.0.6` 的 `LibInit` 无条件打印 stdout；其 `sync.Once` 又只返回首次调用栈上的局部 error，首次失败后后续调用可能返回 nil。index-node 外层 once 恰好封住了后者。
- Tantivy numeric field 缺失是绑定 API 限制，ADR 0002 已诚实记录。
- 当前没有发现新的 filecat-go 内部缺陷；测试中的 AccessDenied 是 TEMP/沙箱权限，不是 filecat 产品 bug。

## 可合并、简化的代码

不改变任务书固定骨架的前提下，建议：

- 建立共享 `pathid`：统一 case-fold、separator-aware prefix、device/volume identity；当前 store/debounce/watch/reconcile/scheduler 各有一份。
- 用组件 DAG 生成启动、取消、await、close guard，替代 `componentCount + 手写 bool 列表`。
- lifecycle/maintenance 共用 `DependencyClassifier` 和 `ModelAdopter`。
- controller 的 BatchCall/UnboundCall 共用 finish 状态机。
- directory expansion 保留 ADR 0003 的单事务，但使用 SQL prefix range；当前会扫描全 catalog 和全部 active tasks。
- vector contract 按 `(model,dims)` 一次验证，避免每帧重复查询；删除只写不读的 `keysByFile`。
- 收敛未被 production 使用且契约更弱的旧 vector API。
- 删除仅测试使用、会绕过 catalog filter 的 `maintenance.SearchKeyword` 兼容入口。
- `Makefile` coverage 目前只打印，不会在低于 80% 时失败，应变成真实 gate。

另外，Bubble Tea 默认 UI 与任务书“任何 UI 为非目标”存在架构偏离。若这是后续批准需求，不应删除，但应补一条 ADR；README 中缺失的 Artifex attribution 文件也应补齐。

## 只读验证

- `go test -mod=readonly -buildvcs=false -count=1 -timeout=300s ./...`：23 packages 全绿，33.3s。
- `go test -race -mod=readonly -buildvcs=false -count=1 -timeout=300s ./...`：全绿，74.8s。
- `go vet -mod=readonly -buildvcs=false ./...`：通过。
- 覆盖率：store 80.3%、debounce 91.6%、errclass 97.2%、scheduler 90.8%。
- `git diff --check -- packages/index-node`：通过。
- 全仓 `gofmt -l .` 仍列出一批 legacy CRLF 文件；M5 changed/new 文件本身没有被列出。因此 PROGRESS 中“整个仓库 gofmt clean”和“M5 新文件 clean”是两个不同结论。
- 工作区原有 modified/untracked 状态保持不变；本审计没有创建或修改文件。

最优先的根治顺序是：先补 model/vector commit fence 与 projection ownership；再拆 `files.status`/projection 状态和对象/路径身份；随后重构 lifecycle DAG、资源监督器与搜索完整性证明。继续逐 bug 加条件分支，会让下一阶段 M6 多帧和 M7 notes 把这些状态歧义进一步放大。