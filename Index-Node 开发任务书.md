# Index-Node 开发任务书

> 本文档是交给 AI 开发 agent 的完整规格。你（agent）的任务是从零实现一个生产级的 index-node —— 分布式非结构化索引引擎中部署在被索引设备上的节点。本文档的架构决策已经过评审，属于**约束**而非建议；实现层面的自由度在 §14 明确列出。

---

## 0. 你的角色与工作规则

你是本项目的实现工程师。遵守以下工作规则：

1. **按 §10 的里程碑顺序开发**。每个里程碑的验收测试全部通过（含 `go test -race ./...`）之前，不进入下一个里程碑。
2. **不要虚构第三方库的 API**。对 tantivy-go、filecat-go、imohash、HNSW 库等依赖，先 `go get` 固定版本，阅读其真实源码/文档（`go doc`、仓库 README、源文件），再编写封装层。所有第三方库只允许在本文档指定的封装包中被 import，业务代码只依赖封装接口——这样库的 API 与预期不符时，适配成本被隔离在一个文件内。
3. **偏离本文档的任何架构级决策，写一条 ADR**（`docs/adr/NNNN-title.md`，包含背景/决策/后果），不要静默偏离。实现级的小决策不需要 ADR。
4. **不确定且无法自行验证的问题**，在代码中留 `// TODO(question): ...` 并继续用文档中的默认值推进，不要阻塞。
5. 每个里程碑结束时更新根目录 `PROGRESS.md`：已完成、已知问题、下一步。

---

## 1. 系统背景（你需要知道的全景）

整个产品是一个非结构化索引引擎，支持索引文本类文件（txt/pdf/docx/xlsx/py/cpp/go/...）、图片、视频。有两种部署形态：

- **分布式版**：三种节点。`index-node`（本项目）部署在需要被索引的设备上，监听指定目录，自动发现、自动索引，并响应查询；`compute-node` 部署在有 GPU 的设备上，跑 AI 模型（SigLIP 图文 embedding 等）；`master-node` 分发查询、聚合结果、管理权限认证、抗高并发。节点间使用 gRPC 通信。
- **单机版**：把三种节点打包成一个应用跑在同一台机器。**本项目的代码必须为此做好准备**：compute-node 客户端和查询入口都抽象为接口，单机版只是换一种 wiring（进程内实现或 localhost gRPC），不允许出现分布式专用的硬编码逻辑。

**本仓库只实现 index-node**。master 与 compute 的实现是非目标（§13），但它们的 gRPC 契约由本仓库的 `api/proto/` 定义并对外提供。

核心搜索引擎为 tantivy（经 tantivy-go 绑定），管理两路倒排索引：

- **文件本体索引**：文本类文件抽取文本入索引；图片经 SigLIP 产生向量；视频抽取固定数量的帧 + 用户便签锚定的时间戳帧，按图片方式产生向量。
- **便签索引**：用户可对整个文件 / 文本文件的某一行 / 整张图片 / 视频的某个时间戳贴便签并填写任意内容。便签分永久与限时两种，限时便签过期后等效删除、不得被搜到。

由于引入了 SigLIP 向量，实际存在**第三路索引**：本地向量近邻索引（ANN）。查询时三路融合返回。

文件监听模块为 `github.com/lizzary/filecat-go`（已存在，直接依赖），其关键事实：

1. `Watcher.Events()` 返回合并后的事件 channel；**消费者过慢会在缓冲填满后阻塞其内部 coalescer** —— 消费 goroutine 必须零工作量。
2. 内核事件缓冲溢出时在 `Errors()` 上报 `ErrOverflow`，**被丢的事件不可恢复** —— 事件流本质不可靠，必须有扫描兜底。
3. rename 对被合成为 `EventMove`（`Path`=目标，`OldPath`=源）；同路径修改风暴在约 50ms 的 settle 窗口内折叠。事件类型共四种：`Created / Removed / Modified / Move`，路径均为绝对路径。

---

## 2. 设计哲学：五条不可妥协的原则

**P1 — 状态收敛，而非事件驱动（level-triggered reconciliation）。** 文件系统是唯一事实源，索引是持续向它收敛的投影。文件事件只是加速收敛的"提示"，不是"命令"。把事件流整个拔掉，系统靠扫描器也必须能达到正确状态。任务的语义永远是"对账这个路径"（reconcile），而非"处理这个事件"。

**P2 — 任务幂等，at-least-once。** 任何任务重放必须无害：worker 先 stat + 比对 catalog，内容未变直接短路。宁可重复劳动，不可丢失数据。所有"完成"标记在 tantivy 提交成功**之后**落盘。

**P3 — 路径级串行 + 世代号。** 同一路径任意时刻至多一个 worker 处理（调度器 in-flight 集合保证）；catalog 为每次变更分配单调递增 `generation`，任务携带它流经全流水线，提交层拒绝 `task.generation != catalog.current_generation` 的结果。两者合起来保证乱序与延迟不污染索引。

**P4 — 恢复层级：文件系统 > catalog > 索引投影。** tantivy 索引和 HNSW 向量索引都是可丢弃、可重建的投影（前者从 catalog+存储字段重建，后者从 vectors 表重建）。catalog 损坏则全量扫盘重建。**唯一不可再生的数据是便签库**——它享受最高保护等级（同步落盘、定期快照、启动校验）。

**P5 — 并发度绑定资源，而非任务（SEDA）。** 每个流水线阶段一个工作池，池的大小对准该阶段的瓶颈资源（磁盘/CPU/网络），阶段间用有界 channel 连接形成背压链；突发洪峰沉淀在磁盘上的持久任务队列里，任何环节不得无界积压内存。

---

## 3. 技术选型（固定，除非 ADR）

| 领域    | 选型                                                       | 说明                                                                                                     |
| ----- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| 语言    | Go ≥ 1.26                                                | 模块名 `github.com/<org>/index-node`（org 由使用者替换）                                                          |
| 全文索引  | `tantivy-go`（anyproto 系绑定或使用方指定的 fork）                   | CGO。只在 `internal/index/tantivy.go` 中 import                                                            |
| 文件监听  | `github.com/lizzary/filecat-go`                          | 只在 `internal/watch/` 中 import                                                                          |
| 嵌入式存储 | SQLite，驱动 `modernc.org/sqlite`（纯 Go）                     | 单文件承载 catalog / 任务队列 / 便签 / 死信 / 向量真值。WAL 模式                                                           |
| 文件哈希  | `github.com/kalafut/imohash`                             | **分块采样哈希**：默认对 ≤128KB 的文件做全量哈希，更大的文件只采样头/中/尾各 16KB 并混入文件大小，输出 128-bit。任意大小文件的哈希成本恒定                    |
| 向量索引  | 首选 `github.com/coder/hnsw`（纯 Go，泛型，支持 Export/Import 序列化） | 只在 `internal/index/vector.go` 中 import。若其 API 缺关键能力（如删除），按 §6.7 的墓碑+重建方案在封装层补齐，或经 ADR 换用 usearch Go 绑定 |
| RPC   | gRPC + protobuf（buf 或 protoc 生成）                         | proto 在 `api/proto/`，生成代码进 `gen/`                                                                      |
| 视频抽帧  | 外部 `ffmpeg` 二进制（子进程调用）                                   | 不用 CGO 绑定。可执行路径可配置，缺失时视频管线降级禁用并告警                                                                      |
| 日志    | 标准库 `log/slog`                                           | 结构化，JSON handler                                                                                       |
| 指标    | `github.com/prometheus/client_golang`                    | `/metrics` HTTP 端点                                                                                     |
| 配置    | YAML（`gopkg.in/yaml.v3`）+ 环境变量覆盖                         | 规范见 §9                                                                                                 |
| 并发工具  | `golang.org/x/sync`（errgroup / semaphore）                | 禁止裸 `go func()`，所有 goroutine 必须被 errgroup 或组件生命周期管理                                                    |

关于 imohash 的**变更检测契约**（必须写进代码注释与文档）：采样哈希不覆盖全文，理论上存在"大小与 mtime 不变、仅采样盲区被修改"而漏检的情况。因此检测顺序固定为：先比对 `(size, mtime_ns, inode)` 三元组——任何真实编辑几乎必然改变 mtime——**哈希的职责是识别 Move/重命名后的内容等同性与 mtime 不可靠场景**（网络盘、某些拷贝工具），而非唯一防线。imohash 不是密码学哈希，禁止用于任何安全用途。

关于 SigLIP：模型跑在 compute-node（或单机版的本地实现）上，index-node 只经 gRPC 收发。向量维度不硬编码（SigLIP base 为 768 维，SoViT-400m 为 1152 维），以 `EmbedResponse.model_info` 为准；**向量必须 L2 归一化后存储**，相似度用内积（等价于余弦）。`model_version` 随向量存储，模型升级时触发按版本号重投（§6.11）。

---

## 4. 目录结构（骨架）

```
index-node/
├── cmd/
│   └── indexnode/main.go            # 入口：加载配置 → lifecycle.Run()
├── internal/
│   ├── config/
│   │   ├── config.go                # 结构体、Load、校验、默认值
│   │   └── config_test.go
│   ├── lifecycle/
│   │   └── lifecycle.go             # 组件装配、启动顺序、优雅关闭、崩溃恢复入口
│   ├── watch/
│   │   ├── manager.go               # 监听根生命周期：开/关/重开(退避)/标脏
│   │   └── consumer.go              # 每根一个零工作量消费 goroutine
│   ├── debounce/
│   │   ├── debounce.go              # 单 goroutine 状态机：合并规则 + settle 窗口
│   │   └── debounce_test.go         # 表驱动测试（§11 必测项）
│   ├── store/
│   │   ├── store.go                 # DB 打开(WAL/busy_timeout/foreign_keys)、迁移、写事务串行化助手
│   │   ├── migrations/*.sql         # 编号迁移脚本，embed 进二进制
│   │   ├── catalog.go               # files 表操作
│   │   ├── taskqueue.go             # tasks 表：入队(去重)、认领、状态转移
│   │   ├── notes.go                 # notes 表
│   │   ├── deadletter.go            # dead_letters 表
│   │   └── vectors.go               # vectors 表（向量真值）
│   ├── scheduler/
│   │   └── scheduler.go             # 认领循环、in-flight 路径集、优先级、停泊/释放
│   ├── pipeline/
│   │   ├── pipeline.go              # 阶段装配、有界 channel、池尺寸
│   │   ├── task.go                  # 任务上下文结构（贯穿各阶段）
│   │   ├── iostage/io.go            # stat、settle 复检、imohash、字节信号量
│   │   ├── extract/
│   │   │   ├── extractor.go         # Extractor 接口 + 按扩展名/嗅探注册表
│   │   │   ├── plaintext.go         # txt/md/代码文件（含编码检测）
│   │   │   ├── pdf.go
│   │   │   ├── office.go            # docx/xlsx（pptx 后续）
│   │   │   └── subprocess.go        # 高危解析器的子进程隔离运行器
│   │   ├── media/
│   │   │   ├── image.go             # 图片预处理（缩放到 SigLIP 输入尺寸、格式归一）
│   │   │   └── video.go             # ffmpeg 抽帧：均匀 N 帧 + 便签锚定时间戳帧
│   │   └── embed/
│   │       ├── client.go            # Embedder 接口 + compute-node gRPC 实现
│   │       ├── batcher.go           # 微批聚合（B 张或 T 毫秒）
│   │       └── breaker.go           # 熔断器 + waiting_dep 停泊/释放
│   ├── index/
│   │   ├── tantivy.go               # schema、单写者 goroutine、批量提交、按 term 删除
│   │   ├── vector.go                # HNSW 封装：增/查/墓碑删/快照/从 vectors 表重建
│   │   └── search.go                # 三路查询 + RRF 融合 + expire_at 过滤 + 降级
│   ├── notes/
│   │   └── service.go               # 便签业务：锚点校验、TTL、审计、触发抽帧任务
│   ├── reconcile/
│   │   └── scanner.go               # 启动全量 / 脏根重扫 / 周期巡检；与 catalog 做 diff
│   ├── errclass/
│   │   └── errclass.go              # 错误分类学：类别、Classify()、重试策略表
│   ├── server/
│   │   ├── grpc.go                  # gRPC server 装配、拦截器(日志/指标/恢复)
│   │   ├── search.go                # SearchService 实现
│   │   ├── admin.go                 # 监听根管理、死信查看/重投、状态
│   │   └── notes.go                 # 便签 CRUD 实现
│   └── obs/
│       ├── log.go                   # slog 装配、路径脱敏钩子
│       ├── metrics.go               # 全部指标定义（§6.13 清单）
│       └── audit.go                 # 追加式审计日志
├── api/proto/
│   ├── indexnode/v1/indexnode.proto # 本节点对外提供的服务
│   └── compute/v1/compute.proto     # 本节点消费的 compute-node 服务契约
├── gen/                             # protoc 生成代码（勿手改）
├── configs/indexnode.example.yaml
├── docs/adr/
├── Makefile                         # build / test / race / proto / lint
├── PROGRESS.md
└── go.mod
```

分层依赖规则（用 lint 或 review 保证）：`cmd → lifecycle → {server, scheduler, pipeline, reconcile, watch, debounce} → {store, index, errclass, obs} → config`。禁止反向依赖；第三方库 import 只出现在其指定封装文件。

---

## 5. 数据模型与存储

### 5.1 SQLite（单文件，WAL 模式）

打开参数：`_pragma=journal_mode(WAL)`、`busy_timeout=5000`、`foreign_keys=ON`、`synchronous=NORMAL`（**notes 相关写事务临时提升为 FULL**，见 P4）。写入纪律：全库共享一个写连接（`SetMaxOpenConns(1)` 的写 handle）+ 独立只读连接池；store 层暴露 `WithTx(func(tx) error)`，所有跨表状态转移必须在单个事务内完成（如"任务完成 + catalog 更新"）。

```sql
-- 迁移 0001：核心表
CREATE TABLE files (
  file_id       INTEGER PRIMARY KEY,          -- 稳定内部 ID，便签/向量的锚点
  path          TEXT NOT NULL UNIQUE,
  size          INTEGER NOT NULL,
  mtime_ns      INTEGER NOT NULL,
  inode         INTEGER,                      -- 平台可得时填写
  sample_hash   BLOB,                         -- imohash 128-bit
  kind          TEXT NOT NULL,                -- text | image | video | other
  generation    INTEGER NOT NULL,             -- 单调递增，见 P3
  status        TEXT NOT NULL,                -- indexed | pending | failed | deleted
  extractor_version   TEXT,
  embed_model_version TEXT,
  indexed_at    INTEGER
);
CREATE INDEX idx_files_status ON files(status);

CREATE TABLE tasks (
  task_id        INTEGER PRIMARY KEY,
  file_id        INTEGER,                     -- 可空（新文件尚未入 catalog）
  path           TEXT NOT NULL,
  op             TEXT NOT NULL,               -- upsert | remove | relocate
  old_path       TEXT,                        -- relocate 专用
  generation     INTEGER NOT NULL,
  state          TEXT NOT NULL,               -- pending|in_flight|retry_wait|waiting_dep|done|dead
  priority       INTEGER NOT NULL DEFAULT 5,  -- 0 最高；用户触发=0，事件=5，扫描=8
  attempts       INTEGER NOT NULL DEFAULT 0,
  crash_count    INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0, -- unix ms；退避写这里
  last_error     TEXT,
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL,
  UNIQUE(path, state) ON CONFLICT IGNORE      -- 简化的待处理去重：同路径同态只留一条
);
CREATE INDEX idx_tasks_claim ON tasks(state, priority, next_attempt_at);

CREATE TABLE notes (
  note_id      INTEGER PRIMARY KEY,
  file_id      INTEGER NOT NULL REFERENCES files(file_id),
  anchor_type  TEXT NOT NULL,                 -- file | line | timestamp
  anchor_line  INTEGER,                       -- anchor_type=line
  anchor_ts_ms INTEGER,                       -- anchor_type=timestamp（视频）
  content      TEXT NOT NULL,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  expire_at    INTEGER                        -- NULL = 永久
);
CREATE INDEX idx_notes_file ON notes(file_id);
CREATE INDEX idx_notes_expire ON notes(expire_at) WHERE expire_at IS NOT NULL;

CREATE TABLE dead_letters (
  file_id      INTEGER PRIMARY KEY,           -- 同文件去重更新，不堆积
  path         TEXT NOT NULL,
  generation   INTEGER NOT NULL,
  stage        TEXT NOT NULL,                 -- io | extract | media | embed | commit
  error_class  TEXT NOT NULL,
  error_chain  TEXT NOT NULL,                 -- JSON：最近 N 次错误链
  attempts_log TEXT NOT NULL,                 -- JSON：各次尝试时间戳
  extractor_version   TEXT,
  embed_model_version TEXT,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
);

CREATE TABLE vectors (                        -- 向量的事实源（P4）
  file_id       INTEGER NOT NULL REFERENCES files(file_id),
  frame_idx     INTEGER NOT NULL DEFAULT 0,   -- 图片=0；视频=帧序号
  frame_ts_ms   INTEGER,                      -- 视频帧对应时间戳
  dims          INTEGER NOT NULL,
  vector        BLOB NOT NULL,                -- float32 小端序列化，已 L2 归一化
  model_version TEXT NOT NULL,
  PRIMARY KEY (file_id, frame_idx)
);

CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT); -- schema 版本、上次干净关闭标记等
```

`meta` 表必须包含 `clean_shutdown` 标记：正常关闭最后写 `true`，启动时读到非 `true` 即判定为崩溃后启动（毒丸检测依赖它，§6.11）。

### 5.2 tantivy schema（单 index + doc_type 区分两路）

| 字段 | 类型 | 属性 | 说明 |
|---|---|---|---|
| doc_type | text(raw) | indexed, fast | `file` \| `note` |
| file_id | i64 | indexed, fast, stored | 删除/更新的定位键 |
| path | text(raw) | indexed, stored | 精确匹配与前缀过滤 |
| path_text | text(tokenized) | indexed | 路径分词可搜 |
| filename | text(tokenized) | indexed, stored | 查询时加权 boost |
| kind | text(raw) | indexed, fast | 过滤 |
| content | text(tokenized) | indexed, **stored** | 文件正文或便签内容。stored 是 Move 快路径与摘要高亮的前提 |
| mtime | i64 | fast | 时间过滤/排序 |
| note_id | i64 | indexed, stored | 仅 note 文档 |
| anchor_type / anchor_line / anchor_ts_ms | — | stored | 仅 note 文档 |
| expire_at | i64 | indexed, fast | 仅 note 文档；**永久便签写 i64::MAX 哨兵值**，统一走 range filter |
| generation | i64 | stored | 调试用 |

删除语义：按 `file_id`（file 文档）或 `note_id`（note 文档）做 term delete。tantivy 无原地更新：更新 = delete + add。CJK 分词是已知开放点（§14）。

### 5.3 向量索引布局

HNSW 图常驻内存，键为 `(file_id << 16) | frame_idx` 打包成 uint64（限制：单文件帧数 < 65536，够用）。持久化策略：vectors 表是真值；HNSW 每 10 分钟或每 5000 次变更用库的 Export 写快照到 `<data_dir>/vector.snapshot.tmp` 再原子 rename；启动时优先 Import 快照，再用 vectors 表补齐快照之后的增量（快照文件头记录截至的最大 rowid/时间戳）；快照损坏或缺失则全量从 vectors 表重建。删除用内存墓碑集合 + 查询后过滤，墓碑占比超过 20% 触发后台全量重建换新图。

---

## 6. 组件规格

### 6.1 watch —— filecat 封装与监听根生命周期

`Manager` 持有 `map[rootPath]*rootWatch`。每根：`filecat.NewWatcher(path, recursive=true, bufferSize=4096, coalesceWindow=50ms)` + 一个消费 goroutine。消费 goroutine 的完整职责只有一件事：把 `FileEvent` 转成 `debounce.RawChange{Op, Path, OldPath, At}` 并**非阻塞**投入 debounce 入口 channel；`select` 的 default 分支命中（入口满）时，调用 `Manager.MarkDirty(root)` 并丢弃该事件。`Errors()` 收到 `ErrOverflow` 同样 `MarkDirty`。MarkDirty 是幂等的：向 reconcile 提交一个该根的重扫请求（去重）。监听根打开失败或运行中死亡（目录被卸载/删除）：按指数退避（5s 起，封顶 5min）重试重开，期间该根状态为 `degraded`，通过健康接口上报；其他根不受影响。配置变更（增删根）经 admin gRPC 进入 Manager，删除根时同步生成"该根前缀下所有 catalog 条目 → remove 任务"（可配置为保留索引只停监听）。

### 6.2 debounce —— 业务级去抖聚合

单 goroutine 状态机。数据结构：`map[path]*pending` + 按到期时间的最小堆。窗口 `settle_window`（默认 1s，可配）。合并规则（表驱动实现 + 表驱动测试）：

| 已挂起 | 新事件 | 结果 |
|---|---|---|
| Created | Modified | Created（窗口重置） |
| Created | Removed | 取消（noop） |
| Modified | Modified | Modified（窗口重置） |
| Modified | Removed | Removed |
| Removed | Created | Modified（覆盖写场景） |
| Move(A→B) | Move(B→C) | Move(A→C) |
| Move(A→B) | Removed(B) | Removed(A) 与 Removed(B) 二合一：产出 remove(A) |
| 任意 | Move 以其为源 | 折叠进 Move 链 |

窗口到期后产出规范化任务并写入 store.taskqueue：`Created/Modified → op=upsert`、`Removed → op=remove`、`Move → op=relocate(old_path)`。**目录事件**：Move 若 Path 是目录（stat 判断，已消失则查 catalog 前缀），产出一个目录级 relocate 任务，由 worker 按 catalog 前缀展开批量改写（§6.6）；目录 Removed 同理展开为批量 remove。入队在一个写事务内完成"任务插入 + files.generation 预递增（若已有条目）"。

### 6.3 store —— 事务纪律

除 §5.1 所述，补充：`taskqueue.Claim(n)` 用单条 SQL 完成认领：`UPDATE tasks SET state='in_flight', attempts=attempts+1, updated_at=? WHERE task_id IN (SELECT task_id FROM tasks WHERE state IN ('pending') AND next_attempt_at<=? ORDER BY priority, task_id LIMIT ?) RETURNING *`。认领即落盘（毒丸证据，P2/§6.11）。所有状态转移函数（`MarkDone/MarkRetry/MarkDead/MarkWaitingDep/ReleaseWaitingDep`）都是显式方法，禁止业务代码手写 UPDATE。

### 6.4 scheduler —— 认领、串行化、优先级

单 goroutine 主循环：`Claim(batch)` → 对每个任务检查内存 in-flight 路径集（`map[path]struct{}`，目录 relocate 以前缀参与冲突判断）→ 冲突的任务立即 `MarkRetry(next_attempt_at=now+200ms)` 放回（简单且正确；不做内存停泊队列，避免与持久队列出现两套状态）→ 无冲突的按 `op` 与 `files.kind` 路由到对应阶段入口 channel（有界，满则本次不再 Claim，天然背压）。worker 完成（任何终态）后回调 scheduler 从 in-flight 集移除。循环空转时用条件变量/唤醒 channel 等待（入队方唤醒），避免忙轮询；同时每 500ms 定时唤醒以拾取到期的 retry_wait 任务。

### 6.5 pipeline —— 各阶段

**任务上下文** `pipeline.Task`：携带 task 行、catalog 快照、generation、以及阶段间中间产物（抽取文本 / 帧图像 / 向量）。上下文只在阶段间单向传递，不共享。

**iostage**（IO 池）：
1. `os.Lstat`。文件不存在且 op=upsert → 转为 remove 语义继续。
2. settle 复检：stat 两次间隔 200ms，size/mtime 仍在变则 `MarkRetry`（瞬时，退避 2s 起）——文件仍在写入。
3. 与 catalog 比对 `(size, mtime_ns, inode)`，全同则直接 `MarkDone`（幂等短路）。不同则计算 imohash；哈希也同（仅 mtime 变）则只更新 catalog 元数据 + tantivy mtime 字段，不重抽取。
4. 打开文件读取（文本类流式读入抽取器；受**双信号量**约束：任务数信号量 = `io_concurrency`，字节数信号量 = `io_bytes_inflight` 默认 256MB；单文件超过 `max_file_size`（默认 512MB）直接判永久错误进死信但保留文件名索引）。
5. `op=relocate`：若 stat 得到的三元组+哈希与 catalog 旧条目一致 → **Move 快路径**：单事务更新 files.path、投递 tantivy "path 字段更新"（delete + 用 stored 字段重建 add），不重抽取不重 embedding；目录级 relocate 在此展开为对 catalog 前缀的批量路径改写 + 批量 tantivy 更新。不一致则降级为 upsert 全流程。

**extract**（CPU 池）：`Extractor` 接口：

```go
type Extractor interface {
    Match(path string, sniff []byte) bool     // 扩展名 + 内容嗅探
    Extract(ctx context.Context, r io.Reader, meta FileMeta) (Doc, error)
}
```

注册表按顺序匹配，兜底 plaintext（带 UTF-8/GBK 等编码检测，二进制嗅探判 other 不抽取）。**隔离策略**：纯 Go 解析器进程内跑但外层 `recover()`（panic 归类为永久错误）；依赖 CGO 或历史上不稳定的解析器通过 `subprocess.go` 以自身二进制 re-exec 一个受限子命令运行（`indexnode extract-worker --stdin`），超时与退出码由父进程归类。抽取产物做长度上限截断（默认 2MB 文本，可配），并记录截断标记。

**media**（CPU 池，含 ffmpeg 子进程信号量 = `NumCPU/2`）：图片 → 解码、按 SigLIP 输入要求缩放/中心裁剪、编码为 JPEG bytes。视频 → `ffprobe` 取时长 → 均匀取 N 帧（默认 5，跳过首尾各 5%）+ 查询 notes 表中该 file_id 的全部 `anchor_ts_ms` 追加为帧 → 每帧同图片预处理。帧集合与 frame_idx/frame_ts_ms 的映射进入上下文。

**embed**（RPC 池）：`Embedder` 接口（`EmbedImages(ctx, [][]byte) ([][]float32, ModelInfo, error)`、`EmbedText(ctx, string)`）。实现为 compute-node gRPC 客户端。微批：batcher 攒 `batch_size`（默认 32）张或 `batch_linger`（默认 100ms）先到者发一批；调用方以 future/channel 等待自己那几张的结果。并发控制用 in-flight 请求信号量（默认 8 批）。熔断器：连续失败阈值触发 open，任务 `MarkWaitingDep`（**不消耗 attempts**）；half-open 探测成功后 `ReleaseWaitingDep` 批量放回 pending。返回向量 L2 归一化、连同 model_version 写 vectors 表（本阶段即写，属中间产物真值），再进提交阶段。

### 6.6 index/tantivy —— 单写者与批量提交

专职 goroutine 独占 IndexWriter，输入为有界 channel 的 `CommitOp`（AddFile / AddNote / DeleteByFileID / DeleteByNoteID / UpdatePath 批量）。攒批提交：满 1000 op 或 3s 先到者 commit。commit 成功后**在一个 SQLite 事务内**执行本批全部任务的 `MarkDone` + files 状态/时间戳更新，随后回调 scheduler 释放路径。commit 失败按瞬时错误整批重试（op 幂等：delete+add）。generation 防线：每个 CommitOp 携带 generation，写者在 add 前查 catalog 当前值（批量预取），过期 op 丢弃并记数。图片/视频的 file 文档 content 字段为空或仅放文件名衍生文本——它们的可搜性由向量与便签承担；但 filename/path 字段照常入索引（死信文件同样保留 filename 索引 + `status=failed` 标记，见前轮设计）。

### 6.7 index/vector —— HNSW 封装

接口：`Add(key uint64, vec []float32)`、`MarkDeleted(key)`、`Search(vec, topK) []Hit`、`SnapshotTo(path)`、`RebuildFromStore(ctx)`。参数：M=16、efConstruction=200、efSearch=64（均可配）。写入路径：embed 阶段写完 vectors 表后向 vector 索引的串行更新 goroutine（单写者，与 tantivy 写者对等）投递 Add；remove/relocate→remove 时投递 MarkDeleted 全部 frame。Search 在读锁下并发安全（若库不保证，封装层加 RWMutex，Add 走写锁——单写者模式下竞争极小）。墓碑>20% 或 model_version 出现混杂时后台重建：新图在旁路构建完成后原子换指针。

### 6.8 index/search —— 三路融合

`Search(req)` 流程：
1. 解析 req（mode: keyword | semantic | hybrid，默认 hybrid；filters: path_prefix / kinds / mtime range；top_k）。
2. 并发发起：(a) tantivy file 文档 BM25（filename 加权 boost 2.0）；(b) tantivy note 文档 BM25，**强制附加 `expire_at > now_ms` range filter**；(c) semantic/hybrid 时经 Embedder.EmbedText 得查询向量 → HNSW Search。compute-node 不可达时 (c) 降级跳过，响应标记 `degraded_semantic=true`——文本与便签检索永不因 AI 依赖不可用而失败。
3. RRF 融合：`score(d) = Σ_lists 1/(60 + rank_i(d))`，按 file_id 去重聚合（同文件多帧命中取最优帧并附 frame_ts_ms；便签命中附 note 摘要与锚点）。返回项包含**命中来源标注**（content/note/semantic）供前端展示。
4. 结果元数据从 catalog 批量补齐（size/mtime/kind/status）。

### 6.9 notes —— 便签服务

CRUD 全部落 SQLite（P4 保护等级）+ 同步投递 tantivy note 文档 add/delete。锚点校验：line 仅 text 类、timestamp 仅 video 类且不超时长。**创建 timestamp 便签时**，若该 file_id 的 vectors 表无对应帧，入队一个 priority=0 的 upsert 任务（media 阶段会拾取新锚点帧）。TTL：查询时 range filter 保证过期即刻不可见（精确）；后台 reaper 每小时扫 `expire_at < now` 的行，物理删除 SQLite 行 + tantivy 文档，全程写审计。文件删除时按配置 `notes.on_file_delete: orphan|cascade`（默认 orphan：保留便签，标记 file 已删，仍可被搜到并提示原文件不存在）。

### 6.10 reconcile —— 对账扫描器

三种触发：启动全量（必跑）、MarkDirty 的脏根重扫（去重合并）、周期巡检（默认 24h，低优先级）。算法：并发遍历监听根（每根一个遍历 goroutine，内部小池并发 stat），流式与 catalog 比对——磁盘有 catalog 无 → upsert 任务；两者有但三元组不同 → upsert；catalog 有磁盘无 → remove。全部产出 priority=8 任务走统一队列。**事件路径与扫描路径在队列汇合，之后世界完全统一**。扫描每轮统计 diff 数上报指标（事件路径健康度，正常应≈0）。启动全量扫描完成前节点健康状态为 `warming`。

### 6.11 errclass + 重试 + 死信

错误四类（`errclass.Class`）：`Transient`（compute 超时/不可达、文件被占用、瞬时 IO、tantivy 提交 IO 失败）、`Permanent`（损坏、不支持、解析确定性失败、超大小上限、权限不足）、`Poison`（隐式：崩进程者）、`Fatal`（磁盘满、DB/索引损坏——不是任务错误，触发全局暂停，§6.14）。`Classify(err)` 基于哨兵错误与 `errors.As` 判别，未知默认 Transient（保守）。

状态机（与前期设计一致）：`pending → in_flight → done | retry_wait | waiting_dep | dead`。退避：`next_attempt_at = now + min(5s × 2^attempts, 30min) × jitter(0.8~1.2)`。上限：Transient 8 次、Permanent 0 次。全局重试预算：令牌桶限制 retry 来源的认领不超过总认领量 20%。**毒丸检测**：启动时若 `meta.clean_shutdown != true`，将所有 `state=in_flight` 的任务 `crash_count+1`；`crash_count >= 2` 直接 `MarkDead(stage=unknown, class=poison)`，否则放回 pending。死信语义遵循前期设计：同 file_id 去重更新；文件后续变更以更高 generation 正常处理成功后自动清除死信；catalog 标 `failed` 但 filename 仍可搜。重投三径：admin gRPC 手动（单条/按类批量）、启动时 extractor_version / embed_model_version 与死信记录不符的自动重投、文件变更顶替。保留期默认 90 天，逾期归档进审计日志后删除。

### 6.12 server —— gRPC 控制面

服务定义见 §8。拦截器链：panic recovery（转 Internal 并记录）→ 请求日志（含 trace id）→ 指标。查询接口与索引流水线共享进程但天然隔离（tantivy reader 基于 segment 快照不被写者阻塞；HNSW 读锁）。认证鉴权是 master 的职责，本节点只做可配置的 mTLS/token 校验开关（默认关，单机版关闭）。

### 6.13 obs —— 日志、指标、审计

**日志**：slog JSON；任务全生命周期携带 `task_id/file_id/generation` 字段（经 context 传递）；只打状态转移点。隐私双轨：本地文件全量（lumberjack 轮转 7 天）；`log.redact_paths=true` 时所有出程序边界（gRPC 上报、远程遥测）的路径做哈希脱敏，仅保留扩展名。
**指标**（Prometheus，全量清单）：`tasks_backlog{state}`、`task_oldest_pending_age_seconds`（核心 SLO）、`stage_duration_seconds{stage}` 直方图、`stage_throughput_total{stage}`、`retries_total{class}`、`dead_letters_total` 与 `dead_letters_size`、`reconcile_diff_total{root}`（事件路径健康度）、`watch_overflow_total`、`breaker_state{dep}`、`pool_inflight{pool}`、`tantivy_commit_duration_seconds`、`vector_index_size` / `vector_tombstone_ratio`、`search_duration_seconds{mode}`、`notes_expired_reaped_total`。
**审计**（追加式 JSONL，独立文件，长保留）：监听根增删、便签 create/update/delete/expire-reap、死信产生与重投（含操作来源）、索引重建、配置变更。

### 6.14 lifecycle —— 启动、关闭、全局故障

**启动序**：config → store(迁移、崩溃检测、毒丸处理) → tantivy/vector 打开（损坏则进入重建流程：tantivy 从 catalog+重索引任务重建；vector 从 vectors 表重建）→ 提交层与池 → scheduler → debounce → watch → reconcile 启动扫描 → gRPC 最后监听。**关闭序**（收到信号后，整体超时默认 30s）：gRPC 停收新请求 → 关 watchers → debounce flush 残留入队 → scheduler 停止认领 → 等池 drain（超时任务留队列，天然由 at-least-once 兜底）→ tantivy 最终 commit → vector 快照 → `meta.clean_shutdown=true` → 关 DB。**Fatal 处理**：磁盘可用低于 `min_free_bytes`（默认 2GB，tantivy 段合并需余量）→ scheduler 暂停认领、健康状态 degraded、指标+日志告警，空间恢复自动续跑；DB/索引损坏按 P4 层级重建，重建期间对外 `warming`。

---

## 7. 并发模型总规格（实现时对照检查）

**刻意单线程的组件**（禁止并行化）：每监听根 1 个消费 goroutine、debounce 状态机 ×1、scheduler 主循环 ×1、tantivy 写者 ×1、vector 索引写者 ×1、SQLite 写连接 ×1。

**工作池**（golang.org/x/sync/semaphore 实现，goroutine 由 errgroup 管理）：

| 池 | 默认并发度 | 约束资源 | 附加约束 |
|---|---|---|---|
| IO | `min(4×NumCPU, 32)`，HDD 配置建议 2 | 磁盘 | 字节信号量 256MB in-flight |
| CPU | `NumCPU − 1`，且受 `cpu_percent_cap`（默认 50%）折算 | CPU | ffmpeg 子进程独立信号量 `NumCPU/2`，占用计入本池 |
| RPC(embed) | goroutine 不限，in-flight 批数信号量 8 | compute 吞吐 | 微批 32 张 / 100ms；熔断见 §6.5 |

**顺序性**：路径级串行（scheduler in-flight 集，目录前缀参与冲突）+ generation 提交防线（tantivy 写者核对）。
**背压链**：tantivy 写者入口 channel(容量 2048) 满 → 池阻塞 → 池入口 channel(每池 256) 满 → scheduler 停止 Claim → 洪峰沉淀在 SQLite 队列。watch→debounce 一跳不允许阻塞：满则标脏丢弃，扫描兜底。
**验证要求**：`go test -race` 全绿；提供一个 `-tags stress` 的压力测试（§11）。

---

## 8. gRPC 接口（proto 骨架，字段可增不可改语义）

```protobuf
// api/proto/indexnode/v1/indexnode.proto
service SearchService {
  rpc Search(SearchRequest) returns (SearchResponse);
}
message SearchRequest {
  string query = 1;
  enum Mode { HYBRID = 0; KEYWORD = 1; SEMANTIC = 2; }
  Mode mode = 2;
  uint32 top_k = 3;                  // 默认 20
  Filters filters = 4;
}
message Filters { string path_prefix = 1; repeated string kinds = 2; int64 mtime_from = 3; int64 mtime_to = 4; }
message SearchResponse { repeated Hit hits = 1; bool degraded_semantic = 2; }
message Hit {
  int64 file_id = 1; string path = 2; string kind = 3;
  double score = 4;
  repeated string sources = 5;       // content | note | semantic
  string snippet = 6;                // 命中摘要
  NoteRef note = 7;                  // 便签命中时
  int64 frame_ts_ms = 8;             // 语义命中视频帧时
  string status = 9;                 // indexed | failed(内容索引失败但可按名搜到)
}

service NotesService {
  rpc CreateNote(CreateNoteRequest) returns (Note);
  rpc UpdateNote(UpdateNoteRequest) returns (Note);
  rpc DeleteNote(DeleteNoteRequest) returns (google.protobuf.Empty);
  rpc ListNotes(ListNotesRequest) returns (ListNotesResponse);   // 按 file 或 path
}
message Note {
  int64 note_id = 1; int64 file_id = 2;
  enum Anchor { FILE = 0; LINE = 1; TIMESTAMP = 2; }
  Anchor anchor = 3; int64 anchor_line = 4; int64 anchor_ts_ms = 5;
  string content = 6; int64 expire_at = 7;    // 0 = 永久
}

service AdminService {
  rpc AddWatchRoot(WatchRootRequest) returns (google.protobuf.Empty);
  rpc RemoveWatchRoot(RemoveWatchRootRequest) returns (google.protobuf.Empty); // keep_index 选项
  rpc GetStatus(google.protobuf.Empty) returns (NodeStatus);   // warming|ready|degraded、积压年龄、脏根、熔断态
  rpc ListDeadLetters(ListDeadLettersRequest) returns (ListDeadLettersResponse);
  rpc RedriveDeadLetters(RedriveRequest) returns (RedriveResponse);  // 按 file_id 列表或按 error_class 批量
  rpc TriggerRescan(RescanRequest) returns (google.protobuf.Empty);
  rpc ReindexAll(google.protobuf.Empty) returns (google.protobuf.Empty);  // P4 重建入口
}

// api/proto/compute/v1/compute.proto —— 本节点是客户端
service EmbedService {
  rpc EmbedImages(EmbedImagesRequest) returns (EmbedImagesResponse);  // batch jpeg bytes
  rpc EmbedText(EmbedTextRequest) returns (EmbedTextResponse);
}
message ModelInfo { string model_version = 1; uint32 dims = 2; }
```

单机版 wiring：`EmbedService` 换成进程内实现或 localhost；`SearchService` 可被同进程 master 直接函数调用（保留接口即可）。

---

## 9. 配置规范（configs/indexnode.example.yaml）

```yaml
node_id: ""                      # 空则首次启动生成并持久化
data_dir: /var/lib/indexnode    # SQLite / tantivy / vector 快照 / 日志
grpc_listen: 0.0.0.0:7701
metrics_listen: 127.0.0.1:7702

watch:
  roots:
    - path: /home/user/docs
      recursive: true
  buffer_size: 4096
  settle_window: 1s

compute:
  endpoint: dns:///compute:7801   # 单机版指向 localhost 或进程内
  batch_size: 32
  batch_linger: 100ms
  inflight_batches: 8
  breaker: { failures: 5, open_for: 30s }

pipeline:
  io_concurrency: 0               # 0 = 自动 min(4×CPU,32)
  io_bytes_inflight: 268435456
  cpu_percent_cap: 50
  max_file_size: 536870912
  max_extract_bytes: 2097152
  video_frames: 5
  ffmpeg_path: ffmpeg

index:
  commit_max_ops: 1000
  commit_interval: 3s
  vector: { m: 16, ef_construction: 200, ef_search: 64, snapshot_interval: 10m }

retry: { base: 5s, cap: 30m, max_attempts_transient: 8, retry_budget_ratio: 0.2 }
dead_letter: { retention_days: 90 }
reconcile: { periodic: 24h }
notes: { on_file_delete: orphan }
resources: { min_free_bytes: 2147483648 }
log: { level: info, redact_paths: false, retain_days: 7 }
```

所有键支持 `INDEXNODE_` 前缀环境变量覆盖（点路径转下划线大写）。config.Load 做强校验，非法配置直接拒绝启动并输出具体字段。

---

## 10. 里程碑与验收标准

- **M0 骨架**：目录、go.mod、config、store（迁移 + 全部表 + WithTx + Claim/状态转移）、obs 基础、Makefile。验收：`make test race` 绿；store 单测覆盖认领竞争（并发 Claim 无双发）与崩溃标记。
- **M1 文本主干**：errclass、scheduler、iostage、plaintext extractor、tantivy 封装与单写者、手工入队 CLI（临时）。验收：对临时目录 1000 个 txt 手工入队后全部可被 keyword 搜到；kill -9 后重启无丢失（§11 崩溃测试）；同文件重复入队被幂等短路。
- **M2 事件接入**：watch + debounce。验收：合并规则表驱动测试全过；对目录做创建/写风暴/移动/删除脚本化操作，最终索引与磁盘一致；Move 快路径不触发重抽取（以指标断言）。
- **M3 对账**：reconcile 三触发。验收：停进程期间增删改文件，启动扫描后收敛；人为塞满 watch 入口触发标脏后收敛；diff 指标正确。
- **M4 可靠性**：完整重试/退避/预算、waiting_dep、毒丸、死信 + 重投（含版本触发）。验收：注入 panic 的假 extractor 走完毒丸路径；模拟 compute 宕机 attempts 不增长且恢复后自动续跑；死信重投 e2e。
- **M5 图片语义**：media/image、embed 客户端（含微批/熔断）、vectors 表、HNSW、semantic 与 hybrid 搜索、RRF。验收：以 mock EmbedService 提供确定性向量，语义检索命中正确；compute 关闭时 hybrid 降级仅标记不失败；重启后向量索引从快照/表恢复一致。
- **M6 视频**：ffprobe/ffmpeg 抽帧、多帧向量、结果带 frame_ts_ms。验收：对样例视频索引后语义命中返回正确时间戳；ffmpeg 缺失时视频管线降级禁用且文本管线不受影响。
- **M7 便签**：NotesService 全量、TTL 查询过滤 + reaper、timestamp 便签触发抽帧、orphan 语义、审计。验收：过期便签在过期瞬间即不可搜（时钟注入测试）；新增时间戳便签后该帧可被语义搜到。
- **M8 控制面完善**：Admin 全接口、NodeStatus、ReindexAll（P4 重建）、mTLS 开关。验收：删除 tantivy 目录后经 ReindexAll 完全恢复；删除向量快照后自动重建。
- **M9 观测与打磨**：全指标清单落地、脱敏开关、压力/浸泡测试、PROGRESS 收尾、示例配置与 README。

## 11. 测试与质量要求

`go test -race ./...` 是每次提交的门槛。必备专项：debounce 合并规则表驱动；**崩溃恢复测试**——测试进程 re-exec 子进程跑流水线并在随机阶段 `kill -9`，断言重启后收敛且无丢失（利用 clean_shutdown 路径）；毒丸循环打破测试；并发 Claim 唯一性；generation 陈旧提交被拒；Move 快路径零抽取断言（指标）；`-tags stress`：5 万小文件 + 持续写风暴 30s，断言内存峰值有界（读取 runtime.MemStats）且最终一致。基准：`BenchmarkTextPipeline` 报告 files/sec。覆盖率不设硬指标，但 store/debounce/errclass/scheduler 四包需≥80%。

## 12. 编码规范

context 作为首参贯穿全部阻塞调用；错误一律 `fmt.Errorf("...: %w", err)` 包装并在边界 Classify；禁止 `panic` 跨包边界（worker 顶层 recover）；禁止包级可变全局状态；接口定义在消费方包；所有时间用 `time.Time`/单调纳秒，落库统一 unix ms 或 ns 并在字段名标注；禁止在热路径 fmt.Sprintf 拼日志（用 slog 字段）。依赖以 go.mod 精确固定版本。

## 13. 非目标

master-node 与 compute-node 的实现；权限/认证体系（仅留 mTLS/token 开关）；任何 UI；OCR 与音频转写（接口层为其留出 stage 扩展点即可）；跨节点索引副本/迁移；pptx 抽取（M 系列之后）。

## 14. 开放决策点（实现时确认并记 ADR）

1. **CJK 分词**：tantivy 默认分词器对中文近乎不可用。确认 tantivy-go 暴露的 tokenizer 能力：优先 jieba/lindera 类，其次 ngram(2) 兜底。这是 M1 期间必须解决的第一个 ADR。
2. tantivy content 字段 stored 的空间成本 vs 移到 SQLite sidecar 表——默认 stored，若实测索引膨胀不可接受再迁移。
3. `coder/hnsw` 的删除与并发读能力实测结论，及是否需要换 usearch。
4. 目录级 relocate 在超大目录（>10 万子项）下的分批策略与事务粒度。
5. inode 在 Windows 上的替代（NTFS FileID）与 imohash 参数是否需要 NewCustom 调整。
6. 单机版打包时 Embedder 进程内实现的接入点验证（保证零改动 wiring）。

> 交付定义：M0–M9 全部验收通过、`PROGRESS.md` 与全部 ADR 齐备、示例配置可直接启动一个可用节点。开始工作，从 M0 起步。