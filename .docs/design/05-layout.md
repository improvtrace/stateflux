# stateflux 设计 · 模块划分（§8）

> v3.20（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 8. 模块划分（v3.10 收编；v3.12 domain/repository；v3.15 内核并入 domain；v3.16 契约收编 api/ + 两运行时；v3.17 controller 更名 runtime；v3.18 账本与控制 fence；v3.19 eventbus/channel 与 Redis 定位收敛；v3.20 调度约束字段 vpc/node、operator/group 业务字段、0–100 优先级区间、数值 outcome 枚举与表/列注释）

```
stateflux/
├── api/              # 全部 proto 契约（生成码与 proto 同目录，module 模式输出）
│   ├── stateflux/task/v1/  # 调度↔执行契约：ExecutorService（Execute + ResultStream）+ TaskMessage/ResultEvent 信封（生成码仅模块内 import）
│   └── cluster/v1/         # 外置 cluster 访问契约：ClusterService（外部实现、stateflux 消费）
├── cmd/stateflux/    # 服务入口：flag/env → config → obs 装配 → internal/server 编排启动
│                     # （工具链命令如实时指标查询，按需在 cmd/ 扩展）
├── internal/         # 引擎收编（Go internal 可见性：外部模块禁止 import；无顶层公开引擎包）
│   ├── server/       # 编排层：biz + scheduler 运行时 + worker 运行时的装配与启停 + Handler 注册点（§1.2.8）
│   ├── biz/          # task/v1 服务端业务：Execute 执行编排 / Collect 结果上报（经 server 装配注入 worker）
│   ├── runtime/      # 控制面（调度侧）运行时归组：随选举启停，仅调度节点运行
│   │   ├── scheduler/  # fenced 晋升/原子名额 claim → EventBus channel 分发
│   │   ├── collector/  # 订阅 ResultEvent → fenced 终态事务/回调派生
│   │   └── reconcile/  # R1~R5（通道丢失、Redis 灾难、名额修复）
│   ├── worker/       # 订阅任务/执行/结果 WAL/发布 ResultEvent + task/v1 gRPC adapter
│   ├── task/         # 任务构建与分发：factory + dispatch（子包相互独立、零依赖）
│   │   ├── factory/  #   TaskFactory 周期任务生成（仅调度节点运行）
│   │   └── dispatch/ #   调度侧分发器：按任务 channel 选 eventbus/channel 实现、选执行节点、
│   │   │             #   ResultSink（RPC 响应 → ResultEvent 统一入口，§5.5）
│   ├── domain/       # 领域层 + 引擎共享内核（v3.15 按 model 划分文件）：
│   │   │             #   task.go（Task/channel/TaskRepository/CreatedTask/PromoteOptions）
│   │   │             #   callback.go（CallbackSpec）、result.go（Outcome/TaskResult/ResultRepository/
│   │   │             #   TerminalEntry/RequeueEntry）、ops.go（OpsRepository/DeadTask）、
│   │   │             #   store.go（组合根 Store）、identity.go/control.go（幂等账本/epoch lease）、errors.go、handler.go（Handler/Registry/Precondition）、
│   │   │             #   retry.go（Backoff/NonRetryable）、snowflake.go（任务 ID 生成）
│   │   ├── repository/ # 函数写入面/晋升/fenced claim/终态/对账/查询；ent 类型安全 API，
│   │   │             #     identity upsert + 条件名额预留承载幂等与并发，FOR UPDATE 承载 attempt
│   │   │             #     fencing；仅认领挪行 SKIP LOCKED 单条 SQL 经 ent 连接原生下沉，§3.1）
│   │   ├── data/     # 数据源基建：pg 连接池 + ent client、redis client（双栈在此被使用，不设显式
│   │   │             #     pg/redis 子包；ent 生成码由 make generate 产出，不入库）
│   │   ├── cacheview/ # 可选 Redis 观测/容量提示（非正确性路径）
│   │   ├── migration/ # 数据面迁移：enqueue 函数、权限、索引/触发器 + ent migrations + 表注释
│   │   │             #     （table_comments.sql：ent 不生成 COMMENT ON TABLE，§3.1）
│   │   └── schema/   # ent 表定义（四阶段 + payload/result + identity/reservation/control lease；生成码依赖 ent，
│   │   │             #     输出至 domain/data/ent，不入库）；列注释随 .Comment() 写进 DDL，枚举为数值编码
│   ├── eventbus/     # 逻辑 topic 与 Send/Subscribe（§3.2）；channel/ 是唯一节点通信抽象，
│   │   │             #   rpc/、redis/ 只是实现——任何实现都不得以 broker 持久性或 ack 提升可靠性
│   │   ├── channel/  # Send/Subscribe + 单向、半双工、全双工能力抽象 + in-memory fake（§12.1）
│   │   ├── rpc/      # unary Execute（半双工 request/reply）与 gRPC 双向 ResultStream（全双工）adapter
│   │   └── redis/    # list/zset/stream/pubsub 的 best-effort adapter
│   ├── cluster/      # ClusterView 外部接口 + RPC 客户端 + static 单机实现
│   ├── config/       # 配置结构与默认值补全（含 metrics/tracing 装配参数）
│   └── obs/          # metrics + tracing：OTel MeterProvider/TracerProvider/OTLP 装配（§6.4）
├── build/            # 打包与本地环境：服务 Dockerfile + docker-compose（PG/Redis 开发栈）
└── hack/             # 开发脚本：dev.sh（中间件启停 + demo 冒烟运行）
```

依赖方向：`cmd/stateflux → internal/server（编排 biz/runtime/worker）→ biz、控制面运行时
（runtime：scheduler/collector/reconcile）、执行侧运行时（worker）、任务运行时（task：factory/dispatch）
→ domain/cluster`；数据访问 `domain ← repository/cacheview ← data ← schema/migration`（接口 ←
聚合实现与缓存视图 ← 数据源基建 ← 表定义/迁移）由装配（server/cmd）注入，运行时包不 import
实现包。目录自上而下即架构分层：装配（cmd/server）→ 业务面（biz）与两运行时（runtime/worker）→
任务运行时（task）与领域（domain）→ 集群视图（cluster）；`domain` 为贯穿各层的共享内核
（模型 + 聚合接口 + 执行接入契约）。
biz/runtime/worker 相互零依赖——调度↔执行仅经 `api/stateflux/task/v1` 契约与 `eventbus/channel`
（RPC / gRPC stream / Redis 可替换的传输实现，dispatch 为调度侧分发编排）传递，全服务只有一条
PG 终态写路径（§5.5）。`domain` 为领域模型 +
纯接口层（§3.1 阶段集合抽象）：按聚合拆分为 TaskRepository（创建/晋升/
fenced 认领）、ResultRepository（终态事务/重置/查询）与 OpsRepository（控制 lease、对账扫描/死信运维/工厂判定），
组合根 Store 供装配整体注入；消费侧按需依赖窄聚合接口（promoter/scheduler → Task，
collector → Result，跨聚合的 reconcile/factory/biz → Store）。默认 ent + PG 实现独立成
`domain/repository` + `domain/data` + `domain/schema` + `domain/migration`（ent 生成码随 data），
可整体替换而不改变调度与执行语义。仓储读写一律走 ent 类型安全 API（ent 特性
`sql/upsert` / `sql/lock` / `sql/execquery`），仅两处例外经 ent 连接原生下沉：fenced claim（候选、
条件名额预留与搬迁必须同事务）与迁移引导 DDL（写入函数、权限、索引/触发器）。

**可见性规则（v3.10 修订的核心动机；v3.11 曾增补 sdk 例外，v3.15 收回）**：

- §1.2.8「引擎不作为三方库对外承诺 API、不支持嵌入业务进程」原先仅是约定——顶层平铺时全部
  引擎包都是 public import path，任何外部模块都能 import。收编 `internal/` 后由 Go internal
  规则**编译器强制**，边界不再依赖纪律。
- **v3.15 起无顶层公开的引擎包**：原 `sdk`（Task/TaskResult/Handler/CallbackSpec 契约类型）
  并入 `internal/domain`——业务 Handler 在服务构建时注册（同模块编译，公开与否不影响该方式）；
  §14 遗留的执行接入演进（HTTP 回调执行器 / Worker 拉取协议 / 独立执行 SDK）需要时再以
  独立模块形式重建依赖面。
- 对外可见的 Go 包为 `api/cluster/v1`（外部选举/成员服务实现方）——跨模块使用方只依赖 RPC 契约。
  业务接入 RPC 契约不存在（§5.1），无业务客户端包；原 `proto/gen/apiv1` 随契约收敛移除。
- `api/stateflux/task/v1`（task.proto，原 dispatch.proto）是调度↔执行的内部契约（调度侧与执行侧
  均在本模块内）。所有 proto 统一收编 `api/`（v3.16 布局约定），其生成码物理公开但**仅模块内
  import**（调度侧 `task/dispatch` 与执行侧 worker，二者均经 `eventbus/channel` 承载），视同内部契约。
- `build/` 与 `hack/` 是非 Go 的工程配套：镜像构建（多阶段 Dockerfile，构建上下文为仓库根）、
  本地开发中间件编排（docker-compose，仅绑定 127.0.0.1）与开发脚本（启停/冒烟）；不参与
  编译，不引入依赖。
- 测试代码（单元/e2e/`internal/test` 测试基座）于 v3.15 清空——当前阶段以 `go build`/
  `go vet` + demo 冒烟验证；需要时按聚合重建（embedded PG / miniredis 依赖届时再引入）。

### 8.1 布局迁移（v3.9 平铺 → v3.10 收编）

> **历史记录**：以下清单是 v3.10 当时的机械搬迁过程（2026-09-10 执行完毕）。命令与路径随后续版本
> 演进（v3.12/v3.15/v3.16 等）已过期，**勿按此执行**；当前验证方式见本节第 6 条与上文可见性规则。

语义零变更，纯机械搬迁；已于 2026-09-10 按序执行完毕，v3.11 增量：`sdk` 提升回顶层、新增
`build/` 与 `hack/`；v3.12 增量：`store`/`storepg` 重组为 `domain`（接口）+ `domain/repository/pg`
（实现，数据访问分层、接口按聚合拆分，§8）；v3.13 增量：`collector`/`reconcile`/`executor`
收拢至 `internal/server/` 下（编排归组，运行语义不变）；v3.15 增量：`sdk` 清空并按 model 并入
`domain` 根（task/callback/result/ops/handler/retry/snowflake），`factory`/`queue`/`dispatch`
归组 `internal/task/`，仓储全面 ent 化，测试代码清空；v3.16 增量：proto 契约收编 `api/`（`proto/`、
`internal/proto/` 取消），`internal/api` 更名 `internal/biz`（承载 task/v1 服务端业务），`executor`
更名 `worker`、`scheduler`/`collector`/`reconcile` 收拢至 `internal/controller/`（控制面归组），
`queue` 下沉 `domain/cacheview`，迁移独立 `domain/migration`；v3.17 增量：`internal/controller`
更名 `internal/runtime`（控制面运行时归组目录更名，归组语义与运行语义不变）；v3.18 增量：业务接入
收敛为 `enqueue_tasks` + `task_identities` 幂等账本，新增 `control_leases`/`concurrency_reservations`
与 R1–R5（v3.19 撤回其中的 tenant 与 Redis Streams 可靠投递）；v3.19 增量：新增 `internal/eventbus`
（channel 能力抽象 + rpc/redis adapters），Redis 定位收敛为不可靠传输实现。原始清单：

1. `git mv` 14 个引擎包入 `internal/`：sdk/api/scheduler/executor/collector/factory/reconcile/
   dispatch/store/storepg/queue/cluster/config/obs（`storepg/ent` 生成码随包整体移动）；
2. `internal/cmd` → `cmd/stateflux`；`internal/testpg`、`internal/e2e` → `internal/test/{testpg,e2e}`；
3. `proto/dispatch.proto` → `internal/proto/dispatch.proto`，`go_package` 改为
   `github.com/improvtrace/stateflux/internal/proto/gen/dispatchv1`，生成码移至
   `internal/proto/gen/dispatchv1`；`apiv1`/`clusterv1` 的 `go_package` **不动**；
4. 全量重写 import 路径（`stateflux/<pkg>` → `stateflux/internal/<pkg>`）；
5. Makefile：`build` 指向 `./cmd/stateflux`，`generate` 指向 `./internal/storepg/ent/schema`，
   `proto` 拆为对外（`proto/*.proto`）与内部（`internal/proto/*.proto`）两条 protoc 命令；
6. 验证：`go build ./...` + `go vet ./...` + demo 冒烟（`hack/dev.sh up` 与 `hack/dev.sh run`）；
   测试基座已按上文可见性规则清空，`make test` 目标当前不存在；README 示例路径同步。
