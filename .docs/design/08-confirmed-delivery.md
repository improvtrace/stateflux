# stateflux 设计 · 落地补充确认（§15）

> v4.0（2026-09-14）。本节是「按设计实现当前项目」时由需求方补充确认的 13 条落地约定，
> 优先级高于 §8 目录树中与之冲突的历史描述；§1–§14 的机制语义（PG 唯一权威、四阶段账本、
> attempt fence、R1–R4 对账、best-effort EventBus）全部保留。§ 编号沿用，本节为 §15。

## 15.1 补充确认条款

| # | 确认 | 落点 |
| --- | --- | --- |
| 1 | 项目启动依赖 **wire** 注入 | `internal/server/wire.go`（provider set）+ 生成 `wire_gen.go`；`cmd/stateflux` 只调用 `server.InitializeApplication` |
| 2 | `api/` 下契约的具体实现放在 `internal/biz` | biz 实现全部生成的服务端接口（task/worker/dispatch/coherence/forward），并注册能力与工厂 |
| 3 | `api/cluster` 定义外置节点信息：`node_id` / `vpc` / `label`；客户端封装在 `internal/cluster`，支持 **http / grpc**；连接信息在 `internal/config.Config.Cluster` | `api/cluster/v1`（NodeInfo 增 `vpc`/`label`）、`internal/cluster`（`View` + grpc/http/static 实现）、`config.Cluster` |
| 4 | `api/stateflux/coherence` 同步集群共识信息（如「异步 redis 队列 ↔ 节点」映射）；由调度节点分配、经 RPC 通知执行节点 | `api/stateflux/coherence/v1`（`CoherenceService`：`GetCoherence`/`Notify`）、调度侧分配器 + 执行侧应用器 |
| 5 | `api/dispatch` 向外提供任务分发接口；支持 `at_least_once` / `at_most_once` / `exactly_once` 三种语义；支持节点间转发；支持异步 redis queue 与同步 rpc 两种投递 | `api/dispatch/v1`（顶层，对外）、`internal/task/dispatch`（调度侧编排）、biz 的 `DispatchService` 服务端 |
| 6 | 新增 `api/stateflux/forward` + `internal/forward`，执行节点间 RPC 转发 | `api/stateflux/forward/v1`、`internal/forward` |
| 7 | `api/stateflux/worker` 定义用户自定义 RPC（执行节点能力，如主机验密、文件上传）；实现放 `internal/biz` | `api/stateflux/worker/v1`、biz 的能力实现 |
| 8 | `internal/worker` **不实现** worker RPC，只组织/注册/管理能力 | `internal/worker`（`Capability` + `Registry` + `Manager` + 执行运行时），rpc 适配在 biz |
| 9 | `internal/domain/cacheview` 管理任务异步执行状态（对 redis 中 task 状态操作的逻辑封装） | `internal/domain/cacheview` |
| 10 | `internal/obs` 定义 stateflux 指标 | `internal/obs`（在 §6.4 基础上补 dispatch/forward/coherence/capability 指标） |
| 11 | `runtime` 是 stateflux 运行时；`runtime/scheduler` 可有多个实例，实现 task 分发的不同触发方式 | `internal/runtime/scheduler`（`Scheduler` + 可插拔 `Trigger`，多实例并存） |
| 12 | `internal/task` 管理 factory、task 接口定义与注册管理；factory 实现放 `internal/biz` | `internal/task`（`Task`/`Factory` 接口 + `Registry`/`Manager`）、biz 的具体 factory |
| 13 | 异步任务分发消费的 encode/decode 参考 go machinery 实现 | `internal/task/codec`：签名头 + JSON 任务体，注册表按 `type`/`operator` 反序列化 |

## 15.2 与 §8 目录树的差异（本节生效）

```
api/
├── cluster/v1/                 # 外置集群视图（对外）
├── dispatch/v1/                # 任务分发（对外，v4 顶层）
└── stateflux/
    ├── task/v1/                # 调度↔执行信封 + ExecutorService
    ├── coherence/v1/           # 共识信息同步（队列↔节点映射）
    ├── forward/v1/             # 节点间 RPC 转发
    └── worker/v1/              # 执行节点能力 RPC

internal/
├── biz/                        # api/ 全部服务端实现 + 具体 factory + 能力实现
├── cluster/                    # ClusterView：grpc / http / static
├── forward/                    # 节点间 RPC 转发器
├── worker/                     # 能力注册/管理 + 执行运行时（rpc 适配在 biz）
├── task/                       # Task/Factory 接口 + Registry/Manager
│   ├── codec/                  # machinery 风格 encode/decode
│   ├── dispatch/               # 调度侧分发编排
│   └── factory/                # 工厂运行器（实现由 biz 提供）
├── runtime/
│   └── scheduler/              # Scheduler + Trigger（多实例）
├── domain/cacheview/           # redis 任务异步状态封装
├── server/                     # wire 装配（wire.go + wire_gen.go）
└── ...
```

## 15.3 关键判定

1. **`api/dispatch` 为顶层对外契约**：§15.1 第 5 条字面为 `api/dispatch`（与第 3 条 `api/cluster`
   同级），而第 4/6/7 条均为 `api/stateflux/*`；据此 dispatch 落在顶层，原
   `api/stateflux/dispatch/v1` 占位随之退役（其调度↔执行语义由 `api/stateflux/task/v1` 承担）。
2. **`exactly_once` 是投递语义而非正确性承诺**：与 §13「不承诺 exactly-once」一致——
   `at_most_once` 至多发一次（放弃重试），`at_least_once` 允许重复，`exactly_once` 由
   「PG 幂等账本 + 去重窗口」在**分发入口**尽力实现，任何通道丢失仍由 R1 对账兜底。
3. **coherence 的 owner 是调度节点**：映射由调度节点计算并推送；执行节点只读应用，
   不自行分配（呼应 §1.2.7 调度侧集权）。
4. **转发不改变归属**：`internal/forward` 只做 RPC 中转（携 `visited` 环路保护与 TTL），
   不产生新的调度决策。

## 15.4 实施状态（v4.0 落地）

| 条款 | 状态 | 证据 |
| --- | --- | --- |
| 1 wire 注入 | ✅ | `internal/server/wire.go` + `wire_gen.go`（`make wire`）；`cmd/stateflux` 仅调用 `server.InitializeApplication` |
| 2 实现落 biz | ✅ | `internal/biz` 实现 executor/capability/dispatch/coherence 服务端 + factory/handler/能力 |
| 3 cluster http/grpc | ✅ | `api/cluster/v1`（vpc/label/roles/capabilities）、`internal/cluster`（grpc/http/static + Cache）、`config.Cluster` |
| 4 coherence | ✅ | `api/stateflux/coherence/v1` + `biz.CoherenceStore/Allocator/Pusher/Syncer/Puller` |
| 5 dispatch 语义/投递/转发 | ✅ | `api/dispatch/v1` + `biz.DispatchServer` + `task/dispatch.Dispatcher`（三种语义、两种投递、转发） |
| 6 forward | ✅ | `api/stateflux/forward/v1` + `internal/forward`（环路/TTL 保护） |
| 7 worker 能力 | ✅ | `api/stateflux/worker/v1`（ListCapabilities/Invoke/VerifyPassword/UploadFile）+ biz SSH/SFTP 实现 |
| 8 worker 组织能力 | ✅ | `internal/worker` 的 `Capability`/`Registry`/`HandlerRegistry`/`Runtime`/`WAL`；RPC 适配在 biz |
| 9 cacheview | ✅ | `internal/domain/cacheview`（任务状态、去重、在途计数、队列路由；Redis/Mem 双实现） |
| 10 obs | ✅ | `internal/obs`（§6.4 指标 + dispatch/forward/coherence/capability/factory 指标 + OTel 装配） |
| 11 runtime/scheduler 多实例 | ✅ | `runtime/scheduler` 的 `Trigger`（tick/notify/coherence/manual）+ `Scheduler`/`Group` |
| 12 task 管理 factory | ✅ | `internal/task`（`Task`/`Registry`）+ `task/factory`（Factory 注册与运行器）；实现在 biz |
| 13 machinery 风格 codec | ✅ | `internal/task/codec`（签名 + 消息体帧） |

验证命令：
```bash
make api && make generate && make wire && make build && make vet
STATEFLUX_TEST_DSN='postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable' go test ./...
```

端到端冒烟（本地 PG+Redis）：factory → enqueue → promote → claim → Redis 队列分发 → worker
（codec 解码/执行）→ gRPC ResultStream → collector → `task_completeds`/`task_results`，已实测完成。
