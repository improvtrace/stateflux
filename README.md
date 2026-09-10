# stateflux

基于 **PostgreSQL（唯一权威）+ Redis（实时视图）** 的分布式任务调度服务（Go 实现，独立部署运行）。
单调度节点集权、执行侧无状态、选举外置——以四个阶段集合承载任务生命周期，
结果至多落一次库、绝不丢（at-least-once 交付 + 业务幂等）。业务经本地事务直写任务表组
（outbox 式）接入（业务接入 RPC 契约不做），执行逻辑以 Handler 在构建时注册。

> 设计方案：[`.docs/design/README.md`](./.docs/design/README.md)（已按主题拆分为多个文件，§ 编号全库沿用，映射见索引）
> （目录布局对齐 **v3.16**；当前处于骨架阶段——目录结构与 proto 契约已定稿，各包业务实现按 §12
> 实施顺序重建，第 11 步扩展开关按需启用。布局演进见 §8/§8.1：v3.10 Go 标准布局收编，
> v3.12 数据访问分层，v3.14 domain 四层，v3.15 内核并入 domain，
> v3.16 契约收编 api/ + biz/调度侧/执行侧两运行时）。

## 特性

- **四阶段生命周期**（§1.3）：`pending → schedulable → processing → completed`，
  生命周期由所在表表达；payload 分离表 + 独立结果集 `task_results`（终态事务一次性写入、不可变）。
- **调度预处理**（§5.2）：约束晋升产出小而纯的就绪集；内置 per-type 并发上限 + 业务 `Precondition` 钩子。
- **单条 SQL 认领**（§9.1）：`schedulable → processing` 挪行原子完成（`FOR UPDATE SKIP LOCKED`），
  attempts +1 作为 fencing token。
- **同步/异步统一信封**（§3.3）：proto `TaskMessage` 内联完整任务，执行侧零 PG 读；
  sync = 调度节点 RPC Execute（并发闸门 + deadline），async = Redis LIST + BRPOP。
- **inprocess 共识集合**（§3.2）：注册/续约/移除全原子、带归属校验（僵尸节点无法覆盖新尝试）；
  终态墓碑 + attempt 校验双防线拦截陈旧副本（§6.2）。
- **结果归集 pull + Ack**（§5.5）：执行侧结果 WAL（落盘先行、Ack 水位截断、重启重放、
  高水位反压）+ Collector 拉取 → 单事务终态搬移 → 墓碑 → Ack；至多落一次库、绝不丢。
- **回调派生**（§5.8）：OnSuccess/OnError 规格在终态回写同事务内派生（幂等键 =
  `parent:attempt:outcome`），可嵌套覆盖链式场景。
- **对账 R1~R4**（§6.3）：滞留重置 / 幽灵清理 / 死信路由 / Redis 丢失重建；
  退避为 capped exponential + full jitter。
- **TaskFactory**（§5.7）：period/cron 生成一次性实例；`next = last_success + period` 现算、
  错过窗口即跳过、超限冻结孤儿。
- **观测**（§6.5）：指标与链路追踪统一 OpenTelemetry（OTLP 导出可替换），§6.5 最小指标集全量埋点。

## 模块（§8，v3.16）

```
stateflux/
├── api/             # 全部 proto 契约（生成码与 proto 同目录）
│   ├── stateflux/task/v1/  # 调度↔执行契约：ExecutorService（Execute/Collect）+ TaskMessage（仅模块内 import）
│   └── cluster/v1/         # 外置 cluster 访问契约：ClusterService（外部实现、stateflux 消费）
├── cmd/stateflux/   # 进程入口：flag/env → config → obs 装配 → internal/server 编排启动
│                    # （工具链命令如实时指标查询，按需在 cmd/ 扩展）
├── internal/        # 引擎收编（Go internal 可见性：外部模块禁止 import）
│   ├── server/      # 编排层：biz + scheduler 运行时 + worker 运行时的装配与启停 + Handler 注册点（§7）
│   ├── biz/         # task/v1 服务端业务：Execute 执行编排 / Collect 结果上报
│   ├── controller/  # 控制面（调度侧）运行时归组：随选举启停，仅调度节点运行
│   │   ├── scheduler/ # 调度核心：约束晋升 / 自适应认领 / sync 分发池 / async LPUSH
│   │   ├── collector/ # Collect 拉取 → 终态事务（含回调派生）→ 墓碑 → Ack
│   │   └── reconcile/ # R1~R4 对账
│   ├── worker/      # 执行侧运行时（常驻所有节点）：消费循环 / 租约 / 结果 WAL / task/v1 gRPC server（委托 biz）
│   ├── task/        # 任务构建与分发：factory（周期任务生成）/ dispatch（客户端池 + 统一结果缓冲）
│   ├── domain/      # 领域层 + 引擎共享内核（按 model 划分文件）：task/callback/result/ops +
│   │   │            #   聚合接口（Task/Result/Ops）+ 组合根 Store + Handler/退避/雪花 ID
│   │   ├── repository/ # 仓储实现层：聚合语义（创建/晋升/认领/终态/对账/查询，ent 类型安全 API）
│   │   ├── data/     # 数据源基建：pg 连接池 + ent client、redis client（ent 生成码构建时生成，不入库）
│   │   ├── cacheview/ # Redis 视图：就绪队列 / inprocess 集合 / 墓碑 / 节点容量
│   │   ├── migration/ # 数据面迁移：bootstrap DDL + ent migrations
│   │   └── schema/   # ent 表定义（代码生成输入：四阶段表 + payload + result）
│   ├── cluster/     # ClusterView 外部接口 + static 单机实现 + RPC 客户端
│   ├── config/      # 配置结构与默认值补全（含 metrics/tracing 装配参数）
│   └── obs/         # metrics + tracing：OTel MeterProvider/TracerProvider/OTLP 装配
├── build/           # 打包与本地环境：服务 Dockerfile + docker-compose（PG/Redis 开发栈）
└── hack/            # 开发脚本：dev.sh（中间件启停 + demo 冒烟运行）
```

依赖方向：`cmd/stateflux → internal/server（编排 biz/controller/worker）→ biz、控制面运行时
（controller）、执行侧运行时（worker）、task（factory/dispatch）→ domain/cluster`；
数据访问 `domain ← repository/cacheview ← data ← schema/migration` 由装配注入，运行时包不 import
实现包。biz/controller/worker 相互零依赖——调度↔执行仅经 `api/stateflux/task/v1` 契约与
`task/dispatch` 传递，全服务只有一条 PG 终态写路径（§5.5）。本项目交付**独立部署的调度服务**
（§1.2.8 服务优先）：全部引擎包收编 `internal/`，「不作为三方库对外承诺 API、不支持嵌入业务进程」
由 Go internal 可见性规则编译器强制；对外 Go 包仅 `api/cluster/v1`，`api/stateflux/task/v1`
生成码仅模块内 import。
业务执行逻辑以 Handler 在**构建时**注册（接入点 `internal/server.RegisterHandlers`），
task/v1 服务端业务由 `internal/biz` 承载。

## 快速开始

### 1. 准备中间件

PostgreSQL 12+ 与 Redis 5+（本地可用 `hack/dev.sh up` 拉起，生产请自备 HA 实例）。

### 2. 注册 Handler（构建时，static 单机模式）

业务执行逻辑以 `domain.Handler` 形式在**构建时**注册，接入点为 `internal/server` 的
`RegisterHandlers`（§7；`-demo` 冒烟模式随实现重建恢复）：

```go
// internal/server —— RegisterHandlers（§7 执行接入点）：
reg.Register(&domain.HandlerFunc{
    HandlerType: "email.send",
    Exec: func(ctx context.Context, task *domain.Task) ([]byte, error) {
        // task.Payload 为 JSON 载荷；返回值原样写入 task_results.result（jsonb）
        return []byte(`{"sent":true}`), nil
    },
})
```

构建：`make build`（入口 `cmd/stateflux`；当前 main 为骨架占位，启动参数随实现重建恢复）。

### 3. 创建任务（业务直写，outbox 式）

在业务状态变更的同一本地事务内 `INSERT INTO pending_tasks + task_payloads`
（字段见 `internal/domain/schema`，payload 必须 JSON，`id` 用业务侧雪花/任意唯一整数；
`idempotency_key` 冲突跳过插入、沿用已存在 task_id）。业务接入 RPC 契约不做（§5.1）；
同步任务回执直查 `task_results`（§5.6）。

### 4. 集群部署

- 全部实例跑同一个二进制 `stateflux`（`make build`，入口 `cmd/stateflux`）；
- 外部选举/成员服务实现 `api/cluster/v1` 的 `GetClusterInfo`（返回节点列表与
  `scheduler_node_id`），各实例以 `--cluster-mode external --cluster-endpoint ...` 接入；
- 被选举为调度节点的实例自动启用控制面运行时（Controller：Scheduler/Collector/Reconciler）与
  Factory；
  故障切换无交接协议——认领与对账全部幂等，新调度节点从 PG 自然接管。
- static 模式（默认）把全部角色赋给本进程，单机闭环（开发/小规模）。

### 5. 观测

`obs` 装配全局 OTel MeterProvider/TracerProvider（OTLP，默认 60s 推送；metrics + tracing，
装配随实现重建，§12.10）；指标清单见设计 §6.5（队列深度、inprocess 大小、claim→投递延迟、
归集延迟、R1 重置次数、注册拒绝数、终态计数、WAL 水位、free_slots、handler 耗时）。

## 开发

```bash
make proto   # 需要 protoc + protoc-gen-go + protoc-gen-go-grpc（契约统一 api/，task/v1 与 cluster/v1 各一条命令）
make generate # ent 代码生成（--feature sql/upsert,sql/lock,sql/execquery；schema：internal/domain/schema → 输出 internal/domain/data/ent；生成码不入库，克隆后先执行）
make build   # 构建内置服务 stateflux（入口 cmd/stateflux，产物 ./stateflux）
```

本地开发与打包：

```bash
hack/dev.sh up    # docker compose 拉起 PG + Redis（build/docker-compose.yml，仅绑定 127.0.0.1）
hack/dev.sh run   # 构建并以 demo Handler 启动单机闭环（API :7000）
hack/dev.sh down  # 停止并清理中间件
docker build -f build/Dockerfile -t stateflux .   # 服务镜像（多阶段构建，distroless 运行）
```

## 实现说明（与设计稿的对应关系）

> 以下为 v3.15 实现沉淀的设计对齐结论；实现清空后按 §12 重建时沿用。

- **attempts 计数**：仅认领时 +1（§14.3 确认决策），fencing token = 每次执行的 attempts 值；
  对账重置不直接改 attempts——重置消耗重试预算体现在随后的重新认领（+1）上；
  attempts 耗尽（`attempts >= max_attempts`）由仓储在重试/重置事务内路由 `completed{dead}`（R3）。
- **回调规格**：任务行 `callback` 字段只含 `OnSuccess`/`OnError` 两个子规格；子规格可嵌套
  （其自身完成后再派生），深度上限 `domain.MaxCallbackDepth = 8`。参数注入照搬 Machinery 核心：
  OnSuccess 注入 `parent_result`、OnError 注入 `parent_error`（模板 JSON 对象合并）。
  派生任务默认 async、继承父任务 type 之外的全部执行参数。
- **幂等键唯一性**（§3.1 遗留项落地）：创建时跨非终态三表查重 + 各表局部唯一索引
  （`CREATE UNIQUE INDEX ... WHERE idempotency_key <> ''`，ent 注解不支持谓词索引，由
  `data.Migrate` 幂等 DDL 补建）+ ent upsert（`CreateBulk ... ON CONFLICT ... DO NOTHING`）。
- **NOTIFY 唤醒**（§5.1）：语句级触发器 `stateflux_new_task`（payload 留空），专用连接 LISTEN
  （`pg.NotifyWatcher`，与 PgBouncer transaction mode 不兼容的约束已隔离）；定时 tick 兜底
  不可关闭——NOTIFY 仅作低延迟优化。
- **同步任务的 inprocess 窗口**：Execute 响应后即移出成员（§5.4）；响应之后、终态落库之前
  的窗口由调度侧统一结果缓冲 + 对账 grace + attempt 校验收敛（at-least-once）。
- **水位型指标**：设计为 ObservableGauge，实现用同步 `Int64Gauge`——采集点位收敛在既有
  写路径（§6.5），语义相同、免去回调装配。
- **v1 未含的扩展开关**（§12.11，按需启用）：调度分片、队列按 type 拆分、completed_tasks
  分区归档、Redis Streams 备选链路（§14 遗留）。

## 边界（§13）

at-least-once 交付（handler 必须幂等）；不强制杀死执行中的任务；不实现业务执行器、选举服务与
成员协议；PG/Redis HA 属部署域；不承诺跨队列全局有序；cancel 能力留待 v2。
