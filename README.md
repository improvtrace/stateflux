# stateflux

基于 **PostgreSQL（唯一权威）+ Redis（实时视图）** 的分布式任务调度框架（Go）。
单调度节点集权、执行侧无状态、选举外置——以四个阶段集合承载任务生命周期，
结果至多落一次库、绝不丢（at-least-once 交付 + 业务幂等）。

> 设计方案：[`.docs/design/stateflux-design.md`](./.docs/design/stateflux-design.md)
> （本实现对应 v3.8，§12 实施顺序第 1~10 步；第 11 步扩展开关按需启用，见文末）。

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
- **观测**（§6.5）：指标统一 OpenTelemetry（OTLP 导出可替换），§6.5 最小指标集全量埋点。

## 模块（§8）

```
stateflux/
├── cmd/stateflux/   # 官方服务装配（参考实现）：全角色合一的单进程入口
├── proto/           # 契约（proto3）：dispatch（内部）/ cluster（外部选举）/ api（业务接入）
├── sdk/             # 业务方唯一依赖面：Task/TaskResult/Handler/CallbackSpec/退避/雪花 ID
├── api/             # 创建（模式 B RPC）/ 长轮询回执 / 死信运维面
├── scheduler/       # 约束晋升 / 自适应认领 / sync 分发池 / async LPUSH / 统一结果缓冲
├── executor/        # 消费循环 / 租约 / 结果 WAL / gRPC server / 客户端池
├── collector/       # Collect 拉取 → 终态事务（含回调派生）→ 墓碑 → Ack
├── factory/         # TaskFactory 周期任务生成（仅调度节点运行）
├── reconcile/       # R1~R4 对账
├── store/           # 阶段集合逻辑接口；默认实现 = ent + PG 四阶段表 + payload 分离
├── queue/           # Redis 视图：就绪队列 / inprocess 集合 / 墓碑 / 节点容量
├── cluster/         # ClusterView 外部接口 + static 单机实现 + RPC 客户端
├── config/          # 配置与 OTel MeterProvider/OTLP 装配
└── obs/             # §6.5 最小指标集（低基数标签纪律）
```

依赖方向：`cmd → 角色包 → store/queue → sdk`。各角色包可独立装配（§1.2.8 框架优先）：
业务方可把 executor 角色嵌入自身进程，其余角色由独立 stateflux 服务承担。

## 快速开始

### 1. 准备中间件

PostgreSQL 12+ 与 Redis 5+（本仓库测试使用 embedded-postgres 与 miniredis，生产请自备 HA 实例）。

### 2. 注册 Handler 并启动单进程（static 单机模式）

```go
// 业务方在嵌入形态下自行装配（cmd/stateflux 是同样的装配）：
reg := sdk.NewRegistry()
reg.Register(&sdk.HandlerFunc{
    HandlerType: "email.send",
    Exec: func(ctx context.Context, task *sdk.Task) ([]byte, error) {
        // task.Payload 为 JSON 载荷；返回值原样写入 task_results.result（jsonb）
        return []byte(`{"sent":true}`), nil
    },
})

wal, _ := executor.OpenWAL(executor.WALConfig{Dir: "/var/lib/stateflux/wal"}, logger)
exec, _ := executor.New(executor.Options{NodeID: "node-1", Registry: reg, Queue: q, WAL: wal})
go exec.Run(ctx)
```

### 3. 创建任务（两种模式，幂等语义一致）

**模式 A（首选）业务直写任务表组（outbox 式）**：在业务状态变更的同一本地事务内
`INSERT INTO pending_tasks + task_payloads`（字段见 `store/ent/schema`，payload 必须 JSON，
`id` 用业务侧雪花/任意唯一整数）。

**模式 B `CreateTasks` RPC**：

```go
conn, _ := grpc.NewClient("127.0.0.1:7000", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := apiv1.NewTaskServiceClient(conn)
resp, _ := client.CreateTasks(ctx, &apiv1.CreateTasksRequest{Items: []*apiv1.CreateTaskItem{{
    Type: "email.send", Payload: []byte(`{"to":"a@b.c"}`),
    IdempotencyKey: "order-1", ExecMode: "async",   // sync 任务用 GetResults 长轮询等回执
}}})
```

同步任务回执（§5.6）：

```go
resp, _ := client.GetResults(ctx, &apiv1.GetResultsRequest{TaskIds: ids, WaitMs: 10000})
```

### 4. 集群部署

- 全部实例跑同一个二进制 `cmd/stateflux`；
- 外部选举/成员服务实现 `proto/cluster.proto` 的 `GetClusterInfo`（返回节点列表与
  `scheduler_node_id`），各实例以 `--cluster-mode external --cluster-endpoint ...` 接入；
- 被选举为调度节点的实例自动启用 Scheduler/Collector/Reconciler/Factory 角色；
  故障切换无交接协议——认领与对账全部幂等，新调度节点从 PG 自然接管。
- static 模式（默认）把全部角色赋给本进程，单机闭环（开发/小规模）。

### 5. 观测

`config.NewMeterProvider` 装配全局 OTel MeterProvider（OTLP gRPC/HTTP，默认 60s 推送）；
指标清单见设计 §6.5（队列深度、inprocess 大小、claim→投递延迟、归集延迟、R1 重置次数、
注册拒绝数、终态计数、WAL 水位、free_slots、handler 耗时）。

## 开发

```bash
make proto   # 需要 protoc + protoc-gen-go + protoc-gen-go-grpc
make generate # ent 代码生成（store/ent/schema）
make test    # 全量测试（store/reconcile/e2e 自动拉起 embedded PG；queue 使用 miniredis）
make build   # 构建 cmd/stateflux
```

测试基座：`internal/testpg`（共享 embedded PostgreSQL，验证 SKIP LOCKED / 部分唯一索引 /
数据修改型 CTE 等 PG 专属行为）。

## 实现说明（与设计稿的对应关系）

- **attempts 计数**：仅认领时 +1（§14.3 确认决策），fencing token = 每次执行的 attempts 值；
  对账重置不直接改 attempts——重置消耗重试预算体现在随后的重新认领（+1）上；
  attempts 耗尽（`attempts >= max_attempts`）由 store 在重试/重置 SQL 内路由 `completed{dead}`（R3）。
- **回调规格**：任务行 `callback` 字段只含 `OnSuccess`/`OnError` 两个子规格；子规格可嵌套
  （其自身完成后再派生），深度上限 `sdk.MaxCallbackDepth = 8`。参数注入照搬 Machinery 核心：
  OnSuccess 注入 `parent_result`、OnError 注入 `parent_error`（模板 JSON 对象合并）。
  派生任务默认 async、继承父任务 type 之外的全部执行参数。
- **幂等键唯一性**（§3.1 遗留项落地）：创建时跨非终态三表查重 + 各表局部唯一索引
  （`CREATE UNIQUE INDEX ... WHERE idempotency_key <> ''`，ent 注解不支持谓词索引，由
  `store.Migrate` 幂等 DDL 补建）+ `INSERT ... ON CONFLICT DO NOTHING`。
- **NOTIFY 唤醒**（§5.1）：语句级触发器 `stateflux_new_task`（payload 留空），专用连接 LISTEN
  （`store.NotifyWatcher`，与 PgBouncer transaction mode 不兼容的约束已隔离）；定时 tick 兜底
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
