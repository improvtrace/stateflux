# 模块包划分与关键接口（承接《分布式 Job 任务调度框架设计方案》，v2 修订）

> 本文是设计方案（§1~§8）的落地补充：给出 Golang 风格的模块包划分与关键 interface 定义。
> 文中「文档 §n」均指原设计方案章节。
>
> **v2 修订**（相对 v1）：
> 1. stateflux **不实现集群选举**，集群成员/角色/地址由**外部接口**提供（`internal/cluster.ClusterView`），集群内部协议 `cluster.proto` 删除；
> 2. 项目**只有一个进程入口** `cmd/stateflux`，本进程承担哪些角色（leader/scheduler/worker）由外部集群信息决定，可单机合并部署；
> 3. 精简 `internal/`：删除 `leader/`（选举）、`result/`（并入 scheduler）、`metrics/`（改为 config 注入观测钩子），`store/postgres`、`cacheview/redis` 子包并入父包。

## 一、模块包划分（Golang 风格）

```
stateflux/
├── cmd/
│   └── stateflux/main.go        # 唯一进程入口：查询 ClusterView 得到本进程角色，按需启动引擎
├── api/
│   └── proto/stateflux/v1/
│       └── dispatch.proto       # 调度角色 ↔ 执行角色 的 gRPC 契约（执行/投递/结果上报/心跳）
├── internal/
│   ├── domain/                  # 领域模型，零外部依赖，全框架唯一的模型引用点
│   │   ├── job.go               #   Job / JobResult / Constraints
│   │   ├── state.go             #   状态机：合法迁移表 + version 递增规则
│   │   └── errors.go            #   错误分类：可重试 / 不可重试 / 版本冲突
│   ├── store/                   # PG 权威存储层（接口与 PG 实现同包）
│   │   ├── jobstore.go          #   JobStore 接口定义
│   │   ├── claim.go             #   认领：UPDATE ... WHERE job_id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING
│   │   ├── batch.go             #   批量回写：COPY / unnest 单事务合并 N 条状态更新
│   │   └── migrations/          #   建表 SQL（含 (status, business_type, create_time) 联合索引）
│   ├── cacheview/               # Redis 内存视图层（接口与 Redis 实现同包，全部 key 带 TTL，可基于 PG 重建）
│   │   ├── view.go              #   CacheView 接口定义
│   │   ├── inflight.go          #   in-processing 实时视图 + 执行超时 TTL
│   │   ├── limiter.go           #   主机并发计数器 + 账号排他锁（Lua 保证原子）
│   │   ├── blacklist.go         #   取消黑名单 {job_id: version}，TTL ≈ max_life_time
│   │   └── subset.go            #   预处理子集缓存 + 灾难恢复重建入口
│   ├── cluster/                 # 【外部接口】集群成员信息——stateflux 不实现选举
│   │   ├── view.go              #   ClusterView 接口：本进程角色、存活节点列表、Leader 位置
│   │   └── static.go            #   静态实现（单机/开发/测试）；生产实现由部署侧注入（K8s/注册中心等）
│   ├── scheduler/               # 调度编排（原 scheduler + result + leader 兜底职责合并于此）
│   │   ├── loop.go              #   双触发主循环：定时器轮询兜底 + 饥饿信号驱动
│   │   ├── filter.go            #   业务约束过滤（调 cacheview：黑名单/主机并发/账号锁）
│   │   ├── dispatcher.go        #   分发编排：Factory 路由 + 本地分发队列 + 同步任务并发闸门
│   │   ├── shard.go             #   分片分配：默认一致性哈希，基于 ClusterView 节点列表，节点增减自动再平衡
│   │   ├── reconcile.go         #   兜底对账：PG 超时任务 vs Redis 视图；重试入队/置失败；Redis 灾难重建
│   │   └── result.go            #   结果统一收口 + 批量回写（条数/时间双阈值 flush，原 result 包并入）
│   ├── dispatch/                # 分发抽象层（文档 §8.1 的 Factory）
│   │   ├── factory.go           #   DispatchFactory / FactoryRegistry 接口 + WorkerConn 抽象
│   │   ├── sync.go              #   同步任务：gRPC 同步调用阻塞等结果（单机模式可走进程内直连）
│   │   └── async.go             #   异步任务：投递即返回
│   ├── exec/                    # 执行引擎（原 worker 包）
│   │   ├── server.go            #   gRPC server：接单、取消指令、心跳
│   │   ├── registry.go          #   business_type → JobExecutor 注册表（业务接入点）
│   │   ├── runmgr.go            #   运行时管理：goroutine 生命周期、version 匹配优雅终止
│   │   └── resultcache.go       #   近期执行结果暂存（gRPC 响应丢失后的恢复依据，文档 §7.4）
│   └── config/                  # 配置加载与默认值；观测钩子由此注入（原 metrics 包并入，不单设包）
└── pkg/
    └── idgen/                   # 雪花 ID 生成器（可对外复用）
```

依赖方向：`cmd/stateflux → cluster + {scheduler, exec} → dispatch → store/cacheview → domain`。
`domain` 不依赖任何外部包；业务方只依赖 `domain` + `exec.JobExecutor`。

### 进程模型（对应修订 1、2）

- 集群里跑的是**同一个二进制** `stateflux` 的 N 个实例；谁是 leader、谁是 scheduler、谁是 worker，**由外部系统决定**并通过 `ClusterView` 暴露，stateflux 内部零选举代码。
- `main.go` 装配逻辑：`ClusterView.Self()` 拿到本进程角色后按需启动引擎——
  - `scheduler` 角色 → 启动调度主循环（loop + dispatcher + result）；
  - `worker` 角色 → 启动执行引擎 gRPC server；
  - `leader` 角色 → 启动兜底对账循环（reconcile）。
- 单机/开发模式：`cluster.static` 把三种角色都给本进程，一个进程闭环（文档 §8.4 的单机实现路径）。
- 分片不再需要 Leader 下发协议：所有 scheduler 角色进程对 ClusterView 返回的**同一份节点列表**做一致性哈希，各自算出自己负责的分片；节点上下线只改变节点列表，哈希环自动再平衡（文档 §7.3 的故障接管由此天然覆盖）。

## 二、关键接口定义（Go 伪代码）

### 1. 领域模型（internal/domain）—— 无变化

```go
type JobStatus string

const (
    StatusPending      JobStatus = "pending"
    StatusInProcessing JobStatus = "in_processing"
    StatusSuccess      JobStatus = "success"
    StatusFailed       JobStatus = "failed"
    StatusCancelled    JobStatus = "cancelled"
)

type DispatchMode string

const (
    ModeSync  DispatchMode = "sync"  // 同步调用，阻塞等结果
    ModeAsync DispatchMode = "async" // 投递后异步执行
)

type Job struct {
    ID           string        // 全局唯一（雪花 ID），一切幂等判断的键
    BusinessType string
    TargetHost   string
    Account      string
    Payload      []byte
    Version      int64         // 每次状态迁移/取消请求 +1，取消一致性的兜底依据
    Status       JobStatus
    DispatchMode DispatchMode
    Constraints  Constraints   // max_host_concurrency、账号排他等业务约束
    CreateTime   time.Time
    MaxLifeTime  time.Duration // ≤ 24h
}

type JobResult struct {
    JobID      string
    Version    int64
    Status     JobStatus // success / failed / cancelled
    ErrMsg     string
    WorkerID   string
    CostTime   time.Duration
    FinishedAt time.Time
}
```

### 2. 外部集群信息接口（internal/cluster）—— 新增，替代原 Leader 选举与集群协议

```go
type Role string

const (
    RoleLeader    Role = "leader"    // 兜底对账
    RoleScheduler Role = "scheduler" // 分发
    RoleWorker    Role = "worker"    // 执行
)

type Node struct {
    ID    string
    Addr  string   // gRPC 地址，供分发调用
    Roles []Role
}

// 由部署侧提供实现并注入：K8s headless service、注册中心、静态配置均可。
// stateflux 只消费，不实现选举、不维护成员协议。
type ClusterView interface {
    Self() (Node, error)                 // 本进程的 ID/地址/角色（外部决定）
    Nodes(ctx context.Context) ([]Node, error) // 当前存活节点列表（含角色）
    Leader(ctx context.Context) (Node, error)  // 当前 leader 位置（外部指定）
}
```

### 3. PG 权威存储（internal/store）

```go
type JobStore interface {
    // 创建入口（文档 §3.1）：只写 PG，不触碰 Redis；以 job_id 幂等（ON CONFLICT DO NOTHING）
    Create(ctx context.Context, jobs []*domain.Job) error

    // 认领：单条 SQL 完成「筛 pending + 置 in_processing + RETURNING」，
    // 即文档 §4 的 SELECT ... FOR UPDATE SKIP LOCKED 范式；
    // 查询与状态迁移原子化，防止多调度节点重复分发。
    ClaimPending(ctx context.Context, q ClaimQuery) ([]*domain.Job, error)

    // 批量回写（文档 §6.3）：单事务合并 N 条最终状态，压制 PG 写 QPS
    BatchFinish(ctx context.Context, results []domain.JobResult) error

    // 兜底扫描用（文档 §3.3）：拉取 in_processing 且超过执行时限的任务
    ListTimedOut(ctx context.Context, before time.Time, limit int) ([]*domain.Job, error)

    // 重试入队：version+1 后重新置 pending（幂等重试的落地点）
    Requeue(ctx context.Context, jobID string, newVersion int64) error

    // 取消请求：version 不匹配返回 ErrVersionConflict
    RequestCancel(ctx context.Context, jobID string, expectVersion int64) error

    Get(ctx context.Context, jobID string) (*domain.Job, error)
}

type ClaimQuery struct {
    Shard         string    // 分片键
    BusinessTypes []string  // 可选过滤
    Limit         int
    ClaimedBy     string    // 本进程节点 ID（来自 ClusterView），便于故障接管时定位
    NotBefore     time.Time // 仅扫描当日任务
}
```

### 4. Redis 内存视图（internal/cacheview）

```go
// 全部 key 带 TTL；Redis 全量丢失可由 scheduler.reconcile 基于 PG 重建（文档 §5）
type CacheView interface {
    // in-processing 实时视图；TTL = 任务最大执行时长，超时即为对账候选
    MarkInProcessing(ctx context.Context, jobID, workerID string, ttl time.Duration) error
    ClearInProcessing(ctx context.Context, jobID string) error
    RebuildInProcessing(ctx context.Context, entries []InflightEntry) error

    // 业务约束：主机并发计数（Lua：超限不增，成功则设置 EXPIRE）
    AcquireHostSlot(ctx context.Context, host string, limit int, ttl time.Duration) (bool, error)
    ReleaseHostSlot(ctx context.Context, host string) error
    // 账号排他锁：SET NX EX
    AcquireAccountLock(ctx context.Context, account string, ttl time.Duration) (bool, error)
    ReleaseAccountLock(ctx context.Context, account string) error

    // 取消黑名单：TTL 略大于 max_life_time，自动回收防膨胀（文档 §3.3）
    AddToBlacklist(ctx context.Context, jobID string, version int64, ttl time.Duration) error
    CancelVersion(ctx context.Context, jobID string) (version int64, ok bool, err error)

    // 预处理子集缓存（短 TTL），避免反复查 PG
    PutPendingSubset(ctx context.Context, shard string, jobs []*domain.Job, ttl time.Duration) error
    PeekPendingSubset(ctx context.Context, shard string) ([]*domain.Job, error)
}
```

### 5. 分发抽象 Factory（internal/dispatch）

```go
// WorkerConn 抽象分发目标：集群模式是远端 worker 角色进程的 gRPC 连接，
// 单机模式是进程内直连（同一二进制内的 exec 引擎），Factory 无感。
type WorkerConn interface {
    Execute(ctx context.Context, job *domain.Job) (*domain.JobResult, error) // 同步
    Submit(ctx context.Context, job *domain.Job) error                       // 异步投递
}

// 同步/异步分发逻辑的隔离点（文档 §8.1）
type DispatchFactory interface {
    Mode() domain.DispatchMode
    // sync:  调用 WorkerConn.Execute 阻塞至返回结果；超时/网络中断返回可重试错误
    // async: 调用 WorkerConn.Submit 成功即返回 Accepted
    Dispatch(ctx context.Context, job *domain.Job, conn WorkerConn) (Outcome, error)
}

type OutcomeKind int

const (
    OutcomeCompleted OutcomeKind = iota // 同步调用直接带回最终结果
    OutcomeAccepted                     // 异步投递受理，结果稍后上报
)

type Outcome struct {
    Kind     OutcomeKind
    Result   *domain.JobResult // Kind == Completed 时非空
    WorkerID string
}

type FactoryRegistry interface {
    Register(mode domain.DispatchMode, f DispatchFactory)
    FactoryFor(job *domain.Job) (DispatchFactory, error) // 按 job.DispatchMode（或 business_type 配置）路由
}
```

### 6. 调度器（internal/scheduler）

```go
type Scheduler interface {
    // 双触发主循环（文档 §3.2）：定时器轮询兜底 + 饥饿信号驱动
    Run(ctx context.Context) error
    // Worker 饥饿 / 本地队列余量不足时调用，立即触发一轮 认领 → 过滤 → 分发
    NotifyDemand(n int)
}

// 分片分配：默认实现为一致性哈希，输入是 ClusterView.Nodes() 的同一份列表，
// 各 scheduler 角色进程各自计算、结果一致；节点增减自动再平衡，无需内部协议。
type ShardAssigner interface {
    Plan(nodes []cluster.Node, self cluster.Node) (myShards []string)
}

// 分发前过滤链：黑名单 → 主机并发 → 账号排他（内部使用 CacheView）
type ConstraintFilter interface {
    Pass(ctx context.Context, jobs []*domain.Job) (dispatchable, blocked []*domain.Job)
}

// 兜底对账（leader 角色进程运行，文档 §3.3 / §7.2）：
// PG in_processing 超时任务 vs Redis 视图；未超重试上限则 Requeue（version+1），否则置 failed
type Reconciler interface {
    Sweep(ctx context.Context) error
    // 灾难恢复：Redis 全量丢失后，扫 PG in_processing 重建全部视图（文档 §5）
    RebuildCacheView(ctx context.Context) error
}
```

### 7. 执行侧（internal/exec）—— 业务唯一接入点

```go
// 业务方只需实现此接口并注册
type JobExecutor interface {
    BusinessType() string
    // ctx 被取消 = 命中取消黑名单或执行超时；
    // 实现方在最小业务单元边界检查 ctx 并优雅退出（文档 §3.3 取消机制）
    Execute(ctx context.Context, job *domain.Job, hb HeartbeatReporter) error
}

type HeartbeatReporter interface {
    Beat(ctx context.Context) error // 由执行引擎代为上报，executor 周期性调用
}
```

### 8. gRPC 契约（api/proto/stateflux/v1）

```proto
// dispatch.proto：调度角色进程 ↔ 执行角色进程（集群内部协议 cluster.proto 已删除，
// 成员/角色/分片信息均走 ClusterView 外部接口，不走 RPC）
service WorkerGateway {
  rpc Execute(ExecuteRequest) returns (ExecuteResponse); // 同步任务，长阻塞调用
  rpc Submit(SubmitRequest) returns (SubmitResponse);    // 异步任务投递
  rpc Report(stream ResultReport) returns (Ack);         // 异步结果上报（执行 → 调度）
  rpc Heartbeat(stream Heartbeat) returns (Ack);
}
```

## 三、关键调用链（对应生命周期四阶段）

1. **创建**：业务方 → `JobStore.Create`（仅 PG，不触碰 Redis）。
2. **分发**：`scheduler.loop`（定时/饥饿触发）→ `JobStore.ClaimPending` → `ConstraintFilter.Pass`（cacheview：黑名单/主机并发/账号锁）→ `FactoryRegistry.FactoryFor` → sync（`WorkerConn.Execute` 阻塞等结果）/ async（`WorkerConn.Submit` 投递）→ `CacheView.MarkInProcessing`。
3. **执行**：`exec.server` 接单 → `runmgr` 启动 goroutine → `JobExecutor.Execute`（周期心跳；命中黑名单则 ctx 取消、优雅终止）。
4. **回写**：结果统一进 `scheduler.result`（同步返回与异步 Report 同口）→ 双阈值 flush → `JobStore.BatchFinish` → 清理 Redis 视图/计数器/锁。

## 四、实施顺序映射（对齐文档 §8）

| §8 步骤 | 落地包 |
|---|---|
| 1. 定义抽象层 | `domain`、`store/jobstore.go`、`cacheview/view.go`、`dispatch/factory.go`、`cluster/view.go` |
| 2. PG 表结构 + 批量工具 | `store/claim.go`、`store/batch.go`、`store/migrations` |
| 3. Redis 视图模型 | `cacheview`（inflight/limiter/blacklist/subset） |
| 4. 单机闭环 | `cluster/static`（三角色合一）+ `scheduler` + `exec` |
| 5. 多节点分片 | `scheduler/shard.go`（一致性哈希）+ 外部 ClusterView 实现 |
| 6. 取消/超时/重试/黑名单 | `cacheview/blacklist` + `scheduler/reconcile.go` + `exec/runmgr.go` |
| 7. 压测调优 | `config`（观测钩子注入）+ `metrics` 埋点 |

## 五、接口设计中的关键决策

1. **ClaimPending 将查询与状态迁移合并为一步**。原方案中「查 pending」与「分发成功后置 in_processing」是两步，两步之间调度节点宕机会导致任务被重复分发。接口层把二者合并为单条 `UPDATE ... WHERE job_id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING`：认领即占有，重试天然幂等（依赖 job_id 唯一约束）。
2. **同步任务必须配并发闸门**。同步调用会占住调度协程直至 Worker 返回，长任务会耗尽并发。`dispatcher` 内置协程池，调用超时上限取 `min(max_life_time, 配置上限)`；超时后任务交由 `Reconciler` 兜底判定，而非无限阻塞。
3. **取消的两级校验点共用同一视图源**。分发前由 `ConstraintFilter` 查黑名单拦截尚未下发的任务；下发后由 `exec.runmgr` 匹配 version 触发 ctx 取消、优雅终止。两处均通过 `CacheView.CancelVersion` 读取，保证视图一致。
4. **集群信息只读不治**（v2）。stateflux 不实现选举、不维护成员协议：`ClusterView` 是唯一集群事实来源，角色/成员/Leader 由外部决定；分片用一致性哈希在本地计算，节点列表变化即自动再平衡——这同时覆盖了文档 §7.3 的节点故障接管（宕机节点从节点列表消失 → 其分片被哈希环均摊，未完成任务由 in-processing TTL + Reconciler 收敛）。代价：节点列表抖动期分片短暂漂移，靠认领原子性和对账兜底保证不丢不重。
