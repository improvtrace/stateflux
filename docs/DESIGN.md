# stateflux 设计

> 本服务由 pam 的 `backend/internal/schedule-stable` 搬运而来（Go 包名沿用
> `schedule`）。原散落在 pam 内的任务表 repo、ent schema 与建表迁移已全部
> 收拢到 `states/` 与 `migrations/`，见 §2。
>
> 状态：实装记录。本版反映「dispatch 前落表 + 立即发布」模型（见 §4 D13–D15），
> 早期 v0 草案的水位补货/StateScheduler 设计已移除；被替换的 task_processings
> 全量字段表定义固化为业务表示例，见 [task_biz_example.md](task_biz_example.md)。

## 1. 总体结构与数据流

```
            ┌────────────────────────────────────────────────────────┐
            │                     entry.go（组装）                    │
            └────────────────────────────────────────────────────────┘
  cron tick ──CronEvent──▶ listener ──▶ CronScheduler
                                     │  Durable：factory.Generate 产出批次
                                     ▼
  Publisher.Dispatch ──落表（pending）──▶ PG: task_persistents ◀──reconcile──┐
       │ ClaimPending（→dispatching）                                        │
       ▼ 组装 Signature（空 RoutingKey + Attribute headers）发布            │
       ▼ dispatch 匹配节点表生成路由 queue:<resource>/<node>（两阶段确认）  │
  queue（Redis，无持久化）──▶ Worker(资源×节点) ──成功内联回写──▶ terminal   │
                                     │ 完成/失败                             │
                                     ▼                                      │
  Reconciler：在途超时收敛（终态/回 pending）＋ DispatchPending 重派 ◀───────┘
  （entry 启动时先跑一轮：重启/停止前未执行完的任务重新 dispatch 到队列）
```

核心不变式：**PG 的 task_persistents 是唯一事实源**。持久化任务在 dispatch
之前先落表（pending），之后按 phase 维护各阶段状态；Redis 队列无持久化，
崩溃后内容可全丢，恢复由 phase + updated_at 对账完成，非终态行由发布通道
重派。队列按 资源×节点 划分（queue:<resource>/<node>）：发布前 dispatch 以
Attribute headers 匹配集群节点表选出目标节点，任务由目标节点的 worker 消费。发布是两阶段（认领→dispatching，发布成功→dispatched），由此产生
**至少一次投递**语义——发布成功但未确认即崩溃的行会被重派，消费端必须按
IdempotencyKey 幂等。

持久化转发（其他节点只经 Dispatcher 提交持久化任务，DB 写入收敛在 leader）：

```
producer Dispatcher ──AttributePersistent──▶ 包装为 persistent_forward
  ──AttributeRoles=LEADER──▶ queue:persistents/<leader>
  leader worker（tasks/persistents）：落表（pending）→ 认领 → 解码内层
  Signature 重新发布 → 确认 dispatched（确认失败由对账收敛）
```

## 2. 目录 Layout

```
stateflux/                        # 根包 package schedule（import 路径 github.com/improvtrace/stateflux）
├── queue/                        # [已就位] machinery fork：异步执行框架（broker/backend/lock/worker）
├── tasks/                        # [已实装] 任务定义核心包（package tasks）
│   ├── task.go                   #   TaskSpec/Registry/NewRow/Validate（handler 签名约定）
│   ├── payload.go                #   Encode/Decode（单 string Arg 承载 JSON）
│   ├── factory.go                #   Factory 接口：产出 []DispatchableTask{Task, Sig}
│   └── persistents/              #   持久化转发的消费侧（不生产任务）
│       └── persistents.go        #   persistent_forward：落表 → 认领 → 重发 → 确认
├── states/                       # [已实装] 调度事实源的数据访问（package states）
│   ├── task_persistent.go        #   task_persistents 的领域类型 + repo 接口 + ent 实现
│   ├── client.go                 #   Open（按 DSN，含启动期就绪检查）/ NewClient（基于 *sql.DB）
│   ├── ledger.go                 #   Signature↔taskID 记账 + SignatureUUID 协议
│   ├── reconcile.go              #   对账循环：在途超时收敛 + DispatchPending 重派
│   └── ent/                      #   ent 生成码；ent/schema 是表结构的唯一来源
├── migrations/                   # [已实装] 数据库迁移的全部逻辑（package migrations）
│   ├── <时间戳>_<名称>.sql        #   Atlas 版本化迁移 + atlas.sum（由 ent schema 生成，不手写）
│   ├── migrations.go             #   go:embed 迁移目录
│   ├── migrate.go                #   Dir（加载）/ Pending（待应用项）/ Migrate（执行，目标库不存在时先建库）+ 账本读写
│   ├── format/                   #   迁移 SQL 的美化格式器（只调空白）
│   └── gen/main.go               #   由 ent schema 生成增量迁移（go run migrations/gen/main.go）
├── cmd/
│   ├── stateflux/                # [已实装] 服务入口（cobra）：装配配置、校验数据库就位、运行调度器
│   ├── datamigrate/              # [已实装] 数据库迁移的独立工具（cobra）：应用 migrations/
│   └── mock-dm/                  # [已实装] dm 服务的本地桩（cobra）：按节点数自动生成集群节点表
├── config/                       # [已实装] 配置：config.go + config.conf.example（ini）
├── api/dm/                       # dm 服务集群节点查询接口的精简 proto 与生成码（dispatch 节点表来源）
├── dispatch/                     # [已实装] 发布器：节点匹配 + 分节点路由
│   ├── dispatcher.go             #   Dispatch：空 RoutingKey 经节点表匹配生成 queue:<resource>/<node>；返回 AsyncResult
│   ├── persistent.go             #   持久化转发协议：AttributePersistent → persistent_forward（仅 leader）
│   ├── nodes.go                  #   节点表拉取（dm）+ TTL 缓存；QueueName/Attribute headers 约定
│   └── dm.go                     #   dm 客户端：unix socket 懒连接 + 服务密钥拦截器
├── wire.go                       # [已实装] InitializeRuntime 注入器（wireinject）
├── wire_gen.go                   # wire 生成物
├── scheduler.go                  # [已实装] 事件总线 Listener + CronScheduler + 发布通道 Publisher（两阶段）
├── entry.go                      # [已实装] Config + Run（生命周期编排）+ wire 装配集（ProviderSet/Runtime/NewSpecs）
├── examples/                     # 场景化集成测试（见 examples/README.md）
├── docs/
│   ├── DESIGN.md                 # 本文档：设计与决策记录
│   └── task_biz_example.md       # 业务任务表示例（task_biz_examples）schema 定义
└── README.md
```

与旧 schedule 的概念对应：`pkg/taskqueue/dispatcher→dispatch`、
`factory→tasks/factory`、`manager→scheduler+listener`、`pkg/state→states`；
`pkg/machinery → queue`（升级为 go-redis v9 版本）。callback 概念暂不实现
（见 §4 D5）。

## 3. 组件说明（均已实装）

### 3.1 tasks/ — 业务任务定义

实装于 `tasks/`（核心包）；业务任务由接入方在 `tasks/` 下按目录新增
（一目录一任务：Resource 常量、Payload、Spec()、Handler、Factory，并按需
提供 ProviderSet）。
`TaskSpec` 关键字段：`Retries`（执行内重试，映射 `Signature.RetryCount`，由
queue 框架承担）、`MaxAttempts`（重试预算落表值）、`Durable`（是否落
task_persistents）、`Payload`（非 Durable 定时任务的固定参数）、`Cron`
（定时触发，robfig/cron v3 表达式，五段或 @every 等描述符）、
`FactoryBatch`/`Factory`（Durable 定时任务到点生成批次）。消费并发由
`Concurrency` 声明（缺省 10）；队列不再静态命名，按 资源×节点 路由。

**handler 签名约定**：`func(payload string, taskID int64) error`——payload 是
发布约定的 JSON 字符串，taskID 由框架经第二个 Arg 传入（Durable 非零、直发
为 0），entry 无需反射即可包装终态回写。Validate 强制此签名。
cron 到点的处理由 `Durable` 决定：Durable → factory 生成批次，落表后
立即发布；非 Durable → dispatcher 直发（丢了下轮周期自然覆盖）。

**DispatchableTask**（factory.go）：`Factory.Generate` 的产物，二选一——
`Task` 非空为持久化任务（调用方先落表再从表行组装 Signature 发布）；`Task`
为空则 `Sig` 直接发布（无状态直发）。

### 3.2 states/ — 事实源与账本

repo 接口、领域类型与 ent 实现都在
`states/task_persistent.go`（`TaskPersistent`、`TaskPhase` 含两阶段
`TaskPhaseDispatching`、`TaskOutcome`、`TaskPersistentRepo`：
Create/Get/ClaimPending/Claim/MarkDispatched/MarkTerminal/Requeue/Inflight；ent 实现
为 PG，FOR UPDATE SKIP LOCKED 认领）。表结构
的唯一来源是 `states/ent/schema`，迁移文件由它经 Atlas 生成到 `migrations/`
（不手写），由 `cmd/datamigrate`（`migrations.Migrate`）执行（目标库不存在时
先以同一账号经维护库 postgres 建库），在 `atlas_schema_revisions` 记账；服务
启动时经 `states.Open` 校验表已就位。

```go
// Phase 状态机（定义于 states，两阶段发布）：
//
//	pending ──claim──▶ dispatching ──publish ok──▶ dispatched ──finish──▶ terminal
//	   ▲                   │                             │
//	   └──requeue（发布失败/对账重派）◀──────────────────┘
// 注意与 queue 内部 task state（Redis backend）区分：那是执行视图，随 Redis
// 重启丢失；task_persistents 是调度事实源。

// SignatureUUID：task_{id}。同一行重派复用同一 UUID，执行框架按 UUID 覆盖
// 写状态，重派不残留旧记录。

// Ledger 记录已发布 Signature.UUID → taskID 的映射（Redis hash）。
// 仅是加速索引：Redis 无持久化，重启即丢，事实源是表里的 Phase+UpdatedAt，
// 对账不依赖 Ledger 存活。

// Reconciler 周期对账（entry 启动时先跑一轮，即启动恢复）：
//   在途行（dispatching 或 dispatched 超时）→ 查 queue backend 的任务终态
//     ├─ 已有终态（成功/失败）→ 按终态落表
//     └─ 无记录（Redis 丢/未执行）→ 回 pending（恢复通道重派）
//   随后 DispatchPending(nil)：全部 pending 行重新 dispatch 到队列
```

### 3.3 scheduler.go — 发布通道 Publisher（两阶段）

```go
// Publisher 是统一的发布出口，两类调用方：
//   CronScheduler：定时到点，Dispatch(batch)——持久化任务先 Create 落表
//     （幂等键冲突返回已有行，重复 tick 不重复发布），再 DispatchPending；
//     无状态任务直接经 dispatcher 发布。
//   Reconciler：DispatchPending(nil)——启动/对账恢复，把 pending 行
//     （崩溃残留与 requeue 行）重新 dispatch 到队列。
type Publisher struct{ repo, disp, reg, ledger, node }

// DispatchPending：ClaimPending（FOR UPDATE SKIP LOCKED，resources 空 =
// 全部资源）→ 逐行组装 Signature（RoutingKey 留空，headers 由表行的目标
// 节点属性生成，见 §3.4 的 Attribute 表；label 非法则组装失败、不发布）→
// dispatch.Dispatch（匹配节点表生成 queue:<resource>/<node>）→ 成功 ledger
// 记账，批量 MarkDispatched；失败 Requeue 回 pending。
```

### 3.4 dispatch/ — 发布器（节点匹配 + 分节点路由）

```go
// Dispatcher 是队列投递出口，不组装 Signature、不感知表行与状态流转。
// 另一类调用方：CronScheduler 的非 Durable 任务自组 Signature 直发，不过表。
type Dispatcher struct {
	server *queue.Server
	nodes  *nodeTable // 经 dm 服务拉取（内置客户端，见 dm.go），固定 TTL 缓存
}

func New(server *queue.Server, ci *cluster.ClusterInfo) *Dispatcher

// Dispatch：RoutingKey 为空时先 attachRoutingKey，已指定的直接投递；
// 返回本次投递的 AsyncResult（经 GetState 查询状态）。
func (d *Dispatcher) Dispatch(ctx context.Context, sig *queue.Signature) (*queue.AsyncResult, error)

// 持久化转发：headers 带 AttributePersistent 时不直投——去掉标记后把
// Signature JSON 编码进 persistent_forward 转发任务的 args 唯一 string Arg
//（{"signature": ...}），s2 资源为 persistents、携带 AttributeRoles=LEADER，
// 经同一路由只发布给 leader（dispatch/persistent.go）。

// attachRoutingKey 路由计算：Attribute headers 与节点表匹配出目标节点，
// 写入 queue:<resource>/<node>（resource 缺省取 Signature.Name）。
// 候选过滤：仅在线节点参与；AttributeVpcs 非空时节点 Vpcs 须与其有交集
//（节点未声明 Vpcs 不匹配）；AttributeRoles 非空时节点 Role 须在集合内；
// AttributeLabels 非空时每个键节点都须声明，且取值与约束有交集（节点未
// 声明标签不匹配）。
// 选点（pickNode，候选按 ID 排序后）：AttributeClaimedNode → AttributeTargetNode
// → 哈希取模（哈希键 AttributeHashBucket，未设置取 Signature UUID）；偏好的
// 节点不在候选内时顺延到下一级。无候选即拒绝发布（持久化行由调用方
// Requeue，恢复通道重派）。

// Attribute headers 键（路由依据，由 publisher 从表行写入）：
//   AttributeResource    ← 任务资源名（= row.Resource / spec.Resource）
//   AttributeVpcs        ← row.Vpc：可执行 vpc 环境，多值英文分号间隔
//   AttributeRoles       ← 限定执行的节点角色，多值英文分号间隔
//   AttributeLabels      ← row.Label 经 dispatch.ParseLabels 解析：map[string][]string
//   AttributeClaimedNode ← row.ClaimedNode：认领时写入的节点，选点最高优先
//   AttributeTargetNode  ← row.Node：用户指定的节点 ID，选点次优先
//   AttributeHashBucket  ← row.HashBucket（0 = 不限，不写）：选点散列键
//   AttributePersistent  ← 持久化标记：存在即由 Dispatch 转发给 leader 落表
```

### 3.5 scheduler.go — 事件总线（Listener）

```go
// Event 目前仅有 CronEvent（cron tick）。
type Event interface{ event() }
type CronEvent struct{ Type string }

// Listener 单事件循环：事件串行分发给全部已注册 Handler，慢 handler 阻塞
// 整个循环——调度逻辑必须非阻塞（发布走带超时的 ctx）。
// Emit 非阻塞投递，缓冲满即丢（定时事件丢了由下个周期补偿）。
```

### 3.6 scheduler.go — CronScheduler（cron 表达式）

内部 cron 对 `reg.Timed()` 中每个 spec 建 Entry（`TaskSpec.Cron` 原样传给
robfig/cron，五段表达式或 @every 等描述符），tick 时抢短 TTL Redis 防重锁
（TTL：五段 cron 固定 30s——最小粒度 1 分钟不会跨 tick 存活；@every 按周期
一半收缩以免节流亚分钟周期。多节点部署保证单节点触发，被锁挡掉的 tick 按
「下轮周期自然覆盖」丢弃）后发 CronEvent 给 listener。HandleEvent 按
Durable 分流：Durable → factory 生成批次，publisher 落表后立即发布；
非 Durable → 自组 Signature（无表行，headers 携带 AttributeResource，
RoutingKey 留空）经 dispatcher 直发。

### 3.7 entry.go — 组装与生命周期

实装为 `Config`（RedisAddrs/Repo/DMAddress/ReconcileInterval）+
`Run(ctx, cfg)`；本节点 ID 不在配置里，启动时经 dm 的
`ListClusterNodesStatus` 响应（`node_id`）获取。wire 装配集（`ProviderSet`/
`Runtime`/`NewSpecs` 与各 provider）与 `Run` 同在 `entry.go`。与伪代码的差异：
handler 包装为**成功内联回写**——执行成功
即 `MarkTerminal(succeeded)`（用脱离取消的 ctx，停机不丢最后一次回写）；
失败不内联回写，因为 queue 可能仍在重试（RETRY 状态），失败终态由对账按
backend 状态收敛。worker 启动用 `LaunchAsync(errCh)` 并排空错误通道。
启动顺序保持设计：绑定校验 → 经 dm 确认本节点 ID → 首轮对账（先于一切发布：
消化崩溃残留，把重启/停止前未执行完的任务重新 dispatch）→ worker →
listener/cron/reconciler；退出：ctx 取消 → 各循环退出 → server.Shutdown 停
worker。

### 3.8 tasks/persistents — 持久化转发消费侧

其他节点不必落表：按发布约定组装 Signature（Name=资源名、Args=[payload
JSON, taskID]、headers 携带 Attribute 路由属性）并标记 AttributePersistent
后经自己的 Dispatcher 发布。Dispatcher 将其包装为 `persistent_forward`
（资源 persistents、仅 leader），leader 侧本包：

1. 解码转发参数里的内层 Signature（JSON，UseNumber 保持整数精确），还原
   AttributeLabels 的 JSON 类型（map[string]any → map[string][]string）；
2. 映射为 task_persistents 行并落表（Id=第二 Arg taskID，缺省回退 UUID 协议
   task_{id}；幂等键=Signature UUID；Vpc/Node/HashBucket/Label 从 headers
   还原，供对账重派保持路由约束）；
3. `Claim(id)` 认领该行（pending→dispatching，只动指定行）；行已不在 pending
   即视为重复投递，不再重发；
4. 对齐执行侧协议（UUID=task_{id}、第二 Arg=id）后重新发布内层 Signature；
5. MarkDispatched 确认。重发失败 Requeue 并返回错误（队列重试/对账兜底），
   确认失败只记日志（发布已成功，不重发，由对账按执行终态收敛）。

本包不生产任务（无 Cron/Factory）；`TaskSpec.SignatureName` 使资源名
（persistents，路由）与 handler 注册名（persistent_forward）分离。

## 4. 决策记录

- **D1 发布语义：两阶段，发布成功才置 dispatched**。认领（pending→dispatching）与
  发布确认（MarkDispatched）分离；发布失败 Requeue，两阶段任一步卡住由对账回收。
  代价是至少一次投递，消费端按 IdempotencyKey 幂等。
- **D2 定时任务是否过表由任务配置决定**：`TaskSpec.Durable`——Durable 任务
  cron 到点经 factory 生成批次，落表后由 Publisher 立即发布；非 Durable
  直发 Redis（丢了下轮覆盖）。
- **D3 重试归属：执行内重试由 queue 框架承担**（`Signature.RetryCount =
  TaskSpec.Retries`）；task_persistents 不累计发布轮数（无 attempts 列），
  重派次数上限交由执行侧重试预算约束，重派仅在 Redis 状态丢失（崩溃）后发生。
- **D4 字段裁剪：框架表只保留调度所需**（task_persistents：阶段/终态/预算/
  幂等键/目标节点属性）。priority、attempts、callback、parent_task_id、
  biz_* 等水位补货与业务扩展字段随 StateScheduler 移除，全量字段表定义保留
  为业务表示例（docs/task_biz_example.md）。
- **D5 子消息（signature 派生链）可靠性暂不保证**：编排用 queue 原生
  OnSuccess/OnError/ChordCallback，接受其丢失窗口（失败有 error 日志可观测）；
  表内派生机制（callback 列 + 终态同事务派生）后置，需要时再实装。
- **D6 示例场景：资产巡检 fan-out**（cron 生成批次 + 落表发布 + 并发消费）
  曾以框架内置业务任务的形式实装，现已移除：框架不内置业务任务，示例场景
  在 `examples/` 内以场景自带任务定义自包含。
- **D7 队列划分：资源×节点静态约定**（原 QueueSpec/队列名注册已随
  StateScheduler 移除）。队列名统一由 `dispatch.QueueName(resource, node)`
  生成，发布与消费两侧共用，无独立注册点；消费并发随
  `TaskSpec.Concurrency` 声明（缺省 10）。
- **D8 完成回写：成功内联、失败走对账**。handler 包装在执行成功时立即
  `MarkTerminal(succeeded)`；失败不内联回写——queue 可能仍在重试（RETRY），
  失败终态由对账按 backend 的 FAILURE 状态收敛（延迟≈对账间隔）。
  taskID 经第二个 Arg 传入 handler（`func(payload string, taskID int64) error`），
  使包装无需反射。
- **D9 定时触发仅保留 cron（CronScheduler）**。固定周期（v0 的 Period 字段
  与 PeriodScheduler 设想）已移除，亚分钟节奏用 robfig/cron 的 @every
  描述符在 `TaskSpec.Cron` 内表达。
- **D10 任务自包含：新增任务只动 tasks/ 树**。消费并发随 TaskSpec 声明，
  路由由 Resource 经节点表推导，任务零框架注册。
- **D11 定时任务多节点防重：tick 前抢短 TTL Redis 锁**（SETNX，TTL 按表达式
  取值：五段 cron 固定 30s——最小粒度 1 分钟不会跨 tick 存活；@every 描述符
  按周期一半收缩，避免节流亚分钟周期）。被锁挡掉的 tick 按「下轮周期自然
  覆盖」语义丢弃；Durable 任务另有幂等键兜底。
- **D12 wire 作为 stateflux 整体的装配机制**（修订 D10 的清单机制）。
  每个任务包自带 `ProviderSet`（依赖经构造器注入，消费者侧小接口由接入方
  提供实现）；注册点收敛在根包 `entry.go` 的 `NewSpecs`（wire 无多绑定，
  清单是显式枚举）。组件构建全部经 wire：`InitializeRuntime(cfg)` 装配出
  `Runtime`，`Run` 只做生命周期编排。新增任务 = 在 tasks/ 下新建目录（含
  ProviderSet）+ entry.go 的 NewSpecs 加一行。
- **D13 持久化模型：dispatch 前落表 + 立即发布，移除 StateScheduler 与水位
  补货**（修订 v0 的「表是发布前的任务缓存、水位事件驱动认领」设计）。定时
  到点的批次落表后随即发布，表不再积压待认领任务；水位触发/CountPending/
  补货逻辑整体移除，队列规格收敛为消费并发声明。收益：状态迁移路径唯一
  （落表→发布→确认），无「表积压但队列空闲」的错配。
- **D14 恢复语义：启动 + 对账统一的重派通道**。Publisher.DispatchPending
  （types 空 = 全部类型）是 pending 行的唯一出口；entry 启动时首轮对账先于
  worker 启动执行，把重启/停止前未执行完的任务（pending/dispatching/
  dispatched）重新 dispatch 到任务队列；周期对账以同一机制消化运行期故障
  （Redis 丢失、发布卡住）。非终态行一律重派，无死信判定（D3）。
- **D15 恢复期任务全部由本表自持**：task_persistents 行与 Signature 的关联
  协议是 `SignatureUUID = task_{id}`，重派复用同一 UUID（执行框架按 UUID
  覆盖写状态），恢复不依赖 Redis 中的任何记录（Ledger 仅为加速索引）。
- **D16 分节点路由：queue:<resource>/<node>**。TaskPersistent.Type 更名
  Resource（即路由键）；Signature 不再携带静态 RoutingKey，发布前由
  dispatch 以 AttributeResource/AttributeVpcs/AttributeRoles 匹配节点表
  （在线 + Vpcs 交集 + Role 包含）选定目标节点生成路由，多节点部署下每
  节点只消费自己的队列（对齐旧 schedule 的 {nodeId}/{partition} 模型，
  去掉副本队列与水位注册），单节点退化为 queue:<resource>/<self>。节点表
  由 dispatch 内置拉取（dm 服务，TTL 缓存），Node.Vpcs 待 dm 节点属性扩展
  后填充；带 vpc 约束的任务在节点表补充属性前不匹配节点（拒绝发布）。
- **D17 选点：约束过滤 + 三级偏好**（扩展 D16 的「哈希取模选定」）。候选 =
  在线且满足 AttributeVpcs（Vpcs 交集）、AttributeRoles（Role 包含）、
  AttributeLabels（标签按键交集：每个键节点都须声明，且取值与约束有交集）
  的节点，按 ID 排序；pickNode 依次取 AttributeClaimedNode（claimed_node）、
  AttributeTargetNode（node，用户指定）、哈希取模（哈希键
  AttributeHashBucket 即 hash_bucket，0 = 不限时取 Signature UUID），偏好的
  节点不在候选内时顺延到下一级，不拒绝发布。表行的 label 书写为
  `key=value;…`（同键可重复），构造 Signature 时经 dispatch.ParseLabels
  解析为 AttributeLabels，解析失败不发布。两点现状：claimed_node 目前由
  ClaimPending 在认领时写入认领它的发布节点，尚未回写 dispatch 选定的
  执行节点；Node.Labels 与 Node.Vpcs 同样待 dm 节点属性扩展后填充，填充前
  带标签约束的任务不匹配节点（拒绝发布）。
- **D18 持久化转发：AttributePersistent → persistent_forward（仅 leader）**。
  其他节点只经 Dispatcher 提交持久化签名：带标记的 Signature 不直接投递，
  而是去掉标记后编码进 persistent_forward 的 `{"signature": ...}` 参数，以
  AttributeRoles=LEADER 只发布给 leader；leader 侧的 tasks/persistents 先
  按发布约定落表（幂等键=Signature UUID，ID 取第二 Arg/UUID 协议，路由属性
  从 headers 还原），再认领并解码重发内层签名，最后确认 dispatched。DB 写入
  因此收敛在 leader，生产方无需接触 task_persistents，也无需第二套雪花节点
  （ID 由生产方分配）。转发任务本身非 Durable（避免自指），瞬时故障由队列
  重试与对账恢复通道兜底；请求方可经 Dispatch 返回的 AsyncResult 查询转发
  状态。`TaskSpec.SignatureName` 为此把资源名（路由）与 handler 注册名分离
  （persistents vs persistent_forward）。

### 后续待做（无待决项）

调度框架已全部实装（见 §2 layout）。剩余：服务合并进 cmd/wire；业务侧
（其他进程）提交任务的跨进程入口（见 §5.3 G1）。

## 5. 旧 schedule 需求承载性评估（2026-11 调研）

调研范围：`backend/internal/schedule/` 全量（30 个 callback 域、约 80 个
method、10 个 factory、35 个队列分区、task/sched_jobs 两级表）与
`schedule2-rewrite-plan.md`。结论：**调度骨架可承载且优于旧实现；
三个真实缺口（其中 G3 与旧设计同为 TBD）、两项分层澄清**。

### 5.1 已覆盖（机制对等或更强）

| 旧需求 | stateflux 对应 | 备注 |
|---|---|---|
| 24 个代码内置周期任务（不落库） | TaskSpec.Cron（@every 描述符）+ Durable=false 直发 | 逐个翻译为 tasks/ 目录即可 |
| DB 定时任务（task 表 CronExpr/Mode） | 业务侧维护 task 表，factory 扫表生成持久化批次，落表后立即发布 | 类型级（TaskSpec）与实例级（用户建的任务）分层，factory 是桥梁 |
| 在途数限流（CountDetached/MaxDetached=1000，每轮 200） | tick 批量上限（FactoryBatch）+ 队列消费端 Concurrency | 无独立水位机制，投递量由触发批次约束 |
| 派发 backoff（2s×1.3³） + 消费重试（RetryCount 斐波那契） | queue RetryCount + 对账重派 | 对账按行 timeout_ms 收敛，优于旧的 30min/24h 粗超时回收 |
| 崩溃恢复（PENDING 30min/RUNNING 24h 回收 + 启动堆积恢复） | 启动首轮对账（D14）+ 周期对账（行级预算 + grace） | 显著增强 |
| 分区并发（35 分区 × 每分区 Concurrency/副本数） | 资源×节点队列（queue:<resource>/<node>）+ TaskSpec.Concurrency，worker 消费本节点队列 | 对齐旧 {nodeId}/{partition} 模型，去掉副本队列 |
| 无显式优先级（靠分区隔离） | 触发批次内按任务类型自然有序；如需优先级在业务表示例列（docs/task_biz_example.md）扩展 | 调度侧不再按优先级认领 |
| leader 单点跑 factory | 定时触发单节点化（D11 锁） | 去掉 dm leader 依赖 |
| mail/file/zip/backup 通用能力重复 | 任务目录内聚实现（tasks/mail/ 等），公共部分升 tasks/ 核心包 | 迁移时消除重复 |

### 5.2 分层澄清（不是调度层职责）

- **目标节点定向（VPC 绑定/一致性哈希/LEADER label）**：旧实现是 callback
  内经 gRPC 二次分发到 worker 节点（12 个 Do* RPC，本地 unix socket / 集群
  隧道）——定向执行在 **handler 层**实现（handler 内 gRPC 调 worker 并自带
  路由），调度层只管投递。task_persistents 的 vpc/node/label/hash_bucket
  列随 Signature headers 投递；调度层定向已由 dispatch 选点启用（D17）。
- **业务进度计数（Total/Current/Success/Failure）与用户任务管理 API**：
  留在业务表（task 等）与 apiserver，stateflux 只管调度事实。业务表
  建表参照 docs/task_biz_example.md。

### 5.3 缺口（按严重度）

- **G1 跨进程提交/唤醒**（结构性，合并期必须决策）：旧模式是 apiserver/dm/
  console/vault 四进程内嵌 dispatcher 直写 Redis 发任务与 wake 消息
  （不走 RPC）。stateflux 当前是闭环进程。候选方案：
  (a) schedule 暴露 gRPC 提交接口，收口单一写入方；
  (b) 提取轻量 producer 包供其他进程内嵌（打破「不被其他模块引用」约束，
      且生产方绕过发布通道，需要直接落表或接受 wake-only）；
  (c) 过渡期沿用旧 dispatch 包与新框架并存。
  本模型下生产方只需落表（pending 行由恢复通道自动带走），G1 的方案 (b)
  复杂度低于 v0（无需实现 wake）。
- **G2 业务并发约束**（chpass 类强需求）：同主体互斥（UniqueEntryRunning）、
  资源配额（ResourceLimits 按 label 计数）、同资产限流（SameLess）、保序
  重试（blockBuffer 前端插回）。框架表已不含 biz_* 预留列（D4），此类约束
  属业务层——迁移 chpass 前在业务表/handler 侧实现，或按需回到框架扩展。
- **G3 编排工作流（workflow/password_workflow）的多阶段接力**：旧实现是
  消息链 + 状态机（continue_workflow_stage 等 5 个 method）。D5 已决
  「子消息可靠性暂不保证」，此类「跨终态派生」业务在 D5 解冻前只能靠
  Durable 任务 + factory 扫表接力（过表轮询替代消息链），或暂缓迁移。
