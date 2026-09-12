# stateflux 设计 · 数据模型与状态机（§3–§4）

> v3.20（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 3. 数据模型

### 3.1 PG 任务账本

四阶段 Pending / Schedulable / Processing / Completed 是 `internal/domain` 暴露的逻辑集合，PG 四张
阶段表为默认实现。业务在自己的本地事务内调用 `stateflux.enqueue_tasks(...)`；业务 role 仅有该函数
的 EXECUTE 权限与结果查询权限，内部表只由服务账号写入。

任务行包含 `id`、`type`、`operator`、`priority`、`channel`、`vpc`、`node`、`run_at`、`timeout_ms`、
`max_attempts`、`attempts`、`owner_node`、`control_epoch`、`error`、`idempotency_key`、`group`、
`batch_id`、`callback`、`parent_task_id` 与时间戳。
`type` 是任务类型，`operator` 是它的**子类型**：`type` 粗、`operator` 细，二者共同标识"做什么"，
并一起决定服务构建时注册的 handler 选择与路由（§1.2.8、§5.1）——新增一种子操作只加 `operator`，
不必新增 `type`；两列都是自由文本、非枚举。
`group` 是**业务分组键**：供上层业务按组查询/聚合（例如"某业务批次还剩多少在途"），框架只建索引、
不解释语义，也不参与调度正确性。它与 `batch_id`（工厂批次，用于孤儿判定，§5.6）**不是一回事**，
也**不等于**并发名额的 `scope_key`（后者仍待定，§14.6）——要按组限流应在 scope 机制里显式定义，
而不是顺手复用 `group`。
注意：`group` 是 PostgreSQL 保留字（`operator` 同为关键字），手写 SQL（`enqueue_tasks`、认领语句）
必须写成 `"group"` / `"operator"`；ent 与 Atlas 生成的 SQL 会自动加引号。
`channel` 是逻辑 channel 名（§3.2）——非空、不设列默认值（默认 channel 由装配/配置补齐，§14.2），
调度侧据此解析实现；`timeout_ms` 是单次执行的预算（§5.4）。
`priority` 是**连续区间** `[0,100]`（`Int8`，默认 50，**不是枚举**）：业务按自己的语义细分，
0 最低、100 最高，调度侧只依赖排序、不解释具体数值；区间由各表的 CHECK 约束钉住，越界值写不进来。
晋升与认领按该列 DESC、run_at ASC 排序（§5.2），因此 100 最先被处理。
`outcome` 是**封闭枚举**，以整型存储：1=succeeded、2=failed、3=dead（§5.5、§6.2 R3），0 保留为
「未设置」，使 Go 零值与漏赋值可检测。编码与区间都是存储契约，改动等同数据迁移；常量与表定义同包
（`internal/domain/schema`），不得下沉到 `internal/domain`（ent 生成码反向 import 本包，会成环）。
`vpc` 与 `node` 是**创建时指定的、业务无关的调度约束**：`vpc` 为目标网络域、`node` 为期望执行节点，
二者是**外部标识的自由文本，不是枚举、不做数值编码**（取值来自集群视图：VPC 名与节点 ID，§5.3），
空串表示不限制；由调度侧在晋升与认领时过滤（§5.2/§5.3），执行侧不参与决策、也不得据此改变执行行为
（§1.2.7）。它们与 `owner_node`（实际认领节点，诊断用）语义不同：重派时 `node` 保持不变。
命名约定：`attempts` 是已认领次数（claim 时 +1），`attempt` 是本次执行的 fence 值——claim 后二者
相等，信封与结果行沿用 `attempt`（§3.3、§5.5）。
字段角色：`control_epoch` 只记录 claim 时的控制面 epoch，供诊断与指标使用，**不参与任何校验**
（§6.2 明确不得要求行内历史 epoch 等于当前 epoch）；`idempotency_key` 仅为溯源与回调派生保留，
唯一性由 `task_identities` 账本裁决（§5.1），任务行上的局部唯一索引方案已随 v3.18 退役。
项目暂不引入 tenant：`idempotency_key` 在整个 stateflux 实例内唯一，接入方必须使用全局唯一前缀。

| 表 | 用途 |
| --- | --- |
| `task_identities` | `idempotency_key → task_id` 的跨阶段身份账本；dedupe 窗口内重复创建返回原 ID。 |
| `task_payloads` | 在途 payload；终态时合并入 completed。 |
| `pending_tasks` / `schedulable_tasks` / `processing_tasks` / `completed_tasks` | 四阶段集合。 |
| `task_results` | `task_id` 主键的不可变终态结果。 |
| `concurrency_reservations` | `scope_key` 的在途计数与上限；claim 时条件预留，终态/重置时释放。 |
| `control_leases` | 控制面 scope 的 owner、expiry、单调 epoch；外部选举只提供候选资格。 |

claim 是一个 PG 事务：验证当前 control epoch → `SKIP LOCKED` 选候选 → 条件预留名额 →
`schedulable → processing` → `attempts + 1`。本次 `attempt` 是执行 fence，**只在 claim 加一**；重试/R1
重置不加。终态、重置和死信验证 `attempt`，终态操作验证当前 control lease。前置条件只能读取不变任务
字段或受限快照，不得 I/O 或写状态。

### 3.2 EventBus 与 Channel

`internal/eventbus` 是任务和结果的统一通信面：`task.{band}` 与 `result` 是逻辑 topic；它支持
`Send`（发布）和 `Subscribe`（订阅）。`{band}` 是 `priority` 数值的**派生档位**而非原始值
——low 0–33、normal 34–66、high 67–100，共 3 个 topic：priority 是连续区间，若直接用数值做
topic 会产生上百个主题，而 topic 只用于订阅与投递分组；精确优先顺序仍由 PG 的 `priority` 排序
决定（§5.2），档位不参与正确性。EventBus 不承诺持久化、至少一次或恰一次；调用者必须接受
**丢失、重复、乱序，以及“返回发送失败但实际已送达”**。PG processing + timeout/grace + 对账是唯一
恢复机制。

`internal/eventbus/channel` 定义能力而非绑定中间件：

| channel 能力 | 语义 | 实现例子 |
| --- | --- | --- |
| 单向 publish/subscribe | 发送不带结果；订阅端随后发布 ResultEvent | Redis pub/sub、list、zset、stream |
| 半双工 request/reply | 先发送请求、再读取一个结果；调用方阻塞等待 | unary RPC、Redis list 组合 |
| 全双工 stream | 两端可并发 Send/Subscribe；适合长连接结果汇聚 | gRPC bidirectional stream、Redis pub/sub |

每个实现声明 `ChannelCapabilities`（是否订阅、是否请求应答、是否全双工、是否提供本地 ack）。ack 是
实现级流控信号，**永不构成任务可靠性条件**。Redis 的 list/zset/stream/pubsub 可全部实现 channel；即使
某实现有 AOF、PEL 或 XACK，框架也不依赖它们。默认结果 channel 是调度节点和执行节点之间的 gRPC
双向 stream：worker 发布 `ResultEvent`，Collector 订阅；它可替换成 Redis channel 而不改变状态机。

### 3.3 信封

`api/stateflux/task/v1` 定义不可变 `TaskMessage`（task_id、attempt、type、payload、priority、deadline、
trace_headers、control_epoch）和 `ResultEvent`（task_id、attempt、outcome、result/error、source）。
同步 RPC channel 的响应适配为 `ResultEvent` 并送入同一 EventBus；异步 channel 的 `Send` 只表示已尝试
发送，结果必须由执行节点另行发布。大 payload/result 使用对象存储引用。

## 4. 状态机与可靠性

```
pending → schedulable → processing ──ResultEvent（attempt 匹配）→ completed
               ▲              │
               └─ retry / R1 ─┘  （不加 attempts；下次 claim 才 +1）
```

Channel 故障、Redis 丢失、RPC 超时或结果事件丢失都只会让 processing 在 grace 后重置并重跑；业务 handler
必须按 `idempotency_key` 或 task_id 保证副作用幂等。Redis 仅缩短正常路径延迟，完全不可用时框架仍由
PG 对账收敛。
