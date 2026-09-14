# stateflux 设计 · 数据模型与状态机（§3–§4）

> v3.21（2026-09-14）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 3. 数据模型

### 3.1 PG 任务账本

四阶段 Pending / Schedulable / Processing / Completed 是 `internal/domain` 暴露的逻辑集合，PG 四张
阶段表为默认实现。业务在自己的本地事务内调用 `stateflux.enqueue_tasks(...)`；业务 role 仅有该函数
的 EXECUTE 权限与结果查询权限，内部表只由服务账号写入。

**任务行模型：公共段 + 阶段段。** 公共段 19 列四表一致：`id`、`type`、`operator`、`priority`、
`channel`、`vpc`、`node`、`label`、`timeout_ms`、`max_attempts`、`idempotency_key`、
`biz_race_labels`、`biz_race_entry`、`biz_group`、`biz_batch_id`、`callback`、`parent_task_id` 与
时间戳，公共列增删必须四处同步。阶段段按阶段语义取舍：

- `attempts`、`claimed_node` 存在于 schedulable/processing/completed——claim 才产生，从未被认领的
  pending 行恒为零值；
- `hash_bucket` 存在于 pending/schedulable/processing——晋升与认领时消费完毕，不入终态历史；
- `error` 仅存在于 completed——终态错误摘要，完整结果在 task_results；
- `run_at` 已随 v3.21 **全链退役**：调度（晋升与认领）不判断时间门槛，延迟任务语义移除，
  R1 重置后立即重新可认领（无退避，§5.5）。

四阶段字段矩阵（✓ = 有该列，— = 无；行序即四表 DDL 列序，✓ 全勾的 19 行为公共段）：

| 字段 | task_pendings | task_schedulables | task_processings | task_completeds | 语义 |
| --- | --- | --- | --- | --- | --- |
| `id` | ✓ | ✓ | ✓ | ✓ | 任务 ID（雪花，客户端生成，非自增） |
| `type` | ✓ | ✓ | ✓ | ✓ | 任务类型：决定 handler 与路由 |
| `operator` | ✓ | ✓ | ✓ | ✓ | type 的子类型；completed 保留终态行 |
| `priority` | ✓ | ✓ | ✓ | ✓ | 0–100 连续区间，越大越优先（晋升/认领排序键） |
| `channel` | ✓ | ✓ | ✓ | ✓ | 逻辑 channel 名；completed 为审计快照 |
| `vpc` | ✓ | ✓ | ✓ | ✓ | 调度目标节点属性：目标网络域（VPC 名） |
| `node` | ✓ | ✓ | ✓ | ✓ | 调度目标节点属性：期望执行节点 ID |
| `timeout_ms` | ✓ | ✓ | ✓ | ✓ | 单次执行预算(ms)；completed 审计 |
| `max_attempts` | ✓ | ✓ | ✓ | ✓ | 最大认领次数，超限由 R3 写死信；completed 审计 |
| `attempts` | — | ✓ | ✓ | ✓ | 已认领次数（claim 时 +1）；completed 为终态最终值 |
| `claimed_node` | — | ✓ | ✓ | ✓ | 认领节点（上次/本次/最后，诊断与审计用） |
| `error` | — | — | — | ✓ | 终态错误摘要；完整结果在 task_results |
| `idempotency_key` | ✓ | ✓ | ✓ | ✓ | 业务幂等键；唯一性由 task_identities 裁决 |
| `callback` | ✓ | ✓ | ✓ | ✓ | OnSuccess/OnError 回调规格 jsonb |
| `parent_task_id` | ✓ | ✓ | ✓ | ✓ | 回调派生来源的任务 ID |
| `label` | ✓ | ✓ | ✓ | ✓ | 调度目标节点属性：节点匹配标签（晋升/认领时过滤；completed 审计） |
| `hash_bucket` | ✓ | ✓ | ✓ | — | 调度目标节点属性：0–255 分桶（晋升/认领时过滤，不入终态） |
| `biz_race_labels` | ✓ | ✓ | ✓ | ✓ | 业务并发约束：竞争约束标签集合（字符串数组，空=不限） |
| `biz_race_entry` | ✓ | ✓ | ✓ | ✓ | 业务并发约束：业务对象唯一标识（同 entry 施加并发约束） |
| `biz_group` | ✓ | ✓ | ✓ | ✓ | 业务属性：业务分组键（框架不解释语义） |
| `biz_batch_id` | ✓ | ✓ | ✓ | ✓ | 业务属性：工厂批次 ID（工厂孤儿判定，§5.6） |
| `created_at` | ✓ | ✓ | ✓ | ✓ | 创建时间（不可变）；completed 兼分区/归档键 |
| `updated_at` | ✓ | ✓ | ✓ | ✓ | 最后更新时间；pending/schedulable/processing 为 R1 对账扫描依据 |
| `outcome` | — | — | — | ✓ | 专有：终态类别数值枚举（0=未设置 1=succeeded 2=failed 3=dead） |
| `completed_at` | — | — | — | ✓ | 专有：终态写入时间（分区/归档键） |

**任务内容与路由。** `type` 是任务类型，`operator` 是它的**子类型**：`type` 粗、`operator` 细，
二者共同标识"做什么"，并一起决定服务构建时注册的 handler 选择与路由（§1.2.8、§5.1）——新增一种
子操作只加 `operator`，不必新增 `type`；两列都是自由文本、非枚举。

**调度参数。** `priority` 是**连续区间** `[0,100]`（`Int8`，默认 50，**不是枚举**）：业务按自己的
语义细分，0 最低、100 最高，调度侧只依赖排序、不解释具体数值；区间由各表的 CHECK 约束钉住
（`PriorityCheck`），越界值写不进来。晋升与认领均按该列 DESC 排序（§5.2），因此 100 最先被处理。
`channel` 是逻辑 channel 名（§3.2）——非空、不设列默认值（默认 channel 由装配/配置补齐，§14.2），
调度侧据此解析实现。`timeout_ms` 是单次执行的预算（§5.4）。

**调度目标节点属性（vpc / node / label / hash_bucket）。** 描述任务期望由什么样的节点执行，
创建时指定、对框架业务无关：`vpc` 为目标网络域、`node` 为期望执行节点，取值来自集群视图
（VPC 名与节点 ID，§5.3）；`label` 是节点匹配标签（自由文本、空串=不限、框架不解释语义）；
`hash_bucket` 是 0–255 的整数分桶（0=不限、接入方自定义语义、框架只做等值匹配，区间由
`BucketCheck` 钉住）。`vpc`/`node`/`label` 是**外部标识的自由文本，不是枚举、不做数值编码**，
空串表示不限制。四列均由调度侧在晋升与认领时过滤（§5.2/§5.3），执行侧不参与决策、也不得据此
改变执行行为（§1.2.7）；`label` 随行保留至终态（completed 审计），`hash_bucket` 在认领消费后不入
终态历史。它们与 `claimed_node`（认领节点，诊断用）语义不同：重派时 `node` 保持不变。

**业务属性（biz_race_labels / biz_race_entry / biz_group / biz_batch_id）。** `biz_group` 是业务
分组键：供上层业务按组查询/聚合（例如"某业务批次还剩多少在途"），框架不解释语义、
也不参与调度正确性；`biz_batch_id` 是工厂批次（业务写入、框架仅用于孤儿判定，§5.6），与
`biz_group` **不是一回事**。`biz_race_labels`（字符串数组）与 `biz_race_entry`（字符串）是**业务
并发约束**：`biz_race_entry` 唯一标识业务对象、`biz_race_labels` 声明其参与的约束标签集合，由
调度侧据此实施并发控制——控制机制待定（§14）。要按组限流应使用 race 约束或显式定义独立机制，
而不是顺手复用 `biz_group`。注意：`operator` 是 PostgreSQL 关键字，手写 SQL（`enqueue_tasks`、
认领语句）必须写成 `"operator"`；ent 与 Atlas 生成的 SQL 会自动加引号。

**终态编码与命名约定。** `outcome` 是**封闭枚举**，以整型存储：1=succeeded、2=failed、3=dead
（§5.5、§6.2 R3），0 保留为「未设置」，使 Go 零值与漏赋值可检测。`attempts` 是已认领次数
（claim 时 +1），`attempt` 是本次执行的 fence 值——claim 后二者相等，信封与结果行沿用
`attempt`（§3.3、§5.5）。编码与区间都是存储契约，改动等同数据迁移；常量与表定义同包
（`internal/domain/schema`），不得下沉到 `internal/domain`（ent 生成码反向 import 本包，会成环）。

**幂等与诊断。** `idempotency_key` 仅为溯源与回调派生保留，唯一性由 `task_identities` 账本裁决
（§5.1），任务行上的局部唯一索引方案已随 v3.18 退役；项目暂不引入 tenant，`idempotency_key` 在
整个 stateflux 实例内唯一，接入方必须使用全局唯一前缀。

| 表 | 用途 |
| --- | --- |
| `task_identities` | `idempotency_key → task_id` 的跨阶段身份账本；dedupe 窗口内重复创建返回原 ID。 |
| `task_payloads` | 在途 payload；终态时合并入 task_results。 |
| `task_pendings` / `task_schedulables` / `task_processings` / `task_completeds` | 四阶段集合。 |
| `task_results` | `task_id` 主键的不可变终态结果。 |

claim 是一个 PG 事务：`SKIP LOCKED` 选候选 → `schedulable → processing` → `attempts + 1`。本次
`attempt` 是执行 fence，**只在 claim 加一**；重试/R1 重置不加。终态、重置和死信验证 `attempt`。
前置条件只能读取不变任务字段或受限快照，不得 I/O 或写状态。

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
trace_headers）和 `ResultEvent`（task_id、attempt、outcome、result/error、source）。
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
