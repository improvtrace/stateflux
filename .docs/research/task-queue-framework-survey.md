# 任务队列框架调研：Machinery / Asynq / Hatchet / Temporal / Celery

> 调研日期：2026-09-08。信息来源：各框架官方 GitHub、官方文档（docs.temporal.io、docs.hatchet.run、docs.celeryq.dev、asynq wiki）与公开资料，逐项核实，不确定处标注"未确认"。
> 语境：为 stateflux（Go + PG + Redis 的分布式任务框架）设计提供横向参照。

## 1. 结论速览

| 框架 | 语言 | 一句话定位 | 状态事实源 | 部署形态 | 活跃度（2026-09） |
| --- | --- | --- | --- | --- | --- |
| **Machinery** | Go | Go 任务队列鼻祖，多 broker + 最完整编排原语 | ResultBackend（Redis/Memcache/Mongo/AMQP） | Server/Worker 分离 + 独立 broker | ⚠️ 维护放缓（最后提交 2025-11，release 2025-08） |
| **Asynq** | Go | 只依赖 Redis 的单二进制任务队列，运维闭环最全 | Redis（List+ZSET+Hash，Lua 原子迁移） | 单二进制嵌入（Server 即 Worker） | ✅ 活跃（v0.26.0，2026-02） |
| **Hatchet** | Go/TS/Py/Ruby | Postgres 之上的任务队列 + 轻量 durable workflow | PostgreSQL（事务化状态转换） | API Server + Engine + Worker（v1 起 Postgres-only，RabbitMQ 可选） | ✅ 活跃（YC W24，v1 2025-03 发布） |
| **Temporal** | 8 门官方 SDK | 重型 Durable Execution 平台（事件溯源 + 确定性重放） | DB（Cassandra/PG/MySQL）中的 Event History | 4 层服务（Frontend/History/Matching/Worker）+ DB（+可选 ES） | ✅ 最成熟（22.9k stars，MIT，Temporal Cloud） |
| **Celery** | Python | Python 生态事实标准的分布式任务队列 | Broker + 独立 ResultBackend | Producer → Broker → Worker → Backend 四段式 | ✅ 活跃（v5.6.3，2026-03） |

**选型一句话**：要轻、要运维工具、Go 单机起步选 Asynq；要多 broker 或 chain/chord 编排（接受维护风险）选 Machinery；只要一个 Postgres、重公平调度/AI Agent 场景选 Hatchet；要最强持久化保证与长周期复杂编排、能承受部署与确定性编程模型成本选 Temporal；Python 项目默认 Celery（可靠性需显式配置）。

---

## 2. Machinery（RichardKnop/machinery）

### 2.1 架构设计

- **核心抽象**：Task（`RegisterTask` 注册的普通 Go 函数，反射调用，JSON 序列化）；**Server（生产端）与 Worker（消费端）角色分离**是其标志性设计；Broker / ResultBackend / Lock 三个接口完全解耦，可自由组合（如 SQS + MongoDB）。
- **状态流转**：`PENDING → RECEIVED → STARTED → (RETRY) → SUCCESS / FAILURE`，每次变化序列化写入 ResultBackend；Group 有 `GroupMeta`（含 `ChordTriggered`）支撑 chord 回调。
- **v2 重构**：废弃工厂模式，改为构造函数注入（`NewServer(cnf, broker, backend, lock)`），broker/backend 拆为独立包；支持 YAML 配置 + 每 10s 热加载；内置 Eager 模式（进程内同步执行，测试用）。

### 2.2 后端与特性

- **Broker**：AMQP / Redis / AWS SQS / GCP Pub/Sub。**ResultBackend**：Redis / Memcache / MongoDB / AMQP（官方自评 AMQP 后端高并发下不可靠，劝退）。
- **特性**：重试（次数 + 斐波那契退避，可 `ErrRetryTaskLater` 指定延迟）；cron 周期任务（依赖分布式 Lock 接口）；ETA 延迟；**Chain / Group / Chord / Workflows——Go 任务队列中编排能力最完整**；Worker 并发数限制；OpenTracing。
- **缺失**：无任务优先级、无死信状态、无任务取消（issue #274 自 2018 年 open 至今）、无内置去重、无官方 Web UI、无独立 per-task 超时；`registeredTasks` 并发安全 bug（#606）2020 年至今未修。

### 2.3 成熟度

7,973 stars；约 2015-2016 年诞生；248 open issues；最后 release v2.0.16（2025-08），最后提交 2025-11，**维护明显放缓**，文档陈旧（部分 wiki 停留 v1）。历史生产验证多，新项目正被 Asynq/River 等替代。

### 2.4 优缺点

- ✅ 多中间件；编排原语最全；接口化设计易扩展；Server/Worker 分离清晰。
- ❌ 维护放缓、文档陈旧；缺优先级/死信/取消/去重/UI；反射 + JSON 性能一般；部分结果后端不可靠；长期 open bug。

---

## 3. Asynq（hibiken/asynq）

### 3.1 架构设计

- **核心抽象**：Client（入队）/ Server（内嵌 goroutine worker 池，`server.Run(handler)` 单二进制运行）/ Task（type 字符串 + payload）。at-least-once 投递。
- **Redis 数据结构（设计亮点）**：每队列用 hash tag 固定 Cluster slot；**List**（pending/active）+ **ZSET**（scheduled/retry/archived，score=到期时间）+ **Hash**（任务本体）；**所有状态迁移用 Lua 脚本原子完成**；Worker 以 **lease 租约**认领任务，崩溃后租约到期自动恢复重投。
- **状态流转**：`Pending → Active → (Scheduled → Retry) → Completed / Archived`。**Archived（重试耗尽，默认 MaxRetry=25）充当死信存储**，UI/CLI 可查看、重跑、删除。

### 3.2 特性

- 重试：指数退避 + 抖动，自定义 RetryDelayFunc；定时任务：Scheduler 组件（cron + 时区，Schedule ID 防重复注册）；延迟：`ProcessIn/ProcessAt`。
- 优先级：多队列 + Weighted/Strict 两种消费模式；超时：per-task Timeout/Deadline；**去重**：`Unique(ttl)` 与 `TaskID` 唯一冲突报错——任务队列里少见的内置幂等原语。
- 编排：无 chain/chord；有 **Group 聚合**（GroupAggregator + grace period/max size/max delay，把小任务聚合为批处理任务，非 DAG）。
- 结果：fire-and-forget 为主；v0.19+ `ResultWriter` 写结果 + `Retain` 保留 completed 任务 + Inspector 轮询读取；**无原生同步等待结果**（issue #265 长期讨论，官方建议 Handler 写 Redis + 客户端轮询/pubsub）。
- 运维闭环：Inspector 程序化 API（暂停队列/重跑/删除）、官方 CLI（stats/dash）、官方 Web UI **asynqmon**、`asynq/x`（Prometheus metrics、限流）、链式中间件。

### 3.3 成熟度

13,684 stars；v0.26.0（2026-02），持续活跃；MIT；Go 生态 Redis 任务队列的事实首选；已知问题：任务卡 active 的个案（#286）、强绑定 Redis。

### 3.4 优缺点

- ✅ 单二进制 + 只需 Redis，部署极简；可靠性设计扎实（lease 恢复 + Lua 原子迁移 + at-least-once）；死信归档、去重、优先级、超时、UI/CLI/Inspector 齐全；活跃维护。
- ❌ 仅 Redis（可用性/容量与 Redis 绑定）；无编排原语（chain/chord/DAG）；无原生同步取结果；asynqmon 迭代放缓。

---

## 4. Hatchet（hatchet-dev/hatchet）

### 4.1 架构设计

- **定位**：2023 年创立（YC W24），"Postgres 之上的任务队列 + 工作流编排"，近年重点面向 AI Agent / LLM 批处理。开源引擎 + hatchet.run 云服务（open-core），MIT。
- **核心抽象**：Task（重试/超时策略）、声明式 **Workflow/DAG**、**Durable Tasks**（v1：durable task 只做"等待（时间/事件）"与"派生子任务"两类操作，官方定位为 Temporal/DBOS 的轻量替代）、Events/Webhooks 触发、Cron/Schedules。
- **组件**：API Server（HTTP 入口）+ Engine（依赖评估、调度、事务化状态转换；无状态可水平扩展）+ Worker（多语言，与引擎双向 gRPC，拉取任务回报结果）。
- **持久化**：**PostgreSQL 是唯一事实来源**（定义、状态、输入输出、重试、队列都在 PG，事务化执行状态转换）；**v1（2025-03）移除 RabbitMQ 必需依赖，默认 Postgres-only**（"Hatchet Lite" 只要一个 PG），RabbitMQ 降为可选高吞吐组件；支持 Embedded Mode（引擎嵌入 worker 进程）。默认 FIFO，可配按键公平调度。

### 4.2 特性

- 重试（指数退避）、任务超时、cron、延迟任务、**按键公平队列（v1 主打，防单租户占满）**、全局/按键并发策略 + rate limit、**条件 DAG**（分支可绑定"父输出>50"/"timer 到期"/"事件到达"混合条件）、事件 + webhook 原生触发、Sticky Assignment / Worker Affinity / Task Slot Cost（面向 GPU 等重资源 worker）、官方 Web UI、OpenTelemetry + Prometheus。
- 幂等：at-least-once 语义，任务代码须幂等（有输入去重 key，细节未确认）；无 Temporal 式确定性重放；灾备依赖 PG 自身 HA，无原生多集群复制（未确认）。
- SDK：Python / Go / TypeScript 主力 + Ruby；Java/PHP/.NET 无官方 SDK。

### 4.3 成熟度与局限

7,899 stars，MIT；单 PG 未分片时吞吐有限（每引擎实例数百 task/s，官方自述调优后可达数万/s）；durable execution 语义比 Temporal 浅；生态与超大规模案例远不如 Temporal；v0/v1 文档并存易混淆。

### 4.4 优缺点

- ✅ 一个 Postgres 即可起步、运维成本低；公平调度/并发控制/AI Agent 场景针对性强；条件 DAG + durable tasks 兼具队列与轻量 durable execution；事件/webhook 一等公民。
- ❌ 吞吐上限受 PG 制约；无确定性重放、保证级别弱于 Temporal；语言覆盖窄；灾备/多集群方案缺失；生态年轻。

---

## 5. Temporal（temporalio/temporal）

### 5.1 架构设计

- **定位**：通用 **Durable Execution** 平台（源自 Uber Cadence），适合长事务、Saga、跨服务编排等关键业务。
- **核心抽象**：**Workflow（必须确定性）+ Activity（承担所有非确定副作用，独立重试/超时）**；Signal / Query / Update / Child Workflow / Timer / Continue-as-New / Nexus（跨服务操作）。
- **服务四层（可独立扩展）**：Frontend（无状态网关、限流）→ **History（核心：按 shard 约 2k 个哈希分片，串行化每个 workflow 的 Event History 写入）** → Matching（Task Queue 分区与 Worker 长轮询派发）→ 内部系统 Worker（归档/批处理等）。任务队列不是独立 broker，而是建立在持久化层之上的逻辑队列。
- **持久化**：Cassandra / PostgreSQL / MySQL（+SQLite 开发用，可选 Elasticsearch 做高级搜索）；**事件溯源 + 确定性重放**——每步都成事件落库，崩溃后重放重建内存状态；SDK 内置 Sticky Execution 减少重放。

### 5.2 特性

- 多层超时（Schedule-to-Start / Start-to-Close / Heartbeat / Workflow 总超时）、重试策略（指数退避、非重试错误分类）、Cron + 新版 Schedules API、Start Delay、Priority（1-5，较新）、Worker 并发/速率 + Server 端全局 RPS 限流、代码式 DAG + Child Workflow + Signal/Update + Continue-as-New（治理 event history 膨胀）、Namespace 多租户、Visibility Store（SQL/ES 检索）、官方 Web UI、Prometheus + OTel、多集群复制（active-passive 灾备）。
- 幂等：Workflow ID 唯一性 + Reuse Policy 提供"实例级"去重；Activity 本体仍是 at-least-once、须业务幂等。
- 事件触发：无原生外部事件总线，用 Signal/Update + 启动 API 模拟。

### 5.3 成熟度与局限

22,905 stars，MIT；Temporal Technologies Inc. 商业化（Temporal Cloud，D 轮 1.05 亿美元）；Stripe/Netflix/Snap/DoorDash/Coinbase 等大规模生产验证；8 门官方 SDK（Go/Java/Python/TS/.NET/PHP/Ruby/Rust）。
局限：**确定性约束**（workflow 内禁随机/系统时间/原生 IO）带来学习曲线与 SDK 版本兼容负担；Event History 无限增长拖慢重放与 DB（靠 Continue-as-New/归档治理）；部署重（4 服务 + DB[+ES]），每步多行事件写入是吞吐瓶颈；对海量毫秒级短任务属"杀鸡用牛刀"。

### 5.4 优缺点

- ✅ 最强持久化保证；状态完全可审计可重放；四层架构经超大规模验证；语言生态与文档最成熟。
- ❌ 部署与运维最重；确定性编程模型心智负担大；DB 写放大限制吞吐；短任务高吞吐场景过重。

---

## 6. Celery（celery/celery）

### 6.1 架构设计

- **定位**：Python 分布式任务队列事实标准（2009 年起）。**Producer → Broker → Worker → ResultBackend 四段式**；Kombu 做 broker 抽象层（各 broker 以 transport 插件接入，能力差异直接映射为功能差异）；Billiard（multiprocessing 分支）提供 prefork 池。
- **Worker 模型**：默认 prefork（并发=CPU 数），另有 solo/threads/eventlet/gevent 池；`--autoscale` 动态伸缩；`--max-tasks-per-child`/`--max-memory-per-child` 防泄漏；四阶段关机（Warm→Soft→Cold→Hard）。
- **Broker**：RabbitMQ（默认、功能最全，5.5 起官方支持 Quorum 队列）、Redis（有 1h 可见性超时、优先级为模拟实现且语义反转、须 noeviction 防 key 驱逐）、SQS（无 events/远程控制/优先级，ETA 超可见性超时会循环重复执行）、Kafka（实验性）。结果后端：Redis / SQLAlchemy / Django ORM / rpc:// / S3 / DynamoDB 等十余种。

### 6.2 特性

- 重试：手动 `self.retry()` + `autoretry_for` + 指数退避 + jitter + 发布端 retry_policy；定时：celery beat（crontab/timedelta/solar；Django 用 django-celery-beat 入库）；ETA/countdown 延迟（不保证精确，任务提前驻留 worker 内存，官方不建议远期调度）。
- 路由与优先级（RabbitMQ 原生 x-max-priority；Redis 模拟；SQS 无）；**Canvas 编排：chain/group/chord/chunks/map**；`rate_limit`（**worker 本地级，非全局**）；软/硬超时（soft 抛可捕获异常 / hard 强杀进程）。
- Ack 语义：**默认 early-ack（`acks_late=False`）——worker 崩溃时已取回未完成任务直接丢失**；可靠性须 acks_late + `task_reject_on_worker_lost` + 幂等组合配置。
- 监控：完整事件流 + Flower（Web UI + HTTP API + Grafana/Prometheus）+ `celery inspect/control` 远程管理 + bootsteps 深度扩展。
- **缺失**：无原生幂等/去重原语；无原生死信队列（依赖 RabbitMQ DLX）；无全局分布式限流；不支持 Windows。

### 6.3 可靠性短板（官方文档 + 社区印证）

1. early-ack 默认丢任务窗口；2. Redis broker 的可见性超时导致长 ETA/retry 循环重投；3. **chord 可靠性欠佳**（轮询 unlock、header ignore_result 使回调永不触发、issue #2725 无限重试，官方自己建议少用）；4. **beat 单点**（同一 schedule 只允许一个调度器，HA 需 RedBeat 等第三方）；5. 结果默认 1 天过期，结果与 broker 无事务一致性。

### 6.4 成熟度与优缺点

28,869 stars；v5.6.3（2026-03）持续活跃；Instagram（日数百万任务）、Mozilla 等规模验证；Python 生态默认选择。
- ✅ 生态最成熟、功能覆盖面最广；Kombu 可插拔 broker；Canvas 编排；Flower + 信号 + bootsteps 运维工具链完善。
- ❌ 可靠性是"配置出来的"而非默认；chord/beat/可见性超时等已知短板多；调参复杂（prefetch/pool/visibility/consumer_timeout 耦合）；无分布式幂等原语。

---

## 7. 横向特性对比表

✅=原生支持　⚠️=部分/需变通　❌=无

| 特性 | Machinery | Asynq | Hatchet | Temporal | Celery |
| --- | --- | --- | --- | --- | --- |
| Broker/存储 | AMQP/Redis/SQS/PubSub（接口可换） | 仅 Redis | PostgreSQL（RabbitMQ 可选） | Cassandra/PG/MySQL | RabbitMQ/Redis/SQS 等（Kombu 插件） |
| 结果存储 | 独立 ResultBackend | ResultWriter + Redis（保留期） | PG（执行记录） | Event History | 独立 ResultBackend |
| 重试 + 退避 | ✅ 斐波那契 | ✅ 指数+抖动 | ✅ 指数 | ✅ 指数+分类 | ✅ 指数+抖动 |
| per-task 超时 | ⚠️ 靠 context | ✅ Timeout/Deadline | ✅ | ✅ 多层超时 | ✅ 软/硬超时 |
| Cron/周期任务 | ✅（分布式锁） | ✅ Scheduler | ✅ | ✅ Cron+Schedules | ✅ beat（⚠️ 单点） |
| 延迟任务 | ✅ ETA | ✅ ProcessIn/At | ✅ | ✅ Start Delay | ✅ ETA（⚠️ 不精确） |
| 优先级 | ❌ | ✅ 多队列 Weighted/Strict | ✅ 公平队列（按键） | ✅ Priority 1-5 | ⚠️ 分 broker |
| 并发限流 | ⚠️ worker 并发数 | ✅ Concurrency + x/rate | ✅ 全局+按键策略 | ✅ 双向 RPS | ⚠️ worker 本地 rate_limit |
| 编排（chain/group/chord/DAG） | ✅ Chain/Group/Chord/Workflow | ❌（⚠️ Group 聚合非 DAG） | ✅ 声明式条件 DAG | ✅ 代码式 DAG+Child+Signal | ✅ Canvas（⚠️ chord 可靠性欠佳） |
| Durable Execution（断点恢复） | ❌ | ❌ | ⚠️ v1 Durable Tasks（仅等待/派生） | ✅ 事件溯源+确定性重放 | ❌ |
| 幂等/去重 | ❌（靠业务） | ✅ Unique(ttl) / TaskID | ⚠️ 输入去重 key | ⚠️ WorkflowID 唯一性（Activity 靠业务） | ❌（靠业务） |
| 死信/归档 | ❌ | ✅ Archived 状态 + UI 重跑 | ⚠️ 未确认 | ⚠️ Failed 可查可重试 | ❌（靠 RabbitMQ DLX） |
| 同步等待结果 | ✅ AsyncResult.Get | ❌（ResultWriter+轮询） | ⚠️ 未确认 | ⚠️ Workflow 结果 via API 轮询/Get | ✅ AsyncResult.get |
| 取消任务 | ❌ | ✅（Inspector/Session） | ✅ 未确认 | ✅ Cancel | ✅ revoke/terminate |
| Web UI | ❌ | ✅ asynqmon | ✅ | ✅ 官方 UI | ⚠️ Flower（第三方） |
| 可观测性 | ⚠️ OpenTracing | ✅ Prometheus + CLI | ✅ OTel + Prometheus | ✅ Prometheus + OTel | ✅ events + Flower |
| 投递语义 | at-least-once（依赖 broker） | at-least-once + lease 恢复 | at-least-once + 事务状态机 | Workflow effectively-once / Activity at-least-once | 可配 early/late ack（默认有丢失窗口） |
| 多租户/隔离 | ❌ | ❌（仅队列） | ⚠️ tenant 概念 | ✅ Namespace | ⚠️ 靠路由拆队列 |
| 语言 SDK | Go | Go（Rust 社区移植） | Go/TS/Python/Ruby | 8 门官方 | Python（协议跨语言客户端） |
| 部署复杂度 | 低-中 | **最低**（单二进制+Redis） | 低（PG-only） | 高（4 服务+DB） | 低-中（外部 broker） |
| 活跃度 | ⚠️ 放缓 | ✅ 活跃 | ✅ 活跃（YC W24） | ✅ 最成熟 | ✅ 活跃 |

## 8. 优缺点总结对比表

| 框架 | 优点 | 缺点 | 适用场景 |
| --- | --- | --- | --- |
| **Machinery** | 多 broker 可插拔；Chain/Group/Chord 编排在 Go 里最全；接口化设计清晰 | 维护放缓、文档陈旧；无优先级/死信/取消/去重/UI；反射+JSON 性能一般；长期 open bug | 已有 AMQP/SQS 设施且需要编排原语的老项目（需接受维护风险） |
| **Asynq** | 单二进制 + 只需 Redis；lease 恢复 + Lua 原子迁移可靠性扎实；死信归档/去重/优先级/Inspector/UI 运维闭环齐全；活跃 | 强绑 Redis；无 chain/chord/DAG；无原生同步取结果；asynqmon 迭代放缓 | Go 新项目、Redis 基础设施、看重运维闭环与长期维护的默认选择 |
| **Hatchet** | 一个 Postgres 起步；按键公平队列/并发控制/条件 DAG/事件触发针对性强；Embedded Mode；AI Agent 场景友好 | PG 吞吐上限（数百 task/s/引擎，需分片）；durable 语义浅、无确定性重放；语言窄；灾备缺失 | 已有 PG 团队、后台任务 + AI Agent/LLM 批处理、重公平调度 |
| **Temporal** | 最强持久化保证（事件溯源+重放）；状态可审计可回放；超大规模验证；8 门 SDK、文档生态最成熟 | 部署最重（4 服务+DB）；确定性编程心智负担；Event History 膨胀治理；DB 写放大限吞吐；短任务过重 | 长周期复杂编排、Saga/资金流、强审计、多语言大团队 |
| **Celery** | Python 事实标准；功能覆盖最广（重试/beat/Canvas/路由/限流/信号）；Flower 运维链；Instagram 级验证 | early-ack 默认丢任务；Redis 可见性超时循环重投；chord 脆弱；beat 单点；无分布式幂等/死信；调参复杂 | Python 项目默认选项；可靠性须显式配置（acks_late+幂等+beat HA） |

## 9. 对 stateflux 的几点参照

1. **PG/Redis 双组件组合有先例**：Hatchet v1 走"Postgres 唯一事实源"路线、Temporal 把队列建在 DB 之上、Asynq 把全部状态放 Redis——stateflux 的"PG 权威 + Redis 可重建视图"是两者折中，与 Hatchet 的事务化状态转换、Temporal 的"队列即 DB 逻辑结构"同构。
2. **每任务只写 PG 两次**对应 Temporal 的痛点（事件写放大）与 Hatchet 的吞吐上限（数百 task/s/引擎）：stateflux 的 5000 task/s 分发上限设计明显激进，但仍需压测验证。
3. **Asynq 的 lease 租约 + 崩溃恢复**与 stateflux 的 inprocess ZSET 租约 + R1 对账同构；Asynq 的 `Archived` 死信状态 + UI 重跑是 stateflux `dead` 状态应配套的运维面（CLI/重跑接口）。
4. **Asynq 的 Unique/TaskID 去重**证明任务队列内置幂等原语可行且被需要——stateflux 已有 `idempotency_key` 字段，应保证创建侧冲突语义清晰（幂等跳过）。
5. **同步等待结果**：Machinery/Celery 有阻塞 Get，Asynq 没有（社区长期诉求）；stateflux 的长轮询 `GetResults` 设计落在两者之间，pub/sub 精准唤醒（开放问题 1）可参考 Celery 结果后端的 pubsub 实践。
6. **普遍教训**：Celery 的 early-ack 丢任务与 chord 脆弱、Machinery 的无死信无取消，都印证 stateflux 准则"at-least-once + 业务幂等是安全前提"以及把对账/墓碑作为一等机制的正确性；取消能力（stateflux 本期暂缓）几乎所有框架都视为必备，v2 应优先补齐。
