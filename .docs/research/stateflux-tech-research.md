# stateflux 技术调研报告

> 日期：2026-09-08。调研基线：`stateflux-design.md` v3.1。
> 方法：围绕设计目标，对 PG 任务队列、Redis 队列、可靠性与吞吐模式、同类框架四个方向进行文献与源码级调研。

---

## 1. 设计目标提炼

从设计方案（v3.1）归纳，stateflux 的目标可总结为以下六条，它们构成调研的评价基准：

| # | 目标 | 来源条款 |
| --- | --- | --- |
| G1 | **极简依赖**：中间件只有 PG 与 Redis，均中心化部署；Go 实现；框架只有一个进程 `cmd/stateflux`（单二进制多角色：API/Executor 常驻，Scheduler+Collector+Reconciler 由外部选举指定） | §1.1、§2.1 |
| G2 | **PG 唯一权威 + Redis 可重建视图**：任务状态机只由 `tasks` 单表决定，Redis 全部 key 带 TTL，可从 PG 对账重建 | §1.2.1、§3.2 |
| G3 | **吞吐目标**：单调度节点 5000 任务/s 分发上限；每任务全生命周期只写 PG 两次（认领 + 终态回写）；批量写 PG、窄通道通信 | §3.1、§10 |
| G4 | **可靠性语义**：投递 at-least-once，业务幂等是安全前提；租约 lease/续约 + 对账（R1~R4）收敛一切故障；墓碑 + attempt 校验拦截陈旧副本 | §1.2.5、§4、§6 |
| G5 | **双通道执行**：同步任务 = 调度节点 gRPC 直调执行节点（并发闸门 + deadline）；异步任务 = Redis LIST 投递 + BRPOP 消费；结果归集 = pull + Ack 两段式 | §5.2–5.5 |
| G6 | **可扩展的简单性**：选举外置（不实现共识协议）、调度节点唯一、四阶段分别可横向扩展（分片/分区/拆队列作为开关） | §1.2.6、§11 |

一句话画像：**「PG+Redis 之上的单二进制分布式任务调度框架，5k 任务/s，at-least-once + 幂等，同步 RPC 回执与异步队列双通道」**。

---

## 2. 核心结论（TL;DR）

1. **生态空位真实存在**：没有任何现成框架同时满足「极简依赖 + 5k 任务/s + 同步 RPC 回执 + 单二进制多角色」。轻量库（River/Asynq）无调度中心与同步回执；重型编排（Temporal/Cadence）运维成本超标；国产调度中心（XXL-Job/PowerJob）是 cron 导向、吞吐不在 5k/s 量级。**自研定位成立**。
2. **核心机制全部有成熟先例**：`SKIP LOCKED` 认领（River/Oban/Solid Queue）、租约 ZSET + recoverer（Asynq 内部实现几乎同构）、单调度者 + 选举外置（K8s scheduler）、DB 选主中心化调度（XXL-Job）、状态全在 DB 且节点无状态可重启（Windmill/Hatchet）。设计不是发明，而是组合已知最优实践。
3. **最大的技术风险是双存储视图**：PG（权威）与 Redis（视图）之间的一致性依赖对账重建，现有框架无直接先例可抄（Windmill/Hatchet 单存储，Asynq 单存储）。需要重点验证 R4 重建路径与墓碑/attempt 校验的完备性。
4. **5k 任务/s 的可行性有数据佐证**：PG 队列社区经验约 10K tx/s（River/HN），Hatchet 基于 PG 声称 20k+ tasks/s；Redis 单实例简单命令 10 万+ OPS（pipeline 可到数十万），每任务 5~6 次 Redis OPS 的推演成立。瓶颈确认在调度节点自身与 PG 写，与设计文档 §10 的排序一致。
5. **三个具体改进建议**（详见 §7）：Redis Streams 可作为 v2 备选消除 LIST 无 ack 缺口；重试退避加 jitter；NOTIFY 仅作唤醒信号不可依赖送达。

---

## 3. 方向一：PG 作为任务队列

### 3.1 SKIP LOCKED 认领模式（对应 G3、G4）

- 核心模式：`SELECT/UPDATE ... FOR UPDATE SKIP LOCKED LIMIT n` 放在**短事务**里，多个 worker 并发认领互不阻塞、绝不重复认领。Neon、Prisma、Netdata 均有权威指南；Rails Solid Queue、Oban（v2.0 起在显式事务中显式使用）、River 全部构建于此模式上。
- 已知边界（对 stateflux 的启示）：
  - **事务必须短**：认领 SQL 里做完查询+状态迁移立即提交（stateflux 的「单条 UPDATE 合并查询与迁移」决策 §9.1 正确）；
  - **部分索引是性能基本盘**：`WHERE status='pending'` 部分索引 + `ORDER BY` 索引列是社区共识做法（stateflux §3.1 一致）；
  - **需要租约/超时回收**：锁只保护认领瞬间，worker 崩溃后必须靠 `locked_at`/lease 超时重派——这正是 stateflux 用 Redis 租约 + 对账承担的部分；
  - **高吞吐上限**：LinkedIn 引述的社区经验约 **50k jobs/s** 是 PG 队列的极限区间，HN 上 River 社区经验为「任务短小时 **~10K tx/s** 可用」。autovacuum 在高频 UPDATE 表上是常见痛点，需关注 fillfactor/HOT update；量大后按时间分区 + detach 归档（stateflux §3.1 已有此设计）。

### 3.2 LISTEN/NOTIFY（对应 §5.1 可选唤醒）

- NOTIFY 提交需要**全局排他锁**，高事务率下是瓶颈（DBOS 有反论与缓解方案）；payload 上限 8000 字节；**不持久化**——无监听者时通知直接丢失；与连接池（PgBouncer transaction mode）不兼容，需要独占连接。
- 结论：**NOTIFY 只能作低延迟唤醒的优化信号，不能作为正确性来源**。stateflux「定时 tick 兜底 + NOTIFY 驱动」的双触发设计正确，但实施时必须：专用连接、payload 只传 task ID 或干脆为空、并保持 tick 为最终兜底。

### 3.3 代表性 PG 队列框架

| 框架 | 语言 | 认领方式 | 调度进程 | 特点 |
| --- | --- | --- | --- | --- |
| [River](https://riverqueue.com/) | Go | SKIP LOCKED | 不需要（内嵌应用进程；周期任务用 PG advisory-lock 选主） | 事务性入队（与业务同事务）；类型安全 generics；约 10K tx/s 经验值 |
| [Oban](https://hexdocs.pm/oban/2.0.0-rc.1/changelog.html) | Elixir | SKIP LOCKED | 内嵌 | 状态机最全（available/scheduled/executing/retryable/cancelled/discarded/completed）， 有 `suspended` 态；350+ worker 生产案例 |
| [Graphile Worker](https://github.com/graphile/worker) | Node | SKIP LOCKED | 内嵌 | LISTEN/NOTIFY + 轮询混合驱动 |
| [pg-boss](https://github.com/timgit/pg-boss) | Node | SKIP LOCKED | 内嵌 | 单 PG 实现完整队列语义 |

来源：[Neon queue guide](https://neon.com/guides/queue-system)、[Prisma: SKIP LOCKED without Redis](https://www.prisma.io/blog/you-dont-need-a-job-queue-postgres-already-has-skip-locked)、[HN: River](https://news.ycombinator.com/item?id=38349716)、[Microsoft: PG as job queue 的代价](https://techcommunity.microsoft.com/blog/adforpostgresql/potential-consequences-of-using-postgres-as-a-job-queue/4514332)、[HN: LISTEN/NOTIFY does not scale](https://news.ycombinator.com/item?id=44490510)、[DBOS 反论](https://www.dbos.dev/blog/postgres-listen-notify-scalability)、[PgDog 池化分析](https://pgdog.dev/blog/scaling-postgres-listen-notify)。

---

## 4. 方向二：Redis 作为队列与共识视图

### 4.1 LIST+BRPOP 的可靠性缺口与替代（对应 G4、G5）

- **BRPOP 即弹出即丢失**：消息交给 consumer 后从 Redis 消失，consumer 崩溃则消息丢失；LIST 无原生 ack、无重放。社区标准补偿是 `BRPOPLPUSH` 到 processing list + 超时扫描回收。
- **Redis Streams**（XREADGROUP/XACK/XPENDING/XAUTOCLAIM）提供 consumer group、per-message ack、PEL（Pending Entries List）崩溃恢复与死信处理，是官方推荐的更可靠队列形态；代价是管理 PEL、trimming 的复杂度与更高的每消息开销。
- 对 stateflux：异步链路选择的 LIST+BRPOP **用 Redis 之外的对账体系（R1 重置 + attempt 校验 + 墓碑）补足了 ack 缺口**，逻辑上等价于把「processing list」搬进了 PG。这是成立的，但要清醒认识到：**BRPOP 后、注册 inprocess 前的窗口，唯一兜底是执行节点宕机后 90s grace 的对账重置**——重复执行窗口 = grace 周期，文档 §4 已如实声明。v2 可评估 Streams `XAUTOCLAIM` 替代自研对账对异步链路的覆盖（同步链路仍需自研）。

### 4.2 Asynq 内部机制（与 stateflux 高度同构，强烈建议精读）

Asynq（Go/Redis，13.7k★）的状态存储设计与 stateflux 几乎一一对应：

| Asynq | stateflux 对应物 |
| --- | --- |
| pending LIST + `RPOPLPUSH/BRPOPLPUSH` → active LIST | 就绪队列 + BRPOP + 注册 inprocess |
| active 任务同时记入 **lease ZSET（默认 30s 到期）** | `stateflux:inprocess` ZSET，score=租约到期（默认 30s） |
| 后台 recoverer 扫过期 lease → 重新入队 | 对账 R1 重置 |
| scheduled/retry/archived 三个 ZSET | PG `tasks` 表的 run_at/status/dead |

这验证了「ZSET score=租约到期 + 周期 recoverer」是经过大规模生产验证的模式。Asynq 默认 max retry=25、指数退避，可作为参数默认值参考。

### 4.3 Redis 容量与持久化（对应 G3 容量模型推演）

- 容量：Redis 单实例简单命令 **~10 万+ OPS**（入门笔记本级别即可达到，[官方 benchmark](https://redis.io/docs/latest/operate/reference/optimization/benchmarks/)），pipeline 批量下可达数十万 OPS——stateflux「每任务 5~6 次 OPS、中心 Redis 承载 10 万+」的推演有官方数据支撑。
- 持久化：AOF `everysec` 崩溃时**最多丢 ~1 秒写入**；`always` 显著拖慢吞吐；即便 `always` 也有磁盘缓存/副本传播的边界情况。**Redis 不是持久性保证的消息队列**——这正是 stateflux「Redis 是可重建视图、PG 才是权威」的论据。生产配置建议 AOF everysec + 主从 + 哨兵，并把「Redis 全量丢失 → R4 重建」作为常态演练场景。
- 分布式锁争议：Kleppmann 对 Redlock 的批评（[How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)）结论适用于租约场景：**lease 无法防止「GC 暂停后苏醒的僵尸进程」，必须配合 fencing token**。stateflux 的 attempt 编号本质就是 fencing token（终态回写要求 attempt 匹配，僵尸节点无法覆盖新尝试），设计正确；实施时必须确保**所有**终态与归集写路径都带 attempt 校验，无一例外。

来源：[Svix reliable queues](https://www.svix.com/resources/redis/reliable-queue/)、[antirez streams consumer patterns](https://redis.antirez.com/fundamental/streams-consumer-patterns.html)、[Redis 官方 job queue 教程](https://redis.io/tutorials/redis-backed-job-queue-for-background-workers/)、[Asynq](https://github.com/hibiken/asynq)、[Asynq 源码解析（bysir）](https://blog.bysir.top/blogs/boom_asynq)、[Redis persistence 文档](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)。

---

## 5. 方向三：可靠性与吞吐设计模式

### 5.1 投递语义（对应 G4）

- 端到端 exactly-once 在分布式队列中不可行（无法区分「响应丢失」与「未执行」）；业界收敛结论是 **exactly-once ≈ at-least-once + 幂等处理**（Kafka 的 EOS、Flink 的端到端一致均建立在此上）。stateflux §1.2.5/§13 的立场与业界共识完全一致。
- 幂等的惯用法是 **idempotency key**（客户端生成的唯一键，服务端去重）。stateflux 的 `idempotency_key` 字段（创建侧去重）+ `task_id+attempt`（执行侧去重）双层设计完备。

### 5.2 租约与 fencing（对应 §3.2/§6）

- 租约时长 : 续约间隔的常见比例为 **3:1**（stateflux 30s/10s 符合）；租约必须假设时钟有漂移，判定以服务端时间为准（stateflux 用 Redis score=到期时间，以 Redis 单点时钟为准，正确）。
- **fencing token 是租约安全性的必要补充**（Kleppmann/Chubby 论述）：stateflux 的 attempt 即 fencing token；墓碑（终态后拒绝注册）是第二道防线，语义类似 Chubby 的 sequencer 校验。

### 5.3 重试与死信（对应 §6.3 R3）

- **指数退避必须加 jitter**：无 jitter 的重试会在故障恢复后形成同步重试风暴（AWS canonical 文章，推荐 full jitter + cap）。stateflux 的 `run_at = now + 退避` 应实现为 capped exponential backoff + full jitter，目前文档只写「退避」，建议在实施规范中明确。
- DLQ 最佳实践：死信与原任务同库保留（stateflux 用同表 `dead` 状态，比独立队列更利于排查，与 SQS「DLQ 保留期 > 原队列」的精神一致）；毒丸修复根因后人工 redrive，不要自动重放。

### 5.4 调度器架构先例（对应 G1、G6）

- 调度器架构三代：**单块调度器**（Borg/K8s）→ **两级调度**（Mesos/YARN）→ **共享状态乐观并发**（Omega）。K8s 明确选择**单活跃调度者**（多实例经 Lease API 选主，非主者 standby）换取简单性——与 stateflux「调度节点唯一 + 选举外置」同构，说明该选择在工业界是第一梯队的主流路线。
- 分片扩展先例：Kafka 消费组再平衡（KIP-848 改为 epoch 化增量分配，消除 stop-the-world）与 Elastic-Job 的 ZK 弹性分片。stateflux §11 的「hash(type) 分片 + 每分片唯一调度者」开关设计与此吻合，属于正确的扩展路径。
- 反压先例：Kafka 的 pause()/resume()（保持心跳但停止拉取）与 stateflux「claim 数由下游容量实时决定」的 credit-based 思路一致；创建侧不设反压、由 PG 吸收，与「先落库后调度」的解耦一致。

来源：[Cambridge CamSaS 调度器架构综述](https://www.cl.cam.ac.uk/research/srg/netos/camsas/blog/2016-03-09-scheduler-architectures.html)、[K8s scheduler](https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/)、[KIP-848](https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/)、[AWS: Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)、[AWS DLQ](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)、[Jay Kreps: exactly-once](https://medium.com/@jaykreps/exactly-once-support-in-apache-kafka-55e1fdd0a35f)。

---

## 6. 方向四：同类框架对比

### 6.1 横向对比

| 框架 | 语言/热度 | 核心依赖 | 调度模型 | 吞吐量级 | 同步回执 | 运维成本 | 与 stateflux 目标差距 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [Temporal](https://docs.temporal.io/temporal-service/temporal-server) | Go/22.9k★ | PG/MySQL/Cassandra（+可选 ES），**4 个服务角色** | worker 长轮询 + Matching 队列 + History 分片（事件溯源） | 集群级数万 task/s | 有（但要求业务 workflow 化改写） | **高** | 运维超标；语义重 |
| [Cadence](https://cadenceworkflow.io/docs/concepts/topology) | Go/9.4k★ | 同上 | 同上 | 参考：1M active workflow 需 40–80 台 host | 有 | 高 | 同上 |
| [Hatchet](https://github.com/hatchet-dev/hatchet) | Go/7.9k★ | **仅 PostgreSQL** | push-based 调度 | **20k+ tasks/s**（官方） | 弱（偏流式/事件） | 中低（引擎可 sidecar） | 最接近的重型方案；无 Redis 视图层、同步回执弱 |
| [Windmill](https://www.windmill.dev/blog/launch-week-1/fastest-workflow-engine) | Rust/17.8k★ | 仅 PG | DB 队列 + 无状态 worker | 官方对标 Airflow 13x | 有 | 低（单二进制） | 定位是脚本自动化平台而非任务框架 |
| [Restate](https://restate.dev/vs/temporal) | Rust/4.4k★ | 内嵌复制日志，无外部 DB | push / RPC 化 handler | 中高 | **强（原生 RPC 语义）** | 低（单二进制） | 自带日志层，不满足「只依赖 PG+Redis」 |
| [River](https://github.com/riverqueue/river) | Go/5.6k★ | PG | 内嵌库，无调度进程 | ~10K tx/s（社区经验） | 无 | 低 | **架构血缘最近**，但是库不是系统：无角色模型、无同步回执、无归集 |
| [Asynq](https://github.com/hibiken/asynq) | Go/13.7k★ | Redis | 内嵌库 | 高（Redis 量级） | 无 | 低 | 同上；其 lease ZSET+recoverer 与 stateflux inprocess 同构 |
| [XXL-Job](https://github.com/xuxueli/xxl-job) | Java/30.5k★ | MySQL | 调度中心集群（**DB 行锁选主**）+ 内嵌执行器，HTTP 回调 | 千级 QPS（cron 导向） | 无（异步回调） | 低 | 哲学最接近（极简依赖+DB 选主），但 Java、cron 导向、无队列视图层 |
| [PowerJob](https://github.com/PowerJob/PowerJob) | Java/7.8k★ | DB + Akka 通信 | Server 集群选主 + 动态分片 | 中 | 无 | 中 | 功能全但引入 Akka，违背极简目标 |
| [Elastic-Job](https://shardingsphere.apache.org/elasticjob/current/cn/features/elastic/) | Java/8.2k★ | **ZooKeeper** | 去中心化 + 弹性分片 | 中 | 无 | 中 | 需要额外 ZK，与「无额外组件」相反 |

### 6.2 对比结论

1. **生态空位**：Go 生态里「库」（River/Asynq）与「平台」（Temporal/Hatchet）之间缺少一个「带调度中心/执行器角色模型的轻量系统」；国产调度器验证了 DB 选主中心化调度的可靠性，但均无高吞吐队列与结果归集体系。stateflux 的「同步 gRPC 直调 + 异步 Redis 队列」双通道在同类中独有。
2. **吞吐参照系**：Hatchet（PG-only，20k+/s）与 River（~10K tx/s）证明 5k 任务/s 在单 PG 权威下可行，前提是批量写与正确的索引/分区设计——stateflux §3.1/§11 的设计方向正确。
3. **可借鉴先例**：K8s Lease 选主 + client-go workqueue 指数退避；Kafka KIP-848 epoch 化增量分配与 pause/resume 反压；XXL-Job DB 锁互斥；Windmill/Hatchet「状态全在 PG、节点无状态可重启」。

---

## 7. 对 stateflux 设计的验证与建议

### 7.1 设计验证（目标 ↔ 调研证据）

| 设计决策 | 调研证据 | 结论 |
| --- | --- | --- |
| SKIP LOCKED 单条 SQL 认领（§9.1） | River/Oban/Solid Queue 同模式；社区经验 10K tx/s | ✅ 主流且够用 |
| 每任务只写 PG 两次（§9.2） | Hatchet 20k/s（PG-only）证明批量写 PG 是可行路径 | ✅ 关键吞吐假设成立 |
| inprocess ZSET 租约 + attempt fencing（§9.3） | Asynq lease ZSET + recoverer 同构；Kleppmann fencing token 论述 | ✅ 生产验证模式；fencing 语义正确 |
| LIST+BRPOP 无 ack 缺口用对账+墓碑补（§9.4） | Redis 官方教程将 Streams 列为更可靠替代；LIST 缺口是公认事实 | ⚠️ 成立但重复执行窗口=grace(90s)，需明确告知业务方 |
| 选举外置 + 调度节点唯一（§9.8） | K8s 单活跃调度者 + Lease 选主；XXL-Job DB 锁选主 | ✅ 工业主流路线 |
| at-least-once + 幂等（§1.2.5） | 业界共识（Kafka/Flink 端到端论证） | ✅ 正确立场 |
| Redis 可重建视图（§1.2.1） | Redis AOF everysec 丢 ~1s 写入 → Redis 不具备队列持久性 | ✅ 分层正确，且是必需 |

### 7.2 建议

1. **【P1】重试退避明确为 capped exponential backoff + full jitter**：文档只写「退避」，实施规范应引用 AWS 模式，否则重试风暴会打在刚恢复的调度节点上。
2. **【P1】NOTIFY 仅作优化信号**：专用连接、不依赖送达、payload 最小化；tick 兜底必须默认开启且不可关闭（对应开放问题外的实施细节）。
3. **【P2】评估 Redis Streams 作为异步链路的 v2 备选**：`XAUTOCLAIM` 可原生覆盖「BRPOP 后崩溃」窗口，把异步链路的重复执行窗口从 grace(90s) 缩到 claim 超时；代价是复杂度与每消息开销。v1 维持 LIST+对账，v2 做开关。
4. **【P2】归集与终态写路径全量加 attempt 校验的测试矩阵**：fencing token 的安全性取决于「无一例外」；建议在实施顺序第 2/6 步的单测中加入僵尸节点（attempt 过期后写回）用例。
5. **【P3】参数默认值参照同类**：Asynq 默认 max retry=25、lease 30s 与 stateflux 一致，可作为 max_attempts 默认值参考；退避初值/上限建议参照 client-go workqueue（如 500ms~1000s）。
6. **【P3】压测目标对标**：把 5000 任务/s 分解为「claim 500×10 tick/s」的可测指标，压测时同时监控 PG autovacuum 与 tasks 表膨胀（高频 UPDATE 场景的已知痛点，fillfactor/HOT update 调优预留）。

### 7.3 风险清单

| 风险 | 等级 | 缓解 |
| --- | --- | --- |
| 双存储视图一致性（PG↔Redis）无现成先例可抄 | 中 | R1~R4 单测全覆盖 + 进程全灭重启收敛验证（§12.7 已列）+ 常态演练 R4 |
| 重复执行窗口 = grace 90s，业务方可能低估 | 中 | 文档明示 + 幂等键强制要求 + grace 参数可配 |
| 单调度节点内存/网络在 5k/s 同步任务下成瓶颈 | 低 | §11 分片开关已预留；同步分发池独立扩容 |
| PG 高频 UPDATE 表膨胀/autovacuum 抖动 | 低 | 分区 + detach 归档已设计；压测阶段调 fillfactor |

---

## 8. 参考资料（按主题）

**PG 队列**：[Neon queue guide](https://neon.com/guides/queue-system) · [Prisma SKIP LOCKED](https://www.prisma.io/blog/you-dont-need-a-job-queue-postgres-already-has-skip-locked) · [River](https://riverqueue.com/) / [brandur.org/river](https://brandur.org/river) · [HN: River](https://news.ycombinator.com/item?id=38349716) · [Oban changelog](https://hexdocs.pm/oban/2.0.0-rc.1/changelog.html) · [Microsoft: PG as job queue](https://techcommunity.microsoft.com/blog/adforpostgresql/potential-consequences-of-using-postgres-as-a-job-queue/4514332) · [Citus: lock tips](https://www.citusdata.com/blog/2018/02/22/seven-tips-for-dealing-with-postgres-locks/)

**LISTEN/NOTIFY**：[DBOS: actually scales](https://www.dbos.dev/blog/postgres-listen-notify-scalability) · [HN: does not scale](https://news.ycombinator.com/item?id=44490510) · [PgDog](https://pgdog.dev/blog/scaling-postgres-listen-notify)

**Redis 队列**：[Svix reliable queue](https://www.svix.com/resources/redis/reliable-queue/) · [antirez: streams consumer patterns](https://redis.antirez.com/fundamental/streams-consumer-patterns.html) · [Redis 官方 job queue 教程](https://redis.io/tutorials/redis-backed-job-queue-for-background-workers/) · [Redis Streams 文档](https://redis.io/docs/latest/develop/data-types/streams/) · [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) · [Redis benchmarks](https://redis.io/docs/latest/operate/reference/optimization/benchmarks/)

**租约与幂等**：[Kleppmann: How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) · [Jay Kreps: exactly-once](https://medium.com/@jaykreps/exactly-once-support-in-apache-kafka-55e1fdd0a35f) · [Confluent EOS](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/) · [AWS: Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) · [SQS DLQ](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)

**调度器架构**：[CamSaS: 三代调度器架构](https://www.cl.cam.ac.uk/research/srg/netos/camsas/blog/2016-03-09-scheduler-architectures.html) · [K8s scheduler](https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/) · [KIP-848](https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/) · [Temporal scaling](https://temporal.io/blog/scaling-temporal-the-basics)

**同类框架**：[Temporal](https://docs.temporal.io/temporal-service/temporal-server) · [Hatchet](https://github.com/hatchet-dev/hatchet) / [vs Temporal](https://hatchet.run/versus/hatchet-vs-temporal) · [Windmill benchmark](https://www.windmill.dev/blog/launch-week-1/fastest-workflow-engine) · [Restate vs Temporal](https://restate.dev/vs/temporal) · [Asynq](https://github.com/hibiken/asynq) / [源码解析](https://blog.bysir.top/blogs/boom_asynq) · [XXL-Job](https://github.com/xuxueli/xxl-job) / [架构解析](https://zhuanlan.zhihu.com/p/649370118) · [PowerJob 对比](https://www.cnblogs.com/Chary/articles/18866332) · [Elastic-Job](https://shardingsphere.apache.org/elasticjob/current/cn/features/elastic/)
