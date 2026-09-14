# stateflux 设计 · 版本历史

> 当前版本见 [README](./README.md)；各主题文件头部标注所对应的版本。

## v3.21（2026-09-14）：任务行模型重构——run_at 退役、调度目标节点属性与公共段/阶段段

- **`run_at` 全链退役**（四表均移除）：调度（晋升与认领）不再判断时间门槛，延迟任务语义移除；
  R1 重置后立即重新可认领（无退避），重试防打爆依赖 `max_attempts`/R3；晋升与认领排序均只按
  `priority DESC`，两处扫描索引改为 `(priority DESC)`；
- **死列清理**：`error` 收敛为 completed 专有列——它只在终态写入，pending/schedulable/processing 中
  恒为零值，全部移除；pending 移除 `attempts`/`claimed_node`（claim 才产生，从未认领的行恒为零值）；
- **认领节点列统一更名为 `claimed_node`**（原 `owner_node`）：schedulable=上次认领节点（重置后
  保留上次值）、processing=本次认领节点、completed=最后认领节点（均为诊断/审计用途）；
- **调度目标节点属性**：`vpc`/`node`/`label` 属公共段（`label` 随行保留至终态，completed 审计
  留档），`hash_bucket` 存在于 pending/schedulable/processing（认领消费后不入终态）。`label` 是
  期望执行节点的匹配标签（自由文本、空串=不限）；`hash_bucket` 是 0–255 整数分桶（Int16，
  `BucketCheck` 钉住区间），0=不限、接入方自定义语义、框架只做等值匹配；二者在晋升与认领时过滤；
- **字段语义分类**：`vpc`/`node`/`label`/`hash_bucket` 为**调度目标节点属性**（调度时据此筛选目标
  节点），`biz_race_labels`/`biz_race_entry`/`biz_group`/`biz_batch_id` 为**业务属性**（业务写入，
  框架不解释或仅按文档用途消费）；
- **业务字段更名与并发约束字段**：`group`→`biz_group`、`batch_id`→`biz_batch_id`（消除与 PG 保留字
  的重名）；新增 `biz_race_labels`（字符串数组）与 `biz_race_entry`（字符串）作为**业务并发约束**
  ——entry 唯一标识业务对象、labels 声明约束标签集合，由调度侧实施并发控制（机制待定，§14）；
  公共段扩至 19 列；
- **payload 归位 `task_results`**：`task_completeds` 不再内联 payload（终态历史只留 outcome/error
  摘要与时间），`task_results` 新增 `payload`（终态事务内从 `task_payloads` 合并入行）——任务载荷与
  业务结果同表、保留期统一，历史表更瘦；
- **schema 迁移 SQL 与索引延后**：新增 `internal/domain/migration/000001_20260914_create_task_tables.sql`
  创建全部 7 张表（命名格式 `{6位版本}_{日期}_{描述}.sql`，按版本序增量执行；表注释独立于
  次级索引本期不建（延后优化）；表注释机制一并移除（ent 不支持表级注释）；
- **移除并发名额与控制租约机制**（含 `concurrency_reservations`、`control_leases` 两表）：claim
  不做名额预留（并发上限控制移出本期范围，按组限流需另行设计）；控制面互斥仅由外部选举承担、
  不做 PG fencing，双主窗口内的控制面周期操作靠幂等收敛；`control_epoch` 相关表述、指标
  （`control.fence_rejects`、`concurrency.reservations`）与 §14 中名额/租约待定项一并移除；
  对账周期收敛为 R1–R4（原 R5 名额修复随之取消）；
- **四表字段模型改为「公共段 + 阶段段」**：公共段 17 列（含 label）四表一致、增删四处同步；
  阶段段按需取舍（attempts/claimed_node 在 schedulable/processing/completed；hash_bucket 在
  pending/schedulable/processing；error 仅 completed）；字段顺序四表统一。v3.20 的「任一列增删
  必须四处同步」就此修订。

## v3.20（2026-09-12）：调度约束字段、数值枚举与注释

- 任务行新增两个**业务无关的调度约束**字段：`vpc`（目标网络域）与 `node`（期望执行节点），创建时
  指定、空串表示不限；由调度侧在晋升与认领时过滤，执行侧不参与决策（§3.1/§5.2/§5.3）；与
  `owner_node`（实际认领节点）语义区分，重派时 `node` 不变；
- 枚举与取值区间：`priority` 改为 **0–100 连续区间**（Int8，默认 50，越大越优先，由各表 CHECK 约束
  钉住区间）——它不是枚举，业务可自行细分语义，调度只依赖排序；`outcome` 保持**数值封闭枚举**
  1=succeeded / 2=failed / 3=dead（0 保留为未设置，使零值与漏赋值可检测）。`vpc`/`node` 是外部标识的
  自由文本（VPC 名与节点 ID），**不是枚举、不做数值编码**。编码定义在 `internal/domain/schema`
  （ent 生成码反向 import 本包，故枚举常量不得下沉到 domain，避免成环）；
- 任务 topic 随 priority 区间化改为**档位**：`task.{band}`（low 0–33 / normal 34–66 / high 67–100），
  避免连续值产生上百个 topic；档位只决定订阅与投递分组，精确优先顺序仍由 PG 的 `priority` 排序决定
  （§3.2/§5.2）；
- 任务行新增 `operator` 与 `group`：`operator` 是 `type` 的**子类型**（type 粗、operator 细，二者共同
  决定 handler 选择与路由，新增子操作不必新增 type）；`group` 是**业务分组键**，供上层业务按组查询/
  聚合，框架只建索引、不解释语义、不参与调度正确性，并与 `batch_id`、并发 `scope_key` 显式区分；
  四张阶段表的 `group` 各建普通索引；
- 表结构自描述：所有列带注释并由 ent 写进 DDL（`entsql.WithComments`）；ent v0.14.6 不支持**表级**
  注释，表注释由迁移引导 DDL 补齐（新增 `internal/domain/migration/table_comments.sql`）；
- 四阶段表字段集保持一致（无 mixin，各 model 独立定义）：新增列或改枚举必须四处同步。

## v3.19（2026-09-12）：通道抽象统一与 Redis 定位收敛

- 新增 `internal/eventbus/channel` 作为任务与结果的唯一节点通信抽象：Send/Subscribe 与能力声明
  （单向 publish/subscribe、半双工 request/reply、全双工 stream）；RPC、gRPC stream 与 Redis
  list/zset/stream/pubsub 都只是它的实现，同步与异步的差别仅是该抽象的不同 channel 实现；
- `runtime`/`collector` 经订阅 EventBus 获取结果集；调度节点与执行节点之间的结果归集由
  eventbus 承载（默认 gRPC 双向 stream，可替换为 Redis pub/sub、list 等），不再由结果通道
  类型决定状态机；
- Redis 收敛为不可靠传输实现：不以 AOF、PEL、XACK 或任何 Redis 状态作为正确性前提，异步通道
  仅确认「已尝试发送」，结果由执行节点另行发布 ResultEvent；
- 撤回 v3.18 引入的 tenant：本期不引入 tenant，`idempotency_key` 由接入方保证全实例唯一
  （`task_identities` 幂等账本保留）；
- 异步链路由 Redis Streams consumer group 回到「通道无差别可替换」：可靠投递不再由 Redis 提供，
  全部由 PG 认领、终态与 R1–R5 对账收敛。

### v3.19 一致性修订（评审后，无设计语义变更）

- 修正全库 § 交叉引用：原指代 SKIP LOCKED、factory/callback、观测、Handler 注册、死信运维、attempt
  语义与结果表不变性的失效或不符引用，全部改为实际出处（§3.1、§5.6、§6.4、§1.2.8、§6.2、§9.6、
  §5.5）；文档与 Go 注释同步；
- 术语统一：明确 `attempts`（已认领次数）与 `attempt`（本次 fence 值）的关系，任务行字段表去掉
  重复的 `attempt`；`handler timeout` 统一为任务级执行预算 `timeout_ms`（默认 60s，与任务行一致），
  删除与 R1 的 `max(timeout, dispatch_grace)` 公式冲突的「必须小于 grace」表述；
- `inprocess` 命名随可靠性收敛撤回：Redis 侧只保留 `domain/cacheview` 容量提示视图（非正确性
  路径），§5.4 与 R2 措辞同步；
- 补齐此前未定义的表述：墓碑 = `task_results` 的 `task_id` 主键终态行；`batch_id` 的工厂孤儿判定
  用途写入 §5.6；`control_epoch` 明确为诊断字段、不参与校验；
- §6.4 指标名与实现对齐（统一 `stateflux.` 前缀、`wal.backlog` 拆分为 `.entries`/`.bytes`、
  水位型指标为同步 Int64Gauge）；README 索引描述改为与实际内容一致；
- §8.1 标注为历史记录并修正验证步骤（`make test` 目标不存在，改为 build/vet + demo 冒烟）；
  §12 增补当前进度与骨架差异；实施前必须定稿的待定项集中于 §14.6–§14.13；
- 任务表 schema 对齐本文档：四阶段表移除 v3.18 遗留的 `exec_mode`(sync/async)、改为**非空**
  `channel`（`internal/domain/schema`，ent 生成码同步重生成）——列上只记录逻辑 channel 名，同步与
  异步由被解析 channel 的能力推导（§3.2、§14.1）；默认 channel 在装配/配置层补齐，故不设列默认值。

## v3.18（2026-09-11）：可靠投递与控制面正确性收敛

- 业务接入从“直写任务表组”收敛为同一本地事务调用 `enqueue_tasks` SQL 函数；引入 tenant 与
  `task_identities`，以跨阶段的稳定幂等账本替代热表扫描和局部唯一索引，并以最小 DB 权限保护内部表；
- 异步链路从 Redis LIST/BRPOP 切换为 Redis Streams consumer group（PEL、XACK、XAUTOCLAIM），
  `inprocess` 明确降级为执行租约视图，不再称为共识；Redis v1 基线提升至 6.2+；
- 外部选举保留，但新增 PG `control_leases` 单调 epoch fence；并发限制改为 claim 事务中的
  `concurrency_reservations` 条件预留，容忍故障切换和短暂双主；
- 统一 attempts 语义：仅 CLAIM 加一，重试/R1 重置不加；`sync/async` 改称 `direct/queue` 投递模式，
  创建方始终查询结果；对账扩展为 R1–R5（新增名额账本修复）。

## v3.17（2026-09-11）：控制面运行时归组目录更名

- `internal/controller/` 更名 `internal/runtime/`（目录名对齐「运行时」归组语义；
  scheduler/collector/reconcile 子包不变，纯目录更名、运行语义零变更）。

## v3.16（2026-09-10）：契约收编 api/ + 两运行时模型 + 契约面收敛

- 所有 proto 收拢至顶层 `api/`（`stateflux/task/v1` 调度↔执行契约、`cluster/v1` 外置集群契约，
  生成码与 proto 同目录）；
- 业务接入 RPC 契约不做（原模式 B `CreateTasks`、`GetResults` 长轮询、死信 RPC 移除），业务方按
  outbox 式直写任务表组接入；
- 运行时模型改为 **biz（task/v1 服务端业务）+ 控制面运行时（controller：scheduler/collector/
  reconcile）+ 执行侧运行时（worker，整体承接原 executor）**，`internal/server` 仅编排三者装配启停；
- Redis 队列视图下沉 `domain/cacheview`，迁移独立 `domain/migration`；`obs` 扩展为 metrics +
  tracing；ent 生成码不入库（构建时经 `make generate` 从 `domain/schema` 产出）。

## v3.15（2026-09-10）：内核并入 domain + 任务运行时归组 + 仓储 ent 化

- `sdk` 清空，共享内核（Task/Callback/Result 模型、Handler/Registry/Precondition 契约、退避、
  雪花 ID）按 model 并入 `internal/domain`（task.go/callback.go/result.go/ops.go/handler.go/
  retry.go/snowflake.go）；
- `factory`/`queue`/`dispatch` 收拢至 `internal/task/`（任务运行时归组）；
- 仓储读写全面改走 ent 类型安全 API（CreateBulk + OnConflict upsert、FOR UPDATE 行锁承载
  attempt fencing），仅认领挪行（§3.1 SKIP LOCKED 单条 SQL）与迁移引导 DDL 保留原生执行；
- 测试代码清空（后续按需重建，§8）。

## v3.14（2026-09-10）：domain 四层完整落地

领域层细分为 `domain`（聚合接口 + 组合根）、`domain/repository`（仓储实现，聚合语义）、
`domain/data`（数据源基建：连接池/ent client/迁移/SQL 套件 + ent 生成码）、`domain/schema`
（ent 表定义，代码生成输入），实现数据库操作的完整分层（§8）。

## v3.13（2026-09-10）：角色包归组

`collector`/`reconcile`/`executor` 收拢至 `internal/server/` 下（服务编排归组；executor 为常驻
角色（§2.1），仅物理位置变化，运行语义不变，§8）。

## v3.12（2026-09-10）：数据访问分层（domain/repository）

`internal/store`（纯接口）与 `internal/storepg`（ent 实现）重组为 `internal/domain`：聚合接口
（Task/Result/Ops）+ 组合根 Store + 语义错误在包根，实现层 `domain/repository/pg`（ent 生成码随包）；
消费侧按聚合依赖窄接口（§8）。

## v3.11（2026-09-10）：布局微调：sdk 提升为顶层公开包

Task/Handler 契约类型是执行接入（§1.2.8）的稳定依赖面，公开以支撑构建时注册与 v2 执行接入演进
（§14 遗留）；新增 `build/`（服务镜像 + 本地中间件编排）与 `hack/`（开发脚本）。
（sdk 公开状态后于 v3.15 收回。）

## v3.10（2026-09-10）：目录布局修订：Go 标准布局收编

引擎包全部移入 `internal/`（「不承诺三方库 API」由编译器强制，§1.2.8），顶层收敛为
`cmd/stateflux` + `internal/` + `proto/`（仅对外契约；内部契约 `dispatch.proto` 移入
`internal/proto/`），测试基座归并 `internal/test/`（§8，附 §8.1 机械迁移清单）。

## v3.9（2026-09-10）：定位修订：独立服务，而非三方框架库

§1.2.8 重写为「服务优先」，移除嵌入形态承诺；`sdk/` 从「业务方唯一依赖面」调整为引擎共享内核；
执行逻辑经 Handler 在服务构建时注册（§1.2.8/§8）。

## v3.8（2026-09-09）：技术栈约定

数据库操作统一走 ent（entgo.io/ent，Kratos 官方集成指南推荐的 ORM，§3.1）、proto 契约统一
proto3（§3.3）、观测采用 OpenTelemetry 指标采集（§6.4）。

## v3.7：结果集设计

`task_results` 独立结果表（终态事务一次性写入、不可变，保留期与历史归档解耦）+ 执行侧异步结果
WAL（append-only 磁盘日志、Ack 水位截断、重启重放、高水位反压，§3.1/§5.4）。

## 更早

- v3.6：阶段集合逻辑抽象、PG 分区表为默认实现（§3.1）；
- v3.5：调度侧集权/执行侧无状态（§1.2.7）；
- v3.4：调度预处理层（§5.2）；
- 更早：Machinery callback（§5.6）、TaskMessage（§3.3）、创建双模式（§5.1，v3.16 移除 RPC 模式）、
  TaskFactory（§5.6）、目录平铺 + sdk/（§8）。
