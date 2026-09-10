# stateflux 设计 · 数据模型与状态机（§3–§4）

> v3.16（2026-09-10）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 3. 数据模型

### 3.1 任务存储模型（逻辑阶段集合 + PG 默认实现）

生命周期四阶段首先是**逻辑抽象**：Pending / Schedulable / Processing / Completed 四个阶段集合，
由 `internal/domain` 以聚合接口暴露（创建入集、约束晋升、认领挪行、终态搬移、对账扫描；
Task/Result/Ops 三个聚合接口 + 组合根 Store，§8）。**PG 四阶段
分区表 + payload 分离表是该接口的默认实现**，整体可替换（如退回单表 + 状态列）而不改变调度与
执行语义；下文沿用表名指代各逻辑阶段。默认实现中，生命周期阶段由**所在表**表达（`status` 列
取消），四张阶段表共享同一套核心字段：

`id`（雪花）、`type`、`priority`、`exec_mode`（sync/async）、`run_at`（最早可调度时间，承载
延迟/重试退避）、`timeout_ms`、`max_attempts`/`attempts`、`owner_node`、`error`（终态错误
摘要，完整结果在 `task_results`）、`idempotency_key`（业务幂等键）、`batch_id`（批量创建归组）、
`callback`（jsonb，OnSuccess/OnError 回调规格，§5.8）、`parent_task_id`（回调派生溯源）、时间戳。

| 表 | 语义 | 要点 |
| --- | --- | --- |
| `task_payloads` | payload 分离表：`task_id` + `payload`（jsonb） | 阶段表查询面不背 payload；终态搬移时合并入 completed 后删除 |
| `pending_tasks` | 已创建，等待调度预处理 | 约束晋升的扫描对象（§5.2） |
| `schedulable_tasks` | 就绪可调度（逻辑就绪集），量级显著小于 pending | 调度认领的扫描对象；可调度性由并发约束与业务约束在晋升时判定 |
| `processing_tasks` | 已调度（在队列/inprocess/分发协程中） | 认领与**重试逻辑**所在；已入本表的任务不可取消 |
| `completed_tasks` | 统一历史表，`outcome ∈ {succeeded, failed, dead}` | 内联 payload（便于排查），**不内联 result**（完整结果在 `task_results`，行内仅留 error 摘要）；按 `created_at` 分区 + detach 归档 |
| `task_results` | 结果集：`task_id` 主键 + `outcome`/`attempt`/`result`（jsonb）/`error`/`completed_at` | 终态事务内**一次性写入、不可变**；业务结果的保留期与 completed 归档策略**解耦**（独立 TTL/分区）；大 result 同 payload 规则（>64KB 走对象存储引用） |

- **每任务全生命周期约 5 次批量行操作**：INSERT pending → 约束晋升挪 schedulable → 认领挪
  processing（attempts+1）→ 终态事务双行（completed 行含 payload 合并 + task_results 行）。
  行操作全部批量化，换取热表小、扫描快、历史天然分离（§9.2、§10）。
- 认领 = `schedulable_tasks` → `processing_tasks` 的单条 `UPDATE ... WHERE id IN (SELECT ...
  FOR UPDATE SKIP LOCKED)` 挪行，查询与迁移原子完成，天然防重复认领；终态搬移**attempt 匹配
  才生效**，重复搬移无副作用（幂等）。
- 超过 `max_attempts` 记 `dead`（outcome），进 completed_tasks 便于排查。
- 索引：pending 晋升扫描 `(priority DESC, run_at)`；schedulable 认领部分索引；processing 对账
  `(updated_at)`；completed 查询 `(type, created_at)`。
- 幂等键唯一性需覆盖非终态三表（实施阶段给出方案：创建时查重 + 各表局部唯一索引）。
- `task_results` 与终态搬移同事务写入（INSERT ... ON CONFLICT DO NOTHING，冲突即重复归集、
  跳过），写入后不可变；结果查询先查本表，未命中再查阶段表判断在途状态（§5.6）。
- 大 payload（>64KB）不入库，业务方写对象存储后传引用。
- **实现载体 = ent**：六张表以 ent schema（entgo.io/ent）定义为代码，迁移由代码生成；认领挪行、
  终态事务、`ON CONFLICT DO NOTHING` 等并发关键路径以 ent 原生 SQL 下沉（Modify/raw），不依赖
  ORM 生成的隐式语句；常规读写走 ent 生成的类型安全 API。业务直写接入（§5.1）不受影响——业务方
  仍可用任意客户端直写任务表组，ent schema 只约束框架侧实现。

### 3.2 Redis 结构

| key | 结构 | 用途 |
| --- | --- | --- |
| `stateflux:queue:{pri}` | LIST | 异步就绪队列，默认按 high/normal/low 三级拆分；消息体 = proto TaskMessage（内联完整任务，§3.3） |
| `stateflux:inprocess` | ZSET | 共识集合：member=task_id，score=租约到期时间 |
| `stateflux:inprocess:detail` | HASH | member → {node_id, attempt, started_at}，归属校验与观测 |
| `stateflux:done:{task_id}` | STRING | 终态墓碑，TTL 自动回收 |
| `stateflux:node:{node_id}` | HASH | 执行节点容量上报（free_slots），供选节点与观测 |

**inprocess 集合的共识语义**（正确性核心）：

- 成员定义：**已被认领、但尚未在 PG 落终态的任务**（执行中 + 执行完但结果未归集）。
- 集合操作全部原子且带归属校验：注册时检查墓碑与已有成员（重复投递/陈旧副本被拒绝），续约与移除
  要求 node_id + attempt 匹配——僵尸节点无法覆盖新尝试的记录。
- Redis 全量丢失可由对账从 PG 重建；重建后正在执行的任务可能被重复派发，由 at-least-once + 业务幂等收敛。

### 3.3 消息载体（proto TaskMessage）

跨节点传输的任务载体由 protobuf 统一定义（`api/stateflux/task/v1`，**proto3 语法**），字段借鉴
Machinery 的 `Signature`（UUID/Name/ETA/Priority/Headers）并适配本框架；内部（dispatch）与外部
（cluster）契约统一 proto3，可选字段（如 `trace_headers`）用 `optional` 显式表达存在性：

| 字段 | 说明 |
| --- | --- |
| `task_id` | 雪花 ID，与 PG 行对应 |
| `attempt` | fencing token，执行侧注册与回写校验的依据 |
| `type` / `payload` | 任务类型与载荷 |
| `priority` / `timeout_ms` / `deadline` | 执行控制 |
| `trace_headers` | 链路追踪传播（Machinery Headers 同款用途） |

- **内联完整任务**：任务创建后 payload 不可变，消息内联安全；大 payload（>64KB）本就走对象存储
  引用。执行侧 BRPOP 后零 PG 读，Redis 消息是「可执行的认领凭证」，PG 仍是状态与 payload 的
  唯一权威。
- **sync/async 统一载体**：异步队列 LPUSH 与同步 `Execute` RPC 请求共用同一信封，减少重复定义、
  便于统一注入 trace 与 attempt 语义。

## 4. 状态机与投递语义

```
pending_tasks ──约束晋升(§5.2)──▶ schedulable_tasks ──认领(CLAIM, attempts+1)──▶ processing_tasks
                                      ▲      │  终态搬移(§5.5, attempt 校验)   │        │
                                      │      └─重试/R1重置(attempt+1, 退避)──┘        ├─▶ completed_tasks{succeeded / failed}
                                      │                                              └─attempts 耗尽─▶ completed_tasks{dead}
```

- 投递语义 **at-least-once**：BRPOP 后注册前宕机、Redis 丢数据重建、终态搬移前执行节点宕机等窗口
  都会导致重复执行，业务 handler 必须按 `idempotency_key`（或 task_id + attempt）幂等。
- `processing_tasks` 中的任务表示「已认领」；任务此刻在队列里、inprocess 里、或同步分发协程中，
  精确位置以 Redis 为准。
- 取消边界：仅 `pending_tasks` / `schedulable_tasks` 中的任务可取消（v2 提供），已入
  `processing_tasks` 不可取消。
