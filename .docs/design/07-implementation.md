# stateflux 设计 · 实施顺序、边界与已确认决策（§12–§14）

> v3.16（2026-09-10）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 12. 实施顺序

1. domain 内核（模型 + Handler 契约，随 domain 包）+ proto 契约（`api/stateflux/task/v1`，proto3，
   含 TaskMessage 统一信封与回调规格）+ config（含 OTel MeterProvider/TracerProvider 装配参数）；
2. store：先定义阶段集合逻辑接口（创建入集/晋升/认领挪行/终态搬移/结果写入/对账扫描），再以默认
   实现落地——ent schema 定义六张表（task_payloads + task_results + 四阶段表）+ ent 迁移 + 各
   操作 SQL（关键并发路径原生下沉；并发认领单测：只允许一个成功；僵尸节点用例：attempt 过期后
   写终态必须被拒）；
3. cacheview（`domain/cacheview`）：inprocess 集合 Lua（注册/续约/移除，含墓碑与归属校验）+ 就绪队列；
4. worker：task/v1 gRPC server + biz（Execute/Collect 服务端业务）+ Handler 注册表 + 消费循环 +
   结果 WAL（落盘/重放/水位反压）；
5. scheduler（`internal/controller/scheduler`）：单调度节点闭环（约束晋升 → 自适应认领 → sync
   分发池 / async LPUSH）+ 统一结果缓冲；
6. collector（`internal/controller/collector`）：Collect 拉取 → 终态事务（processing→completed 搬移，
   payload 合并 + task_results 写入 + 回调派生，同一事务；单测：重复归集不重复写结果、不重复派生）
   → 墓碑 → Ack（僵尸节点归集上报必须被拒）；
7. reconcile（`internal/controller/reconcile`）：R1~R4（退避 full jitter）+ 进程全灭重启的收敛验证；
8. cluster.static 单机闭环 → 接入外部 ClusterView，验证调度节点故障切换；
9. 死信运维：dead 直查与 redrive（人工修复后重跑，不自动重放；SQL 直查，不设运维 RPC）；
10. OTel 观测埋点（§6.5 指标清单 + tracing）+ 压测调优（batch/tick/lease/grace 参数扫描；监控 PG
    autovacuum 与阶段表膨胀，fillfactor/HOT update 预留调优）。最小指标集见 §6.5：队列深度、
    inprocess 大小、claim→投递延迟、归集延迟、R1 重置次数（核心健康信号）、墓碑/attempt 拒绝数；
11. 按需启用扩展开关：调度分片、队列按 type 拆分、completed_tasks 分区归档。

## 13. 边界（不做的事）

- 不承诺 exactly-once：at-least-once 交付，业务 handler 必须幂等；
- 不强制杀死执行到一半的任务，只做超时 ctx 与优雅退出；
- 不实现业务执行器、选举服务与成员协议（均为外部/业务侧接入点）；
- PG 与 Redis 的 HA 属于部署域，不在框架范围内；
- 不承诺跨队列/跨分片全局有序，只在同队列内按优先级 + run_at FIFO；
- 不实现 chain/group/chord 编排原语：回调仅提供 OnSuccess/OnError 派生语义（§5.8），可嵌套规格
  覆盖链式场景，任务组聚合不属于框架范围；
- 取消（cancel）能力本期不做，v2 优先补齐；v2 取消仅覆盖 pending/schedulable 中的任务，已入
  processing 不可取消（见 §14 已确认决策）。

## 14. 已确认决策与遗留项（2026-09 评审）

### 已确认

1. **同步任务的创建方回执**：长轮询保底，Redis pub/sub 精准唤醒作为可选项实现（v3.16 收敛：
   长轮询随业务接入 RPC 移除，创建方直查 `task_results`，pub/sub 保留为预留扩展）；
2. **inprocess 成员语义**：维持「已认领未终态」（含结果未归集），避免归集尾部窗口的对账模糊区间；
3. **attempts 计数时机**：认领时 +1；「对账重置消耗一次重试预算」写入业务方文档，不为精确计数
   增加每任务 PG 写；
4. **取消能力**：本期不做，v2 优先补齐——同类框架（Asynq/Temporal/Celery 等）均将 cancel 视为
   必备能力；
5. **队列粒度**：默认按 priority 三级，出现类型级热点后再按 type 拆分（扩展开关原则）；
6. **任务表组四阶段分区模型**：pending/schedulable/processing/completed + payload 分离 + 统一
   历史表（2026-09 评审）；重试/对账重置回 schedulable（约束不重评）；v2 取消仅覆盖
   pending/schedulable，已入 processing 不可取消；
7. **结果集与执行侧 WAL**：task_results 独立表（终态事务一次性写入、不可变，completed 不内联
   result）；执行侧异步结果以磁盘 WAL 承载（Ack 水位截断、重启重放、高水位反压，仍非权威状态）
   （2026-09 评审）。
8. **技术栈约定**（2026-09-09 评审）：数据库操作统一走 ent（entgo.io/ent，Kratos 官方集成指南
   推荐的 ORM；schema 即代码，关键并发 SQL 原生下沉，§3.1）；proto 契约统一 proto3（§3.3）；
   观测统一 OpenTelemetry（指标 + tracing，§6.5）。
9. **定位：独立服务（2026-09-10 评审）**：stateflux 交付为独立部署的调度服务（单一官方二进制），
   不作为三方框架库、不支持嵌入业务进程（§1.2.8 服务优先）；业务经直写任务表组跨进程接入
   （§5.1；v3.16 收敛为直写单模式），执行逻辑以 Handler 在服务构建时注册（§7）。

### 遗留

1. **执行接入的演进**（定位修订的连带项，§1.2.8）：当前业务执行逻辑需在构建时注册 Handler
   （自定义构建）；独立服务形态下的演进方向——HTTP 回调执行器（服务直接回调业务 HTTP 端点）
   或 Worker 拉取协议（业务 Worker 进程接入拉取执行），v2 评估择一或并存。

### 遗留（v2 候选）

- **Redis Streams（`XAUTOCLAIM`）作为异步链路备选开关**：原生覆盖「BRPOP 后崩溃」窗口，把重复
  执行窗口从 grace(90s) 缩到 claim 超时；v1 维持 LIST + 对账；
- **执行中取消**：cancel 表 + 黑名单 + version 传播（v1 旧方案的完整设计）；
- 跨队列/跨分片全局有序（§13 边界，长期不做）。
