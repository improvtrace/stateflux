# stateflux 设计 · 实施顺序、边界与已确认决策（§12–§14）

> v3.20（2026-09-12）。§ 编号全库沿用，文件映射见 [README](./README.md)。

## 12. 实施顺序

> **当前进度**（2026-09-12 评审后）：第 1 项已完成（`internal/eventbus/channel` 的 Send/Subscribe
> 与 capability 契约 + in-memory fake）；第 2 项部分完成——四阶段、payload/result 与
> `task_identities` 的 ent schema 均已建（identity 表于 2026-09-14 补齐建模；并发名额与控制租约
> 机制已随 v3.21 移除）。骨架与本文档的已知差异（实施第 2 项时一并修正）：各阶段表上的
> `idempotency_key` 局部唯一索引方案应随 `task_identities` 账本退役（§3.1）。
> 已完成对齐（2026-09-12）：任务行的 `channel`（§3.1/§5.3）已落到 ent schema，v3.18 遗留的
> `exec_mode`(sync/async) 已移除——同步/异步由被解析 channel 的 Capabilities 推导
> （§3.2、§14.1），换 Redis 数据结构或换 transport 都不改状态机。v3.20 又在 schema 落了
> `vpc`/`node`（业务无关调度约束）、`operator`/`group`（type 子类型与业务分组键）、`priority` 的
> 0–100 区间（含 CHECK）与数值 `outcome`、全列注释；表注释机制已随 v3.21 移除（ent 不支持
> 表级注释）。

1. 定义 TaskMessage、ResultEvent、`eventbus/channel` 的 Send/Subscribe 与 capability 契约；先实现
   in-memory fake，作为所有运行时测试基座。
2. 实现 PG identity、四阶段、result，以及 claim/terminal/R1 SQL；
   测试丢失或重复 ResultEvent 不破坏终态。
3. 实现 EventBus adapters：unary RPC request/reply、gRPC 双向 ResultStream、Redis list/zset/stream/
   pubsub best-effort adapters。adapter 测试只验证协议行为，不验证 Redis 持久性。
4. 实现 worker：订阅 task、执行 handler、WAL、重连后重发 ResultEvent；实现 Scheduler channel 选择。
5. 实现 Collector result 订阅与批终态；RPC 返回结果必须走同一 ResultEvent 入口。
6. 实现 R1–R4 与故障演练：发送前/后断开、重复消息、所有 Redis 数据删除、ResultStream 断开、双主。
7. 接入 ClusterView、OTel、压测；最后才增加 topic/shard 与新的 adapter。

## 13. 边界

- 不承诺 exactly-once；Redis/RPC 可靠性不属于任务正确性前提。
- 不引入 tenant、多租户权限或 tenant 级配额；idempotency key 由接入方全局命名。
- 不把 Redis ACK/PEL/AOF、RPC 成功返回或 worker 本地状态视为权威。
- 不实现业务创建 RPC、取消、workflow 编排或外部选举服务。

## 14. 已确认决策与遗留项

1. `internal/eventbus/channel` 是唯一节点通信抽象；实现可为 RPC、RPC stream、Redis list/zset/stream/
   pubsub，并标注单向、半双工或全双工能力。
2. 默认同步任务走 unary RPC request/reply；默认异步任务走 Redis channel；二者都以 ResultEvent 归集。
3. 默认结果归集走 worker 与调度节点间的 gRPC 双向 stream，Collector 订阅 EventBus；Redis 结果通道是
   可替换实现。
4. Redis 仅改善通信和削峰，完全丢失时以 PG 对账重跑。
5. v2 候选：外部 Worker 协议、取消/暂停、分片和显式租户模型。

**以下为实施前必须定稿的待定项（v3.19 评审遗留，尚无结论）：**

6. **故障切换后在途 processing 的接管策略**：由 R1 按时间统一重置收敛（无 epoch 接管机制）；
   是否需要更快的接管路径待定（§6.2）。
7. **WAL 回收策略**：`LocalAck=false` 的通道下按时间/大小截断的参数，并显式声明「未确认结果可有
   意丢弃、由 R1 兜底」，否则 WAL 会无界增长（§5.4、§6.3）。
8. **`timeout_ms` 与 `dispatch_grace` 的约束关系**：R1 取 `max()` 时旧稿的「必须小于 grace」不是
    安全必要条件，需定稿为强校验、上限还是仅告警；worker 端超时强制与放弃语义也需写明（§5.4、§6.2）。
9. **R4 的触发信号与作用域**：「所有 channel 不可用」如何判定，以及它与 R1 + 调度常规循环的职责
    边界；否则应并入 R1（§6.1、§6.2）。
10. **`enqueue_tasks` 契约冻结**：函数签名、payload 传参方式、返回结构与权限（EXECUTE +
    `task_results` 只读）。业务侧无 RPC（§13），该函数是唯一对外接入面（§1.2.8、§5.1）。
11. **§10 待定参数**：worker credit 的语义与上报路径、R1 的 `skew` 默认值、dedupe 窗口长度、对账
    周期（R1 扫描间隔）、NOTIFY 通道名；callback 深度上限 8 是否入表。
12. **biz_race 并发约束的调度机制**：`biz_race_labels`/`biz_race_entry` 的消费方式——在晋升还是
    认领时实施、互斥粒度（同 entry 串行？同 label 集合互斥？）与等待/跳过策略（§3.1/§5.2）。
