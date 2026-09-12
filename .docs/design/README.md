# stateflux 设计方案

> 当前版本 **v3.20**（2026-09-12）——v3.19 引入 `internal/eventbus/channel` 统一通信抽象
> （RPC、gRPC stream 与 Redis 都只是它的实现）并把 Redis 收敛为不可靠传输实现；v3.20 新增业务无关
> 调度约束 `vpc`/`node`、`operator`/`group` 业务字段、0–100 优先级区间、数值 `outcome` 枚举与表/列
> 注释。本期不引入 tenant。
> 变更历史见 [changelog.md](./changelog.md)。
>
> 评审后已作**一致性修订**（仅修正交叉引用、术语与骨架差异记录，无设计语义变更），
> 见 [changelog.md](./changelog.md) 的「v3.19 一致性修订」小节；实施前必须定稿的待定项
> 见 [07-implementation.md](./07-implementation.md) §14.6–§14.13。

原单文件设计文档按主题拆分为以下文件；**全文 §N 编号跨文件沿用**——源码注释与 README 中的
§ 引用即指向这些编号，不随拆分改变：

| 文件 | 章节 | 内容 |
| --- | --- | --- |
| [01-principles.md](./01-principles.md) | §1–§2 | 设计准则、工程原则、总体架构与角色模型 |
| [02-data-model.md](./02-data-model.md) | §3–§4 | PG 任务账本、EventBus/Channel 能力、TaskMessage 信封、状态机与可靠性 |
| [03-lifecycle.md](./03-lifecycle.md) | §5 | 四阶段流程：创建/晋升/分发/执行/归集/工厂/回调 |
| [04-mechanisms.md](./04-mechanisms.md) | §6–§7 | 故障恢复矩阵、fence 与对账（R1–R5）、channel 运行规则与反压、观测 + RPC 契约 |
| [05-layout.md](./05-layout.md) | §8 | 模块划分：目录树、依赖方向、可见性规则、布局迁移 |
| [06-decisions.md](./06-decisions.md) | §9–§11 | 关键设计决策、默认参数与压测目标、分布式扩展路径 |
| [07-implementation.md](./07-implementation.md) | §12–§14 | 实施顺序、边界（不做的事）、已确认决策与遗留项 |
| [changelog.md](./changelog.md) | — | 版本历史（v3.4 → v3.19） |

阅读顺序：01 → 02 → 03 为主线；04/05 面向实现；06/07 面向评审与排期。
实现级细节留待实施阶段（§12）。
