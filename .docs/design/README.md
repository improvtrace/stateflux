# stateflux 设计方案

> 当前版本 **v3.17**（2026-09-11）——控制面运行时归组目录更名（controller → runtime）。
> 变更历史见 [changelog.md](./changelog.md)。

原单文件设计文档按主题拆分为以下文件；**全文 §N 编号跨文件沿用**——源码注释与 README 中的
§ 引用即指向这些编号，不随拆分改变：

| 文件 | 章节 | 内容 |
| --- | --- | --- |
| [01-principles.md](./01-principles.md) | §1–§2 | 设计准则、工程原则、总体架构与角色模型 |
| [02-data-model.md](./02-data-model.md) | §3–§4 | 任务存储模型（PG/Redis）、TaskMessage、状态机与投递语义 |
| [03-lifecycle.md](./03-lifecycle.md) | §5 | 四阶段流程：创建/晋升/分发/执行/归集/回执/工厂/回调 |
| [04-mechanisms.md](./04-mechanisms.md) | §6–§7 | 故障恢复、墓碑、对账、反压、观测 + RPC 契约 |
| [05-layout.md](./05-layout.md) | §8 | 模块划分：目录树、依赖方向、可见性规则、布局迁移 |
| [06-decisions.md](./06-decisions.md) | §9–§11 | 关键设计决策、默认参数与容量模型、分布式扩展路径 |
| [07-implementation.md](./07-implementation.md) | §12–§14 | 实施顺序、边界（不做的事）、已确认决策与遗留项 |
| [changelog.md](./changelog.md) | — | 版本历史（v3.4 → v3.17） |

阅读顺序：01 → 02 → 03 为主线；04/05 面向实现；06/07 面向评审与排期。
实现级细节留待实施阶段（§12）。
