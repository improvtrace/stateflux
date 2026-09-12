package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// PendingTask holds the schema definition for the PendingTask entity.
// pending_tasks：已创建，等待调度预处理——约束晋升的扫描对象（§5.2）；可取消边界内（§4）。
//
// 本表字段独立定义（四阶段表不共享 mixin）、一行一条：字段集与 §3.1 的任务行一致，
// 任一列的增删都必须在 pending/schedulable/processing/completed 四处同步。
// 列注释随 ent 生成落到 DDL（entsql.WithComments）；枚举与区间见 enums.go。
type PendingTask struct {
	ent.Schema
}

// Fields of the PendingTask.
func (PendingTask) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Comment("任务 ID（雪花，客户端生成，非自增）").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").Comment("任务类型：决定 handler 与路由（§5.1）"),
		field.String("operator").Comment("任务子类型（type 的细分）：与 type 一起决定 handler 与路由（§1.2.8/§5.1）").Default(""),
		field.Int8("priority").Comment("调度优先级：0–100 区间（越大越优先，默认 50）；非枚举，调度只依赖排序（§3.1）").Default(int8(DefaultPriority)),
		field.String("channel").Comment("逻辑 channel 名，调度侧据此解析 eventbus 实现；同步/异步由 channel 能力推导（§3.1/§5.3）").NotEmpty(),
		field.String("vpc").Comment("目标网络域（业务无关调度约束）；空串=不限；自由文本非枚举（§3.1/§5.3）").Default(""),
		field.String("node").Comment("期望执行节点 ID（业务无关调度约束）；空串=任意；自由文本非枚举；实际认领节点见 owner_node（§3.1）").Default(""),
		field.Time("run_at").Comment("最早可调度时间：承载延迟任务与重试退避（§5.5）"),
		field.Int64("timeout_ms").Comment("单次执行预算(ms)；R1 兜底阈值取 max(timeout_ms, dispatch_grace)+skew（§5.4/§6.2）").Default(60000),
		field.Int32("max_attempts").Comment("最大认领次数；超限由 R3 写死信（§6.2）").Default(3),
		field.Int64("attempts").Comment("已认领次数；claim 时 +1，作为执行 fence（§3.1/§9.6）").Default(0),
		field.String("owner_node").Comment("实际认领节点（诊断用）；执行节点不持有权威状态（§1.2.4）").Default(""),
		field.String("error").Comment("终态错误摘要；完整结果在 task_results（§3.1）").Default(""),
		field.String("idempotency_key").Comment("业务幂等键（溯源与回调派生）；唯一性由 task_identities 裁决（§3.1/§5.1）").Default(""),
		field.String("group").Comment("业务分组键：供上层业务按组查询/聚合；框架不解释、不参与调度正确性；≠ batch_id、≠ 并发 scope（§3.1）").Default(""),
		field.String("batch_id").Comment("工厂批次 ID，用于孤儿判定（§5.6）").Default(""),
		field.JSON("callback", json.RawMessage{}).Comment("OnSuccess/OnError 回调规格 jsonb；深度上限由写入侧校验（§5.6）").Optional(),
		// parent_task_id 与 attempt/outcome 一起构成派生任务的幂等键（§5.6）。
		field.Int64("parent_task_id").Comment("回调派生来源的任务 ID（§5.6）").Default(0),
		field.Time("created_at").Comment("创建时间（不可变）").Immutable().Default(time.Now),
		field.Time("updated_at").Comment("最后更新时间；R1/R5 对账扫描依据（§6.2）").Default(time.Now).UpdateDefault(time.Now),
	}
}

// Edges of the PendingTask.
func (PendingTask) Edges() []ent.Edge {
	return nil
}

// Indexes 晋升扫描索引 (priority DESC, run_at)；group 供上层业务按组查询；idempotency_key 普通索引
// 供排查与溯源（不承担唯一性，唯一性见 task_identities）。
func (PendingTask) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("priority", "run_at").Annotations(entsql.DescColumns("priority")),
		index.Fields("group"),
		index.Fields("idempotency_key"),
	}
}

// Annotations 显式表名 + priority 区间 CHECK + 列注释落 DDL（表注释见 migration 引导 DDL）。
func (PendingTask) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "pending_tasks", Checks: map[string]string{"priority_range": PriorityCheck}},
		entsql.WithComments(true),
	}
}
