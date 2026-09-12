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

// CompletedTask holds the schema definition for the CompletedTask entity.
// completed_tasks：统一历史表（§3.1）——outcome ∈ {succeeded, failed, dead}（数值编码见 enums.go）；
// 内联 payload（便于排查），不内联 result（完整结果在 task_results，行内仅留 error 摘要）；
// 按 created_at 分区 + detach 归档为扩展开关（§11，默认实现为普通表）。
//
// 本表字段独立定义（四阶段表不共享 mixin）、一行一条：前段字段集与 §3.1 的任务行一致，
// 任一列的增删都必须在 pending/schedulable/processing/completed 四处同步；末段为本表专有列。
// 列注释随 ent 生成落到 DDL（entsql.WithComments）。
type CompletedTask struct {
	ent.Schema
}

// Fields of the CompletedTask.
func (CompletedTask) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Comment("任务 ID（雪花，客户端生成，非自增）").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").Comment("任务类型：决定 handler 与路由（§5.1）"),
		field.String("operator").Comment("任务子类型（type 的细分）：与 type 一起决定 handler 与路由；终态行保留（§1.2.8/§5.1）").Default(""),
		field.Int8("priority").Comment("调度优先级：0–100 区间（越大越优先，默认 50）；非枚举，调度只依赖排序（§3.1）").Default(int8(DefaultPriority)),
		field.String("channel").Comment("创建时选择的逻辑 channel 名（审计用）（§3.1）").NotEmpty(),
		field.String("vpc").Comment("目标网络域（业务无关调度约束）；空串=不限；自由文本非枚举（§3.1/§5.3）").Default(""),
		field.String("node").Comment("期望执行节点 ID（业务无关调度约束）；空串=任意；自由文本非枚举（§3.1）").Default(""),
		field.Time("run_at").Comment("原定最早可调度时间（审计用，终态后不再参与调度）"),
		field.Int64("timeout_ms").Comment("单次执行预算(ms)（审计用）（§10）").Default(60000),
		field.Int32("max_attempts").Comment("最大认领次数（审计用）（§6.2）").Default(3),
		field.Int64("attempts").Comment("已认领次数（终态时的最终值）（§3.1/§9.6）").Default(0),
		field.String("owner_node").Comment("实际认领节点（诊断用）；执行节点不持有权威状态（§1.2.4）").Default(""),
		field.String("error").Comment("终态错误摘要；完整结果在 task_results（§3.1）").Default(""),
		field.String("idempotency_key").Comment("业务幂等键（溯源与回调派生）；唯一性由 task_identities 裁决（§3.1/§5.1）").Default(""),
		field.String("group").Comment("业务分组键：供上层业务按组查询/聚合；框架不解释、不参与调度正确性；≠ batch_id、≠ 并发 scope（§3.1）").Default(""),
		field.String("batch_id").Comment("工厂批次 ID，用于孤儿判定（§5.6）").Default(""),
		field.JSON("callback", json.RawMessage{}).Comment("OnSuccess/OnError 回调规格 jsonb（§5.6）").Optional(),
		field.Int64("parent_task_id").Comment("回调派生来源的任务 ID（§5.6）").Default(0),
		field.Time("created_at").Comment("创建时间（不可变，也是历史表分区/归档键）（§11）").Immutable().Default(time.Now),
		field.Time("updated_at").Comment("最后更新时间").Default(time.Now).UpdateDefault(time.Now),

		// ---- 以下为 completed 专有 ----
		field.Int8("outcome").Comment("终态类别（数值）：0=未设置 1=succeeded 2=failed 3=dead（§3.1/§5.5）"),
		field.JSON("payload", json.RawMessage{}).Comment("从 task_payloads 合并入行的 payload（终态同事务，便于排查）（§5.5）").Optional(),
		field.Time("completed_at").Comment("终态写入时间（历史表分区/归档键）（§11）"),
	}
}

// Edges of the CompletedTask.
func (CompletedTask) Edges() []ent.Edge {
	return nil
}

// Indexes 查询索引 (type, created_at)；死信运维 (outcome，§6.2 R3)；工厂孤儿判定 (batch_id，§5.6)；
// group 供上层业务按组查询历史。
func (CompletedTask) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("type", "created_at"),
		index.Fields("outcome"),
		index.Fields("batch_id"),
		index.Fields("group"),
	}
}

// Annotations 显式表名 + priority 区间 CHECK + 列注释落 DDL（表注释见 migration 引导 DDL）。
func (CompletedTask) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "completed_tasks", Checks: map[string]string{"priority_range": PriorityCheck}},
		entsql.WithComments(true),
	}
}
