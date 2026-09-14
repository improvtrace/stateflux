package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskCompleted holds the schema definition for the TaskCompleted entity.
// task_completeds：统一历史表（§3.1）——outcome ∈ {succeeded, failed, dead}（数值编码见 enums.go）；
// 不内联 payload 与 result（两者都在 task_results，行内仅留 error 摘要与 outcome）；
// 按 created_at 分区 + detach 归档为扩展开关（§11，默认实现为普通表）。
//
// 本表字段独立定义（四阶段表不共享 mixin）、一行一条。字段分公共段与阶段段：
// 公共段 19 列与 pending/schedulable/processing 四表一致，公共列增删四处同步；
// 本表阶段段：含 attempts/claimed_node/error（终态最终值，审计用）与专有列
// outcome/payload/completed_at；不含 hash_bucket（认领后消费完毕）、run_at（调度不判断时间门槛）。
// 列注释随 ent 生成落到 DDL（entsql.WithComments）。
type TaskCompleted struct {
	ent.Schema
}

// Fields of the TaskCompleted.
func (TaskCompleted) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Comment("任务 ID（雪花，客户端生成，非自增）").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").Comment("任务类型：决定 handler 与路由（§5.1）"),
		field.String("operator").Comment("任务子类型（type 的细分）：与 type 一起决定 handler 与路由；终态行保留（§1.2.8/§5.1）").Default(""),
		field.Int8("priority").Comment("调度优先级：0–100 区间（越大越优先，默认 50）；非枚举，调度只依赖排序（§3.1）").Default(int8(DefaultPriority)),
		field.String("channel").Comment("创建时选择的逻辑 channel 名（审计用）（§3.1）").NotEmpty(),
		field.Int64("timeout_ms").Comment("单次执行预算(ms)（审计用）（§10）").Default(60000),
		field.Int32("max_attempts").Comment("最大认领次数（审计用）（§6.2）").Default(3),
		field.Int64("attempts").Comment("已认领次数（终态时的最终值）（§3.1/§9.6）").Default(0),
		field.String("claimed_node").Comment("最后认领的节点（审计用）；执行节点不持有权威状态（§1.2.4）").Default(""),
		field.String("error").Comment("终态错误摘要；完整结果在 task_results（§3.1）").Default(""),
		field.String("idempotency_key").Comment("业务幂等键（溯源与回调派生）；唯一性由 task_identities 裁决（§3.1/§5.1）").Default(""),
		field.JSON("callback", json.RawMessage{}).Comment("OnSuccess/OnError 回调规格 jsonb（§5.6）").Optional(),
		// parent_task_id 与 attempt/outcome 一起构成派生任务的幂等键（§5.6）。
		field.Int64("parent_task_id").Comment("回调派生来源的任务 ID（§5.6）").Default(0),
		field.String("vpc").Comment("调度目标节点属性：目标网络域（VPC 名）；空串=不限；自由文本非枚举（§3.1/§5.3）").Default(""),
		field.String("node").Comment("调度目标节点属性：期望执行节点 ID；空串=任意；自由文本非枚举（§3.1）").Default(""),
		field.String("label").Comment("调度目标节点属性：期望执行节点的匹配标签（审计用）；自由文本非枚举，框架不解释语义（§3.1/§5.2）").Default(""),
		field.Strings("biz_race_labels").Optional().Comment("业务并发约束：业务对象参与的竞争约束标签集合（字符串数组）；空=不限；调度侧据此实施并发控制，机制待定（§3.1/§14）"),
		field.String("biz_race_entry").Comment("业务并发约束：唯一标识业务对象的竞争入口（字符串）；空=不参与；同 entry 的任务由调度侧施加并发约束，机制待定（§3.1/§14）").Default(""),
		field.String("biz_group").Comment("业务属性：业务分组键，供上层业务按组查询/聚合；框架不解释、不参与调度正确性（§3.1）").Default(""),
		field.String("biz_batch_id").Comment("业务属性：工厂批次 ID（业务写入，框架用于孤儿判定，§5.6）").Default(""),
		field.Time("created_at").Comment("创建时间（不可变，也是历史表分区/归档键）（§11）").Immutable().Default(time.Now),
		field.Time("updated_at").Comment("最后更新时间").Default(time.Now).UpdateDefault(time.Now),

		// ---- 以下为 completed 专有 ----
		field.Int8("outcome").Comment("终态类别（数值）：0=未设置 1=succeeded 2=failed 3=dead（§3.1/§5.5）"),
		field.Time("completed_at").Comment("终态写入时间（历史表分区/归档键）（§11）"),
	}
}

// Edges of the TaskCompleted.
func (TaskCompleted) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + priority 区间 CHECK + 列注释落 DDL。
func (TaskCompleted) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_completeds", Checks: map[string]string{"priority_range": PriorityCheck}},
		entsql.WithComments(true),
	}
}
