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
		field.Int64("id").Comment("task ID (snowflake, client-generated, not auto-increment)").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").Comment("task type: determines handler and routing (§5.1)"),
		field.String("operator").Comment("task subtype (refinement of type): together with type determines handler and routing; retained in terminal rows (§1.2.8/§5.1)").Default(""),
		field.Int8("priority").Comment("scheduling priority: 0-100 range (higher first, default 50); not an enum, scheduling relies on ordering only (§3.1)").Default(int8(DefaultPriority)),
		field.String("channel").Comment("logical channel name chosen at creation (audit) (§3.1)").NotEmpty(),
		field.Int64("timeout_ms").Comment("per-execution budget in ms (audit) (§10)").Default(60000),
		field.Int32("max_attempts").Comment("max claim count (audit) (§6.2)").Default(3),
		field.Int64("attempts").Comment("claim count (final value at terminal state) (§3.1/§9.6)").Default(0),
		field.String("claimed_node").Comment("claiming node (last claim, audit); executors hold no authoritative state (§1.2.4)").Default(""),
		field.String("error").Comment("terminal error summary; full result in task_results (§3.1)").Default(""),
		field.String("idempotency_key").Comment("business idempotency key (provenance and callback derivation); uniqueness ruled by task_identities (§3.1/§5.1)").Default(""),
		field.JSON("callback", json.RawMessage{}).Comment("OnSuccess/OnError callback spec as jsonb (§5.6)").Optional(),
		// parent_task_id 与 attempt/outcome 一起构成派生任务的幂等键（§5.6）。
		field.Int64("parent_task_id").Comment("task ID of the callback origin this task derives from (§5.6)").Default(0),
		field.String("vpc").Comment("scheduling target-node attribute: target network domain (VPC name); empty = unrestricted; free text, not an enum (§3.1/§5.3)").Default(""),
		field.String("node").Comment("scheduling target-node attribute: preferred executor node ID; empty = any; free text, not an enum (§3.1)").Default(""),
		field.String("label").Comment("scheduling target-node attribute: node matching label (audit); free text, not interpreted by the framework (§3.1/§5.2)").Default(""),
		field.Strings("biz_race_labels").Optional().Comment("business concurrency constraint: set of race constraint labels the business object joins (string array); empty = unrestricted; scheduler enforces concurrency control by it, mechanism TBD (§3.1/§14)"),
		field.String("biz_race_entry").Comment("business concurrency constraint: string uniquely identifying the business object (race entry); empty = not participating; tasks sharing an entry are concurrency-constrained by the scheduler, mechanism TBD (§3.1/§14)").Default(""),
		field.String("biz_group").Comment("business attribute: business grouping key for upstream query/aggregation; not interpreted by the framework, not involved in scheduling correctness (§3.1)").Default(""),
		field.String("biz_batch_id").Comment("business attribute: factory batch ID (written by business; used for orphan detection, §5.6)").Default(""),
		field.Time("created_at").Comment("creation time (immutable; also history partition/archive key) (§11)").Immutable().Default(time.Now),
		field.Time("updated_at").Comment("last update time").Default(time.Now).UpdateDefault(time.Now),

		// ---- 以下为 completed 专有 ----
		field.Int8("outcome").Comment("terminal outcome (numeric): 0=unset 1=succeeded 2=failed 3=dead (§3.1/§5.5)"),
		field.Time("completed_at").Comment("terminal write time (history partition/archive key) (§11)"),
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
