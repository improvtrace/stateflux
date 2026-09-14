package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskSchedulable holds the schema definition for the TaskSchedulable entity.
// task_schedulables：就绪可调度（逻辑就绪集），量级显著小于 pending——调度认领的扫描对象；
// 可调度性由并发约束与业务约束在晋升时判定（§5.2）。重试/对账重置的任务也回到本表
// （约束已通过不重评，§5.5）。认领挪行为单条原生 SQL（FOR UPDATE SKIP LOCKED，§3.1）。
//
// 本表字段独立定义（四阶段表不共享 mixin）、一行一条。字段分公共段与阶段段：
// 公共段 19 列与 pending/processing/completed 四表一致，公共列增删四处同步；
// 本表阶段段：含 attempts/claimed_node（重置后保留上次认领值）与 hash_bucket（认领时过滤），
// 不含 error（终态才写）、run_at（调度不判断时间门槛，§5.2）。
// 列注释随 ent 生成落到 DDL（entsql.WithComments）；枚举与区间见 enums.go。
type TaskSchedulable struct {
	ent.Schema
}

// Fields of the TaskSchedulable.
func (TaskSchedulable) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Comment("task ID (snowflake, client-generated, not auto-increment)").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").Comment("task type: determines handler and routing (§5.1)"),
		field.String("operator").Comment("task subtype (refinement of type): together with type determines handler and routing (§1.2.8/§5.1)").Default(""),
		field.Int8("priority").Comment("scheduling priority: 0-100 range (higher first, default 50); not an enum, scheduling relies on ordering only (§3.1)").Default(int8(DefaultPriority)),
		field.String("channel").Comment("logical channel name; the scheduler resolves the eventbus implementation by it; sync/async derived from channel capabilities (§3.1/§5.3)").NotEmpty(),
		field.Int64("timeout_ms").Comment("per-execution budget (ms); R1 threshold = max(timeout_ms, dispatch_grace)+skew (§5.4/§6.2)").Default(60000),
		field.Int32("max_attempts").Comment("max claim count; over the limit goes to dead letter via R3 (§6.2)").Default(3),
		field.Int64("attempts").Comment("claim count; +1 on each claim, used as execution fence (§3.1/§9.6)").Default(0),
		field.String("claimed_node").Comment("claiming node (last claim, diagnostic); preserved after reset; executors hold no authoritative state (§1.2.4)").Default(""),
		field.String("idempotency_key").Comment("business idempotency key (provenance and callback derivation); uniqueness ruled by task_identities (§3.1/§5.1)").Default(""),
		field.JSON("callback", json.RawMessage{}).Comment("OnSuccess/OnError callback spec as jsonb; depth limit validated on write (§5.6)").Optional(),
		// parent_task_id 与 attempt/outcome 一起构成派生任务的幂等键（§5.6）。
		field.Int64("parent_task_id").Comment("task ID of the callback origin this task derives from (§5.6)").Default(0),
		field.String("vpc").Comment("scheduling target-node attribute: target network domain (VPC name); empty = unrestricted; free text, not an enum (§3.1/§5.3)").Default(""),
		field.String("node").Comment("scheduling target-node attribute: preferred executor node ID; empty = any; free text, not an enum (§3.1)").Default(""),
		field.String("label").Comment("scheduling target-node attribute: node matching label, filtered at claim; empty = unrestricted; free text, not interpreted by the framework (§3.1/§5.2)").Default(""),
		field.Int16("hash_bucket").Comment("scheduling target-node attribute: hash bucket (0-255), 0 = unrestricted; semantics defined by the integrator, framework does exact matching only; filtered at claim (§3.1/§5.2)").Default(0),
		field.Strings("biz_race_labels").Optional().Comment("business concurrency constraint: set of race constraint labels the business object joins (string array); empty = unrestricted; scheduler enforces concurrency control by it, mechanism TBD (§3.1/§14)"),
		field.String("biz_race_entry").Comment("business concurrency constraint: string uniquely identifying the business object (race entry); empty = not participating; tasks sharing an entry are concurrency-constrained by the scheduler, mechanism TBD (§3.1/§14)").Default(""),
		field.String("biz_group").Comment("business attribute: business grouping key for upstream query/aggregation; not interpreted by the framework, not involved in scheduling correctness (§3.1)").Default(""),
		field.String("biz_batch_id").Comment("business attribute: factory batch ID (written by business; used for orphan detection, §5.6)").Default(""),
		field.Time("created_at").Comment("creation time (immutable)").Immutable().Default(time.Now),
		field.Time("updated_at").Comment("last update time; scan basis for R1 reconciliation (§6.2)").Default(time.Now).UpdateDefault(time.Now),
	}
}

// Edges of the TaskSchedulable.
func (TaskSchedulable) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + priority/hash_bucket 区间 CHECK + 列注释落 DDL。
func (TaskSchedulable) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_schedulables", Checks: map[string]string{"priority_range": PriorityCheck, "bucket_range": BucketCheck}},
		entsql.WithComments(true),
	}
}
