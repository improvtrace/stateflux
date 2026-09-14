package schema

import (
	"encoding/json"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskResult holds the schema definition for the TaskResult entity.
// task_results：独立结果集（§3.1/§5.5）——终态事务内一次性写入、不可变（task_id 主键幂等，
// INSERT ... ON CONFLICT DO NOTHING，冲突即重复归集、跳过）；payload 与业务结果都在本表
// （终态时从 task_payloads 合并入行），保留期与 completed 归档策略解耦（独立 TTL/分区）；
// 大 payload/result 同规则改存对象存储引用（§3.1）。
// 字段一行一条；列注释随 ent 生成落到 DDL（entsql.WithComments）；outcome 数值编码见 enums.go。
type TaskResult struct {
	ent.Schema
}

// Fields of the TaskResult.
func (TaskResult) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").StorageKey("task_id").Comment("task ID (snowflake, not auto-increment); idempotent primary key of the immutable result").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.Int8("outcome").Comment("terminal outcome (numeric): 1=succeeded 2=failed 3=dead (0 reserved as unset) (§3.1/§5.5)"),
		field.Int64("attempt").Comment("attempt at write time (fencing token); stale results rejected by it on duplicate collection (§5.5)"),
		field.JSON("payload", json.RawMessage{}).Comment("task payload jsonb merged from task_payloads (same terminal transaction, immutable) (§5.5)").Optional(),
		field.JSON("result", json.RawMessage{}).Comment("business result jsonb; immutable; large results follow the payload rule (§3.1)").Optional(),
		field.String("error").Comment("error message for failure/dead letter (empty when succeeded)").Default(""),
		field.Time("completed_at").Comment("terminal write time (independent basis for result retention TTL/partitioning) (§3.1)"),
	}
}

// Edges of the TaskResult.
func (TaskResult) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + 列注释落 DDL。
func (TaskResult) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_results"},
		entsql.WithComments(true),
	}
}
