package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskPayload holds the schema definition for the TaskPayload entity.
// task_payloads：payload 分离表（§3.1）——阶段表查询面不背 payload；
// 终态搬移时合并入 task_results 后删除分离行（§5.5，同一事务）。
// 字段一行一条；列注释随 ent 生成落到 DDL（entsql.WithComments）。
type TaskPayload struct {
	ent.Schema
}

// Fields of the TaskPayload.
func (TaskPayload) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").StorageKey("task_id").Comment("task ID (snowflake, not auto-increment); same value as stage tables, one-to-one").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.JSON("payload", json.RawMessage{}).Comment("task payload jsonb; immutable after creation; switch to object storage reference beyond 64KB (§3.1/§3.3)"),
		field.Time("created_at").Comment("creation time (immutable)").Immutable().Default(time.Now),
	}
}

// Edges of the TaskPayload.
func (TaskPayload) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + 列注释落 DDL。
func (TaskPayload) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_payloads"},
		entsql.WithComments(true),
	}
}
