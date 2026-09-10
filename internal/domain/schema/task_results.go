package schema

import (
	"encoding/json"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskResult holds the schema definition for the TaskResult entity.
// task_results：独立结果集（§3.1/§14.7）——终态事务内一次性写入、不可变（task_id 主键幂等，
// INSERT ... ON CONFLICT DO NOTHING，冲突即重复归集、跳过）；业务结果的保留期与 completed
// 归档策略解耦（独立 TTL/分区）；completed 不内联 result；大 result 同 payload 规则（§3.1）。
type TaskResult struct {
	ent.Schema
}

// Fields of the TaskResult.
func (TaskResult) Fields() []ent.Field {
	return []ent.Field{
		// task_id 主键（§3.1，雪花生成非自增）；id 字段经 StorageKey 映射为 task_id 列。
		field.Int64("id").StorageKey("task_id").
			Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("outcome"), // succeeded/failed/dead
		// 写入时的 attempt：fencing token，重复归集校验依据（§5.5）。
		field.Int64("attempt"),
		field.JSON("result", json.RawMessage{}).Optional(),
		field.String("error").Default(""),
		field.Time("completed_at"),
	}
}

// Edges of the TaskResult.
func (TaskResult) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名。
func (TaskResult) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "task_results"}}
}
