package schema

import (
	"encoding/json"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// CompletedTask holds the schema definition for the CompletedTask entity.
// completed_tasks：统一历史表（§3.1）——outcome ∈ {succeeded, failed, dead}；内联 payload
// （便于排查），不内联 result（完整结果在 task_results，行内仅留 error 摘要）；
// 按 created_at 分区 + detach 归档为扩展开关（§11，默认实现为普通表）。
type CompletedTask struct {
	ent.Schema
}

// Fields of the CompletedTask.
func (CompletedTask) Fields() []ent.Field {
	return []ent.Field{
		field.String("outcome"), // succeeded/failed/dead
		// 终态搬移时从 task_payloads 合并入行（§5.5，同一事务）。
		field.JSON("payload", json.RawMessage{}).Optional(),
		field.Time("completed_at"),
	}
}

// Edges of the CompletedTask.
func (CompletedTask) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (CompletedTask) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 查询索引 (type, created_at)；死信运维 (outcome，§12.9)；工厂孤儿判定 (batch_id，§5.7)。
func (CompletedTask) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("type", "created_at"),
		index.Fields("outcome"),
		index.Fields("batch_id"),
	}
}

// Annotations 显式表名。
func (CompletedTask) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "completed_tasks"}}
}
