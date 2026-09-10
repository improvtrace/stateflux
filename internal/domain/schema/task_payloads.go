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
// 终态搬移时合并入 completed 后删除分离行（§5.5，同一事务）。
type TaskPayload struct {
	ent.Schema
}

// Fields of the TaskPayload.
func (TaskPayload) Fields() []ent.Field {
	return []ent.Field{
		// task_id 主键（§3.1，雪花生成非自增）；id 字段经 StorageKey 映射为 task_id 列。
		field.Int64("id").StorageKey("task_id").
			Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		// 任务载荷（jsonb；创建后不可变，§3.3 内联安全；>64KB 业务方走对象存储引用，§3.1）。
		field.JSON("payload", json.RawMessage{}),
		field.Time("created_at").Immutable().Default(time.Now),
	}
}

// Edges of the TaskPayload.
func (TaskPayload) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名。
func (TaskPayload) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "task_payloads"}}
}
