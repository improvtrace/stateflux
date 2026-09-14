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
		field.Int64("id").StorageKey("task_id").Comment("任务 ID（雪花，非自增）；与阶段表同值，一一对应").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.JSON("payload", json.RawMessage{}).Comment("任务载荷 jsonb；创建后不可变；>64KB 由业务方改存对象存储引用（§3.1/§3.3）"),
		field.Time("created_at").Comment("创建时间（不可变）").Immutable().Default(time.Now),
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
