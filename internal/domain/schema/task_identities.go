package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskIdentity holds the schema definition for the TaskIdentity entity.
// task_identities：幂等身份账本（§3.1/§5.1）——idempotency_key → task_id 的跨阶段映射；
// enqueue_tasks 在业务本地事务内与 payload、pending 原子写入，dedupe 窗口内重复创建
// ON CONFLICT 返回原 task_id（§5.1）。键由接入方保证全实例唯一（无 tenant，§3.1）；
// 窗口长度与清理策略属待定参数（§14），created_at 为清理扫描依据。
// 字段一行一条；列注释随 ent 生成落到 DDL（entsql.WithComments）。
type TaskIdentity struct {
	ent.Schema
}

// Fields of the TaskIdentity.
func (TaskIdentity) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").StorageKey("task_id").Comment("任务 ID（雪花，非自增）；幂等命中时返回给业务方的原 ID").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("idempotency_key").Unique().Comment("业务幂等键（全实例唯一）：账本裁决键，dedupe 与溯源的依据（§3.1/§5.1）").NotEmpty(),
		field.Time("created_at").Comment("写入时间（不可变）；dedupe 窗口与账本清理的扫描依据（窗口长度待定，§14）").Immutable().Default(time.Now),
	}
}

// Edges of the TaskIdentity.
func (TaskIdentity) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + 列注释落 DDL。
func (TaskIdentity) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_identities"},
		entsql.WithComments(true),
	}
}
