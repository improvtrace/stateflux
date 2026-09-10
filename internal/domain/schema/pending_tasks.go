package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/index"
)

// PendingTask holds the schema definition for the PendingTask entity.
// pending_tasks：已创建，等待调度预处理——约束晋升的扫描对象（§5.2）；可取消边界内（§4）。
type PendingTask struct {
	ent.Schema
}

// Fields of the PendingTask.
func (PendingTask) Fields() []ent.Field {
	return []ent.Field{}
}

// Edges of the PendingTask.
func (PendingTask) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (PendingTask) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 晋升扫描索引：(priority DESC, run_at)（§3.1）。
func (PendingTask) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("priority", "run_at").Annotations(
			entsql.DescColumns("priority"),
		),
		idempotencyIndex(),
	}
}

// Annotations 显式表名。
func (PendingTask) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pending_tasks"}}
}
