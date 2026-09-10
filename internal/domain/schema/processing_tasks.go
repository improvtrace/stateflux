package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/index"
)

// ProcessingTask holds the schema definition for the ProcessingTask entity.
// processing_tasks：已调度（在队列/inprocess/同步分发协程中，精确位置以 Redis 为准，§4）——
// 认领与重试逻辑所在；已入本表的任务不可取消（§4）。
type ProcessingTask struct {
	ent.Schema
}

// Fields of the ProcessingTask.
func (ProcessingTask) Fields() []ent.Field {
	return []ent.Field{}
}

// Edges of the ProcessingTask.
func (ProcessingTask) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (ProcessingTask) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 对账扫描索引 (updated_at)；per-type 并发约束计数 (type)（§3.1/§5.2）。
func (ProcessingTask) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("updated_at"),
		index.Fields("type"),
		idempotencyIndex(),
	}
}

// Annotations 显式表名。
func (ProcessingTask) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "processing_tasks"}}
}
