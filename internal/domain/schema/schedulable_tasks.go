package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/index"
)

// SchedulableTask holds the schema definition for the SchedulableTask entity.
// schedulable_tasks：就绪可调度（逻辑就绪集），量级显著小于 pending——调度认领的扫描对象；
// 可调度性由并发约束与业务约束在晋升时判定（§5.2）。重试/对账重置的任务也回到本表
// （约束已通过不重评，§5.5）。认领挪行为单条原生 SQL（FOR UPDATE SKIP LOCKED，§9.1）。
type SchedulableTask struct {
	ent.Schema
}

// Fields of the SchedulableTask.
func (SchedulableTask) Fields() []ent.Field {
	return []ent.Field{}
}

// Edges of the SchedulableTask.
func (SchedulableTask) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (SchedulableTask) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 认领扫描索引（§3.1）；「部分索引」细化（run_at 到期谓词）由 migration 引导 DDL 补建。
func (SchedulableTask) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("priority", "run_at").Annotations(
			entsql.DescColumns("priority"),
		),
		idempotencyIndex(),
	}
}

// Annotations 显式表名。
func (SchedulableTask) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "schedulable_tasks"}}
}
