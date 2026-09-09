package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Payload holds the schema definition for the Payload entity.
// task_payloads：payload 分离表（§3.1）——阶段表查询面不背 payload；
// 终态搬移时合并入 completed 后删除分离行。
type Payload struct {
	ent.Schema
}

// Fields of the Payload.
func (Payload) Fields() []ent.Field {
	return []ent.Field{
		// task_id 主键（§3.1）；ent 的 id 字段经 StorageKey 映射为 task_id 列。
		field.Int64("id").StorageKey("task_id"),
		// 任务载荷（jsonb；>64KB 业务方走对象存储引用，§3.1）。
		field.JSON("payload", json.RawMessage{}),
		field.Time("created_at").Immutable().Default(time.Now),
	}
}

// Edges of the Payload.
func (Payload) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名。
func (Payload) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "task_payloads"}}
}

// Result holds the schema definition for the Result entity.
// task_results：独立结果集（§3.1/§14.7）——终态事务内一次性写入、不可变（task_id 主键幂等，
// 冲突即重复归集、跳过）；业务结果的保留期与 completed 归档策略解耦；completed 不内联 result。
type Result struct {
	ent.Schema
}

// Fields of the Result.
func (Result) Fields() []ent.Field {
	return []ent.Field{
		// task_id 主键（§3.1）；id 字段经 StorageKey 映射为 task_id 列。
		field.Int64("id").StorageKey("task_id"),
		field.String("outcome"), // succeeded/failed/dead
		field.Int64("attempt"),
		field.JSON("result", json.RawMessage{}).Optional(),
		field.String("error").Default(""),
		field.Time("completed_at"),
	}
}

// Edges of the Result.
func (Result) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名。
func (Result) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "task_results"}}
}

// Pending holds the schema definition for the Pending entity.
// pending_tasks：已创建，等待调度预处理——约束晋升的扫描对象（§5.2）。
type Pending struct {
	ent.Schema
}

// Fields of the Pending.
func (Pending) Fields() []ent.Field {
	return []ent.Field{}
}

// Edges of the Pending.
func (Pending) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (Pending) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 晋升扫描索引：(priority DESC, run_at)（§3.1）。
func (Pending) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("priority", "run_at").Annotations(
			entsql.DescColumns("priority"),
		),
		idempotencyIndex(),
	}
}

// Annotations 显式表名。
func (Pending) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pending_tasks"}}
}

// Schedulable holds the schema definition for the Schedulable entity.
// schedulable_tasks：就绪可调度（逻辑就绪集），量级显著小于 pending——调度认领的扫描对象；
// 可调度性由并发约束与业务约束在晋升时判定（§5.2）。重试/对账重置的任务也回到本表（约束不重评）。
type Schedulable struct {
	ent.Schema
}

// Fields of the Schedulable.
func (Schedulable) Fields() []ent.Field {
	return []ent.Field{}
}

// Edges of the Schedulable.
func (Schedulable) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (Schedulable) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 认领扫描索引（§3.1）。
func (Schedulable) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("priority", "run_at").Annotations(
			entsql.DescColumns("priority"),
		),
		idempotencyIndex(),
	}
}

// Annotations 显式表名。
func (Schedulable) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "schedulable_tasks"}}
}

// Processing holds the schema definition for the Processing entity.
// processing_tasks：已调度（在队列/inprocess/同步分发协程中）——认领与重试逻辑所在；
// 已入本表的任务不可取消（§4）。
type Processing struct {
	ent.Schema
}

// Fields of the Processing.
func (Processing) Fields() []ent.Field {
	return []ent.Field{}
}

// Edges of the Processing.
func (Processing) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (Processing) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 对账扫描索引 (updated_at)；per-type 并发计数 (type)（§3.1/§5.2）。
func (Processing) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("updated_at"),
		index.Fields("type"),
		idempotencyIndex(),
	}
}

// Annotations 显式表名。
func (Processing) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "processing_tasks"}}
}

// Completed holds the schema definition for the Completed entity.
// completed_tasks：统一历史表（§3.1）——outcome ∈ {succeeded, failed, dead}；
// 内联 payload（便于排查），不内联 result（完整结果在 task_results）；
// 按 created_at 分区 + detach 归档为扩展开关（§11，默认实现为普通表）。
type Completed struct {
	ent.Schema
}

// Fields of the Completed.
func (Completed) Fields() []ent.Field {
	return []ent.Field{
		field.String("outcome"), // succeeded/failed/dead
		// 终态时从 task_payloads 合并入行（§5.5）。
		field.JSON("payload", json.RawMessage{}).Optional(),
		field.Time("completed_at"),
	}
}

// Edges of the Completed.
func (Completed) Edges() []ent.Edge {
	return nil
}

// Mixin 共享核心字段。
func (Completed) Mixin() []ent.Mixin {
	return []ent.Mixin{stageMixin{}}
}

// Indexes 查询索引 (type, created_at)；死信运维 (outcome)；工厂孤儿判定 (batch_id)（§3.1/§12.9/§5.7）。
func (Completed) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("type", "created_at"),
		index.Fields("outcome"),
		index.Fields("batch_id"),
	}
}

// Annotations 显式表名。
func (Completed) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "completed_tasks"}}
}
