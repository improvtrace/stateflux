package schema

import (
	"encoding/json"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskResult holds the schema definition for the TaskResult entity.
// task_results：独立结果集（§3.1/§5.5）——终态事务内一次性写入、不可变（task_id 主键幂等，
// INSERT ... ON CONFLICT DO NOTHING，冲突即重复归集、跳过）；业务结果的保留期与 completed
// 归档策略解耦（独立 TTL/分区）；completed 不内联 result；大 result 同 payload 规则（§3.1）。
// 字段一行一条；列注释随 ent 生成落到 DDL（entsql.WithComments）；outcome 数值编码见 enums.go。
type TaskResult struct {
	ent.Schema
}

// Fields of the TaskResult.
func (TaskResult) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").StorageKey("task_id").Comment("任务 ID（雪花，非自增）；不可变结果的幂等主键").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.Int8("outcome").Comment("终态类别（数值）：1=succeeded 2=failed 3=dead（0 保留未设置）（§3.1/§5.5）"),
		field.Int64("attempt").Comment("写入时的 attempt（fencing token），重复归集据此拒绝陈旧结果（§5.5）"),
		field.JSON("result", json.RawMessage{}).Comment("业务结果 jsonb；不可变；大结果同 payload 规则（§3.1）").Optional(),
		field.String("error").Comment("失败/死信的错误信息（succeeded 时为空）").Default(""),
		field.Time("completed_at").Comment("终态写入时间（结果保留期 TTL/分区的独立依据）（§3.1）"),
	}
}

// Edges of the TaskResult.
func (TaskResult) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + 列注释落 DDL（表注释见 migration 引导 DDL）。
func (TaskResult) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_results"},
		entsql.WithComments(true),
	}
}
