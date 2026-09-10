package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"entgo.io/ent/schema/mixin"
)

// incrementDisabled 显式关闭整型主键的自增：任务 ID 由客户端雪花生成（§3.1），
// ent 对用户定义的整型主键默认按自增列生成 DDL，须以注解覆盖。
var incrementDisabled = false

// stageMixin 四阶段表（pending/schedulable/processing/completed）共享的核心字段（§3.1）：
// 生命周期阶段由所在表表达，status 列取消。
type stageMixin struct {
	mixin.Schema
}

func (stageMixin) Fields() []ent.Field {
	return []ent.Field{
		// 雪花 ID（客户端生成，非自增）。
		field.Int64("id").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").NotEmpty(),
		field.String("priority").Default("normal"), // high/normal/low
		field.String("exec_mode").Default("async"), // sync/async
		// 最早可调度时间：承载延迟任务与重试退避（§5.5 重试路径）。
		field.Time("run_at"),
		field.Int64("timeout_ms").Default(60000),
		field.Int32("max_attempts").Default(3),
		// attempts：认领时 +1（§14.3），作为 fencing token 贯穿执行、归集与对账校验。
		field.Int64("attempts").Default(0),
		field.String("owner_node").Default(""),
		// 终态错误摘要（完整结果在 task_results，completed 不内联 result，§3.1）。
		field.String("error").Default(""),
		// 业务幂等键：非终态三表唯一性覆盖（§3.1/§5.1），冲突跳过插入、沿用已存在 task_id。
		field.String("idempotency_key").Default(""),
		field.String("batch_id").Default(""),
		// OnSuccess/OnError 回调规格（§5.8），jsonb；规格可嵌套，深度上限由写入侧校验。
		field.JSON("callback", json.RawMessage{}).Optional(),
		// 回调派生溯源（§5.8，派生任务幂等键 = parent_task_id + attempt + outcome）。
		field.Int64("parent_task_id").Default(0),
		field.Time("created_at").Immutable().Default(time.Now),
		field.Time("updated_at").Default(time.Now).UpdateDefault(time.Now),
	}
}

// idempotencyIndex 幂等键普通索引（加速创建时查重）。局部唯一性（仅非空值，覆盖非终态三表）
// 由 migration 引导 DDL 补建——ent 注解不支持谓词索引（§3.1 幂等键唯一性方案：创建时查重 +
// 各表局部唯一索引）。
func idempotencyIndex() ent.Index {
	return index.Fields("idempotency_key")
}
