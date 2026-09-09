package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"entgo.io/ent/schema/mixin"
)

// stageMixin 四阶段表共享的核心字段（§3.1）：生命周期阶段由所在表表达，status 列取消。
type stageMixin struct {
	mixin.Schema
}

func (stageMixin) Fields() []ent.Field {
	return []ent.Field{
		// 雪花 ID（客户端生成，非自增）。
		field.Int64("id"),
		field.String("type").NotEmpty(),
		field.String("priority").Default("normal"), // high/normal/low
		field.String("exec_mode").Default("async"), // sync/async
		// 最早可调度时间：承载延迟任务与重试退避。
		field.Time("run_at"),
		field.Int64("timeout_ms").Default(60000),
		field.Int32("max_attempts").Default(3),
		// attempts：认领时 +1（§14.3），作为 fencing token 贯穿执行与归集校验。
		field.Int64("attempts").Default(0),
		field.String("owner_node").Default(""),
		// 终态错误摘要（完整结果在 task_results，§3.1）。
		field.String("error").Default(""),
		// 业务幂等键：非终态三表局部唯一（创建时查重 + 各表局部唯一索引）。
		field.String("idempotency_key").Default(""),
		field.String("batch_id").Default(""),
		// OnSuccess/OnError 回调规格（§5.8），jsonb。
		field.JSON("callback", json.RawMessage{}).Optional(),
		// 回调派生溯源。
		field.Int64("parent_task_id").Default(0),
		field.Time("created_at").Immutable().Default(time.Now),
		field.Time("updated_at").Default(time.Now).UpdateDefault(time.Now),
	}
}

// idempotencyIndex 幂等键普通索引（加速创建时查重）。局部唯一性（仅非空值，三张非终态表）
// 由 store 引导迁移以原生 DDL 补建——ent 注解不支持谓词索引（§3.1 幂等键唯一性方案）。
func idempotencyIndex() ent.Index {
	return index.Fields("idempotency_key")
}
