package schema

import (
	"encoding/json"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// TaskSchedulable holds the schema definition for the TaskSchedulable entity.
// task_schedulables：就绪可调度（逻辑就绪集），量级显著小于 pending——调度认领的扫描对象；
// 可调度性由并发约束与业务约束在晋升时判定（§5.2）。重试/对账重置的任务也回到本表
// （约束已通过不重评，§5.5）。认领挪行为单条原生 SQL（FOR UPDATE SKIP LOCKED，§3.1）。
//
// 本表字段独立定义（四阶段表不共享 mixin）、一行一条。字段分公共段与阶段段：
// 公共段 19 列与 pending/processing/completed 四表一致，公共列增删四处同步；
// 本表阶段段：含 attempts/claimed_node（重置后保留上次认领值）与 hash_bucket（认领时过滤），
// 不含 error（终态才写）、run_at（调度不判断时间门槛，§5.2）。
// 列注释随 ent 生成落到 DDL（entsql.WithComments）；枚举与区间见 enums.go。
type TaskSchedulable struct {
	ent.Schema
}

// Fields of the TaskSchedulable.
func (TaskSchedulable) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Comment("任务 ID（雪花，客户端生成，非自增）").Annotations(entsql.Annotation{Incremental: &incrementDisabled}),
		field.String("type").Comment("任务类型：决定 handler 与路由（§5.1）"),
		field.String("operator").Comment("任务子类型（type 的细分）：与 type 一起决定 handler 与路由（§1.2.8/§5.1）").Default(""),
		field.Int8("priority").Comment("调度优先级：0–100 区间（越大越优先，默认 50）；非枚举，调度只依赖排序（§3.1）").Default(int8(DefaultPriority)),
		field.String("channel").Comment("逻辑 channel 名，调度侧据此解析 eventbus 实现；同步/异步由 channel 能力推导（§3.1/§5.3）").NotEmpty(),
		field.Int64("timeout_ms").Comment("单次执行预算(ms)；R1 兜底阈值取 max(timeout_ms, dispatch_grace)+skew（§5.4/§6.2）").Default(60000),
		field.Int32("max_attempts").Comment("最大认领次数；超限由 R3 写死信（§6.2）").Default(3),
		field.Int64("attempts").Comment("已认领次数；claim 时 +1，作为执行 fence（§3.1/§9.6）").Default(0),
		field.String("claimed_node").Comment("上次认领的节点（诊断用）；重置后保留上次值；执行节点不持有权威状态（§1.2.4）").Default(""),
		field.String("idempotency_key").Comment("业务幂等键（溯源与回调派生）；唯一性由 task_identities 裁决（§3.1/§5.1）").Default(""),
		field.JSON("callback", json.RawMessage{}).Comment("OnSuccess/OnError 回调规格 jsonb；深度上限由写入侧校验（§5.6）").Optional(),
		// parent_task_id 与 attempt/outcome 一起构成派生任务的幂等键（§5.6）。
		field.Int64("parent_task_id").Comment("回调派生来源的任务 ID（§5.6）").Default(0),
		field.String("vpc").Comment("调度目标节点属性：目标网络域（VPC 名）；空串=不限；自由文本非枚举（§3.1/§5.3）").Default(""),
		field.String("node").Comment("调度目标节点属性：期望执行节点 ID；空串=任意；自由文本非枚举（§3.1）").Default(""),
		field.String("label").Comment("调度目标节点属性：期望执行节点的匹配标签，认领时据此过滤；空串=不限；自由文本非枚举，框架不解释语义（§3.1/§5.2）").Default(""),
		field.Int16("hash_bucket").Comment("调度目标节点属性：hash 分桶（0–255），0=不限；接入方自定义语义，框架只做等值匹配；认领时据此过滤（§3.1/§5.2）").Default(0),
		field.Strings("biz_race_labels").Optional().Comment("业务并发约束：业务对象参与的竞争约束标签集合（字符串数组）；空=不限；调度侧据此实施并发控制，机制待定（§3.1/§14）"),
		field.String("biz_race_entry").Comment("业务并发约束：唯一标识业务对象的竞争入口（字符串）；空=不参与；同 entry 的任务由调度侧施加并发约束，机制待定（§3.1/§14）").Default(""),
		field.String("biz_group").Comment("业务属性：业务分组键，供上层业务按组查询/聚合；框架不解释、不参与调度正确性（§3.1）").Default(""),
		field.String("biz_batch_id").Comment("业务属性：工厂批次 ID（业务写入，框架用于孤儿判定，§5.6）").Default(""),
		field.Time("created_at").Comment("创建时间（不可变）").Immutable().Default(time.Now),
		field.Time("updated_at").Comment("最后更新时间；R1 对账扫描依据（§6.2）").Default(time.Now).UpdateDefault(time.Now),
	}
}

// Edges of the TaskSchedulable.
func (TaskSchedulable) Edges() []ent.Edge {
	return nil
}

// Annotations 显式表名 + priority/hash_bucket 区间 CHECK + 列注释落 DDL。
func (TaskSchedulable) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "task_schedulables", Checks: map[string]string{"priority_range": PriorityCheck, "bucket_range": BucketCheck}},
		entsql.WithComments(true),
	}
}
