package task

// Semantics 是投递语义（§15.1#5）。它是任务分发的领域词汇，不是配置项——config 只保存
// 默认值，取值域定义在这里，供分发器、调度周期与 API 适配层共用。
type Semantics string

const (
	// AtLeastOnce 至少一次：允许重复，失败可重试。
	AtLeastOnce Semantics = "at_least_once"
	// AtMostOnce 至多一次：放弃重试，允许丢失。
	AtMostOnce Semantics = "at_most_once"
	// ExactlyOnce 尽力恰一次：入口去重 + 幂等账本，通道丢失仍由对账兜底。
	ExactlyOnce Semantics = "exactly_once"
)

// Delivery 是投递形态（§15.1#5）。
type Delivery string

const (
	// DeliveryRedisQueue 异步 redis queue 投递。
	DeliveryRedisQueue Delivery = "redis_queue"
	// DeliverySyncRPC 同步 rpc 投递。
	DeliverySyncRPC Delivery = "sync_rpc"
)
