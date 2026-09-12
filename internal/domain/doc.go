// Package domain 是领域层与引擎共享内核：任务模型（task/callback/result）、幂等身份与控制
// 租约（identity/control）、聚合接口（TaskRepository/ResultRepository/OpsRepository 与组合根
// Store）、执行接入契约（Handler/Registry/Precondition）、重试策略与 ID 生成。只含模型与纯
// 接口，存储实现见子包 repository/data/cacheview/migration/schema。
package domain
