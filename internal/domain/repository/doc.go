// Package repository 聚合仓储实现：经 ent 类型安全 API 读写，identity upsert 与条件名额预留
// 承载幂等与并发，FOR UPDATE 承载 attempt fencing；仅 fenced 认领挪行（SKIP LOCKED 单条 SQL，
// 候选筛选、名额预留与阶段搬迁必须同事务）经 ent 连接原生下沉（§8、§9.1）。
package repository
