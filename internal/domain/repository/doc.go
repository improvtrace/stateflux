// Package repository 聚合仓储实现：经 ent 类型安全 API 读写，CreateBulk +
// OnConflict 承载幂等批量写，FOR UPDATE 承载 attempt fencing；仅认领挪行
// （SKIP LOCKED 单条 SQL）经 ent 连接原生下沉。
package repository
