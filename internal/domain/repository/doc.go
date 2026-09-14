// Package repository 定义各表的仓储接口（每表一个文件，§3.1）：实现见 internal/domain/data
// （ent 类型安全 API；identity upsert 承载幂等，FOR UPDATE 承载 attempt fencing，认领挪行
// SKIP LOCKED 单条 SQL 经 ent 连接原生下沉，§5.2）。接口以 ent 实体为数据载体；
// 跨表事务由调用方以 tx.Client() 构造仓储保证（§5.2/§5.5）。
package repository
