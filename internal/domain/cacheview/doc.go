// Package cacheview 是可选 Redis 观测/容量提示（非正确性路径）：节点容量上报与诊断视图，
// 由 R2 清理。其丢失不得改变 PG 结论，也不构成共识或投递保证——Redis 的定位见 §9.2。
// 基于 domain/data 提供的 redis client。
package cacheview
