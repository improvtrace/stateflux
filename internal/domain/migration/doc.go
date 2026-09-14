// Package migration 数据面迁移。
//
// 文件命名格式：`{6位版本}_{日期yyyymmdd}_{描述}.sql`
// （如 000001_20260914_create_task_tables.sql）。
// 版本号 6 位数字、单调递增，按版本序执行一次；已发布文件不可修改——结构变更一律新增
// 版本文件（次级索引本期不建，延后优化时新增如 000002_20260914_create_indexes.sql）。
package migration
