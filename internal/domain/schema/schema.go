package schema

// incrementDisabled 显式关闭整型主键的自增：任务 ID 由客户端雪花生成（§3.1），ent 对用户定义的
// 整型主键默认按自增列生成 DDL，须以注解覆盖。四阶段表与 payload / result / identity 表都以
// &incrementDisabled 引用它。
//
// 说明：本包不使用 ent Mixin——各 model 独立、完整地定义自己的字段（见各 *_tasks.go /
// task_payloads.go / task_results.go）。本文件只承载不构成「模型字段共享」的注解变量。
var incrementDisabled = false
