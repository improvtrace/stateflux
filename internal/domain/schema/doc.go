// Package schema ent 表定义，为代码生成输入：四阶段表（pending / schedulable / processing /
// completed）+ payload / result + 幂等身份（task_identities）。生成码依赖 ent（entgo.io/ent），
// 输出至 internal/domain/data/ent（make generate），不入库。
package schema
