// Package migrations 收拢数据库迁移的全部逻辑：内嵌本目录下的 Atlas 版本化
// 迁移（*.sql 与校验和 atlas.sum），负责加载（Dir）、查询待应用项（Pending）
// 与执行（Migrate，目标库不存在时先建库；Pending 不改动数据库）。生成迁移
// 文件用的 SQL 美化格式器在子包 format。独立的迁移工具是 cmd/datamigrate，
// 服务启动前执行。
//
// 迁移文件由 ent schema 经 Atlas 生成，不手写：修改 states/ent/schema 后执行
// go run migrations/gen/main.go -name <名称> -dev-url <空库 DSN>，详见
// migrations/gen/main.go。
package migrations

import "embed"

// FS 内嵌迁移目录。
//
//go:embed *.sql atlas.sum
var FS embed.FS
