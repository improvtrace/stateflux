//go:build ignore

// gen 由 ent schema 生成增量迁移文件，写入本目录（migrations/）。
//
// 用 ent 内置的 Atlas（migrate.NamedDiff，回放模式）：先把 migrations/ 现有文件
// 回放到开发库，再把回放得到的表结构与 ent schema 期望的结构做 diff，差异写成
// 新迁移文件 <时间戳>_<name>.sql 并更新 atlas.sum；schema 没有变化时不产文件。
// 迁移如实反映 schema 变更——删列、删索引也会生成 DROP 语句，执行前请评审。
// 迁移只进不退：不生成 .down.sql，回滚靠新写一个反向迁移。
//
// 开发库必须是空库（Atlas 用完即清），仅用于计算 diff，不要指向真实环境。
//
//	go run migrations/gen/main.go -name add_xxx -dev-url postgres://postgres:postgres@127.0.0.1:5432/stateflux_dev?sslmode=disable
//
// 修改 states/ent/schema 之后：先 go generate ./states/ent，再执行本命令。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	atlas "ariga.io/atlas/sql/migrate"
	"entgo.io/ent/dialect/sql/schema"

	// postgres 驱动：注册 database/sql 的 "postgres"。
	_ "github.com/lib/pq"

	"github.com/improvtrace/stateflux/migrations/format"
	entmigrate "github.com/improvtrace/stateflux/states/ent/migrate"
)

func main() {
	name := flag.String("name", "changes", "migration name (file name is <timestamp>_<name>.sql)")
	devURL := flag.String("dev-url", os.Getenv("STATEFLUX_DEV_URL"), "development database DSN (must be empty; defaults to STATEFLUX_DEV_URL)")
	dirPath := flag.String("dir", "migrations", "migration directory (relative to the repository root)")
	flag.Parse()
	if *devURL == "" {
		log.Fatal("gen: -dev-url or STATEFLUX_DEV_URL is required")
	}

	dir, err := atlas.NewLocalDir(*dirPath)
	if err != nil {
		log.Fatalf("gen: open migration directory %s: %v", *dirPath, err)
	}
	before, err := dir.Files()
	if err != nil {
		log.Fatalf("gen: read migration directory %s: %v", *dirPath, err)
	}
	// 首个迁移用检查模式：期望结构直接取自 ent schema，不经开发库回放后的
	// 规范化，生成的 SQL 更干净。之后的迁移用回放模式：以迁移目录回放出的
	// 结构（而非开发库的当前状态）为基准，目录即真相。
	mode := schema.Mode(schema.ModeReplay) // 常量是非类型化的 iota，需显式取 Mode 类型
	if len(before) == 0 {
		mode = schema.ModeInspect
	}
	err = entmigrate.NamedDiff(context.Background(), *devURL, *name,
		schema.WithDir(dir),
		// 每个迁移一个 <时间戳>_<名称>.sql（Atlas 标准布局），内容经 SQL 美化。
		// 不沿用 ent 对本地目录的缺省格式：那会额外产出 .down.sql，而随附的
		// 执行器不识别反向文件。
		schema.WithFormatter(format.Formatter{}),
		schema.WithMigrationMode(mode),
		// ent 缺省跳过 DROP（防误删）；此处要求迁移如实反映 schema 变更。
		schema.WithDropColumn(true),
		schema.WithDropIndex(true),
	)
	if err != nil {
		log.Fatalf("gen: generate migration: %v", err)
	}
	after, err := dir.Files()
	if err != nil {
		log.Fatalf("gen: read migration directory %s: %v", *dirPath, err)
	}
	if len(after) == len(before) {
		fmt.Printf("gen: schema is unchanged, no migration file generated (%s)\n", *dirPath)
		return
	}
	for _, f := range after[len(before):] {
		fmt.Printf("gen: generated migration %s (%s updated)\n", filepath.Join(*dirPath, f.Name()), filepath.Join(*dirPath, "atlas.sum"))
	}
}
