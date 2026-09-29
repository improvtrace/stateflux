// Package migration 实现 PG 数据库的版本化迁移（stateflux migrate 子命令的内核）。
//
// 迁移单元以文件名 ${version}_${date} 标识（如 chronos.0.1_20260929），同一名下可
// 同时存在 .sql 与 .go 文件：SQL 为主（整文件在单事务内经 lib/pq 简单协议执行，
// 支持多语句与 $$ 函数体），Go 迁移用于 SQL 不便表达的部分（注册进 goMigrations，
// 与同名 SQL 在同一事务内、SQL 之后执行）。全部 .sql 文件经 go:embed 嵌入二进制，
// 执行时自包含、不依赖部署机上的文件。
//
// 版本顺序以 build/version（嵌入的版本历史，见 pkg/buildinfo）的行序为准；
// 每个版本应用成功后在 version 表插入一条 {id, version, date}（安装即首条记录），
// 已记录的版本跳过——重复执行幂等。
package migration

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	// pq 驱动：注册 database/sql 的 "postgres"（与 internal/domain/data 同一驱动；
	// 无参数的 Exec 走简单协议，单次可执行整个多语句 SQL 文件）。
	_ "github.com/lib/pq"

	"github.com/improvtrace/stateflux/internal/domain/data"
	"github.com/improvtrace/stateflux/pkg/buildinfo"
)

// sqlFS 嵌入本目录全部版本化 SQL 迁移（文件名 ${version}_${date}.sql）。
//
//go:embed *.sql
var sqlFS embed.FS

// versionTableDDL 是版本账本的建表语句（migrate 引导创建，先于任何版本应用）：
// 每应用一个版本（含首次安装）插入一行，date 为应用时间。
const versionTableDDL = `CREATE TABLE IF NOT EXISTS version (
  id      BIGSERIAL PRIMARY KEY,
  version TEXT NOT NULL UNIQUE,
  date    TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// GoMigration 是一个 Go 实现的迁移：Name 与同名 SQL 文件的基名一致
// （${version}_${date}）；Up 在该版本的事务内、同名 SQL 之后执行。
type GoMigration struct {
	Name string
	Up   func(ctx context.Context, tx *sql.Tx) error
}

// goMigrations 是 Go 迁移注册表；各版本 .go 文件经 init/Register 登记。
var goMigrations = map[string]GoMigration{}

// Register 登记一个 Go 迁移（同名重复登记视为编码错误，panic 暴露）。
func Register(m GoMigration) {
	if _, dup := goMigrations[m.Name]; dup {
		panic(fmt.Sprintf("migration: duplicate Go migration %q", m.Name))
	}
	goMigrations[m.Name] = m
}

// Migration 是一个版本的可执行迁移单元。
type Migration struct {
	// Name 文件基名（${version}_${date}）。
	Name string
	// Version 所属版本（${alias}.${major}.${minor}）。
	Version string
	// SQL 嵌入的 SQL 内容；无同名 .sql 文件时为空。
	SQL string
	// Go 同名 Go 迁移；无则为 nil。
	Go *GoMigration
}

// Options 是迁移执行参数。
type Options struct {
	// Target 目标版本；空 = 嵌入历史的最新版本（正常安装/升级路径）。
	Target string
	// Rebuild 重建模式：先 DROP/CREATE SCHEMA，再重放迁移至 Target，且不写
	// version 记录——供降级流程使用（版本记录随后由数据恢复回填）。
	Rebuild bool
}

// baseRe 校验迁移文件基名：${version}_${date}（version 为 alias.major.minor，
// date 为 YYYYMMDD）。
var baseRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*\.[0-9]+\.[0-9]+_[0-9]{8}$`)

// identifierRe 校验 schema 标识符（DROP/CREATE SCHEMA 用，防注入）。
var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Available 返回按版本历史排序的全部迁移单元（SQL 与 Go 的并集）。
// 迁移的版本必须登记在 build/version 中，且一个版本只允许一个迁移单元
// （一个 ${version}_${date} 基名）。
func Available() ([]Migration, error) {
	return availableFrom(sqlFS, goMigrations, buildinfo.Versions())
}

// availableFrom 是 Available 的可测内核：迁移来源与版本历史均可注入。
func availableFrom(fsys fs.FS, reg map[string]GoMigration, versions []string) ([]Migration, error) {
	order := make(map[string]int, len(versions))
	for i, v := range versions {
		if _, dup := order[v]; dup {
			return nil, fmt.Errorf("migration: build/version 中版本 %s 重复", v)
		}
		order[v] = i
	}

	bases := map[string]bool{}
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("migration: read embedded sql: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".sql")
		if !baseRe.MatchString(base) {
			return nil, fmt.Errorf("migration: SQL 文件名 %q 不符合 ${{version}}_${{date}} 约定", e.Name())
		}
		bases[base] = true
	}
	for name := range reg {
		if !baseRe.MatchString(name) {
			return nil, fmt.Errorf("migration: Go 迁移名 %q 不符合 ${{version}}_${{date}} 约定", name)
		}
		bases[name] = true
	}

	var out []Migration
	for base := range bases {
		version := base[:strings.IndexByte(base, '_')]
		if _, ok := order[version]; !ok {
			return nil, fmt.Errorf("migration: %s 的版本 %s 未登记于 build/version", base, version)
		}
		m := Migration{Name: base, Version: version}
		if content, err := fs.ReadFile(fsys, base+".sql"); err == nil {
			m.SQL = string(content)
		}
		if g, ok := reg[base]; ok {
			g := g
			m.Go = &g
		}
		out = append(out, m)
	}
	// 同版本重复迁移单元（如同版本两个日期）直接冲突。
	seen := map[string]string{}
	for _, m := range out {
		if prev, dup := seen[m.Version]; dup {
			return nil, fmt.Errorf("migration: 版本 %s 有多个迁移单元（%s / %s）", m.Version, prev, m.Name)
		}
		seen[m.Version] = m.Name
	}
	sort.Slice(out, func(i, j int) bool {
		if oi, oj := order[out[i].Version], order[out[j].Version]; oi != oj {
			return oi < oj
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// Run 对 dsn 执行迁移并返回本次实际应用的版本（按应用顺序）。
// 默认升级到嵌入历史的最新版本；opts.Target 指定目标版本（须在历史中）；
// opts.Rebuild 为降级重建模式（见 Options.Rebuild）。searchPath 与 serve 共用
// 同一 DSN 加工规则（data.WithSearchPath），默认 public。
func Run(ctx context.Context, dsn, searchPath string, opts Options) ([]string, error) {
	versions := buildinfo.Versions()
	if len(versions) == 0 {
		return nil, fmt.Errorf("migration: build/version 没有有效版本行")
	}
	target := opts.Target
	if target == "" {
		target = versions[len(versions)-1]
	}
	targetIdx := -1
	for i, v := range versions {
		if v == target {
			targetIdx = i
			break
		}
	}
	if targetIdx < 0 {
		return nil, fmt.Errorf("migration: 目标版本 %s 不在 build/version 历史中（可用: %s）",
			target, strings.Join(versions, ", "))
	}
	if searchPath == "" {
		searchPath = "public"
	}
	if !identifierRe.MatchString(searchPath) {
		return nil, fmt.Errorf("migration: 非法 schema 名 %q", searchPath)
	}

	db, err := sql.Open("postgres", data.WithSearchPath(dsn, searchPath))
	if err != nil {
		return nil, fmt.Errorf("migration: open pg: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("migration: connect pg: %w", err)
	}

	if opts.Rebuild {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`DROP SCHEMA %s CASCADE; CREATE SCHEMA %s;`, searchPath, searchPath)); err != nil {
			return nil, fmt.Errorf("migration: rebuild schema %s: %w", searchPath, err)
		}
	}

	// 版本账本引导（两种模式都需要：重建模式的记录由随后的数据恢复回填）。
	if _, err := db.ExecContext(ctx, versionTableDDL); err != nil {
		return nil, fmt.Errorf("migration: ensure version table: %w", err)
	}

	applied := map[string]bool{}
	if !opts.Rebuild {
		rows, err := db.QueryContext(ctx, "SELECT version FROM version")
		if err != nil {
			return nil, fmt.Errorf("migration: read version table: %w", err)
		}
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				return nil, fmt.Errorf("migration: scan version row: %w", err)
			}
			applied[v] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("migration: iterate version rows: %w", err)
		}
	}

	all, err := Available()
	if err != nil {
		return nil, err
	}

	var ran []string
	for _, m := range all {
		if order := indexOf(versions, m.Version); order > targetIdx {
			break
		}
		if applied[m.Version] {
			continue
		}
		if err := applyOne(ctx, db, m, opts.Rebuild); err != nil {
			return nil, fmt.Errorf("migration: 应用 %s 失败: %w", m.Name, err)
		}
		ran = append(ran, m.Version)
	}
	return ran, nil
}

// applyOne 在单事务内执行一个迁移单元（SQL → Go）并记录版本（重建模式不记录）。
func applyOne(ctx context.Context, db *sql.DB, m Migration, rebuild bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	if m.SQL != "" {
		// lib/pq 无参 Exec 走简单协议：整个多语句文件一次执行（$$ 函数体安全）。
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("sql: %w", err)
		}
	}
	if m.Go != nil {
		if err := m.Go.Up(ctx, tx); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("go: %w", err)
		}
	}
	if !rebuild {
		if _, err := tx.ExecContext(ctx, "INSERT INTO version (version) VALUES ($1)", m.Version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record version: %w", err)
		}
	}
	return tx.Commit()
}

func indexOf(vs []string, v string) int {
	for i, s := range vs {
		if s == v {
			return i
		}
	}
	return -1
}

// versionFmt 校验版本字符串：${alias}.${major}.${minor}。
var versionFmt = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*\.([0-9]+)\.([0-9]+)$`)

// goFuncSuffix 从版本派生合法 Go 函数名后缀（chronos.0.2 → Chronos0_2）。
func goFuncSuffix(version string) (string, error) {
	parts := versionFmt.FindStringSubmatch(version)
	if parts == nil {
		return "", fmt.Errorf("版本 %q 不符合 ${{alias}}.${{major}}.${{minor}} 格式", version)
	}
	alias := version[:strings.IndexByte(version, '.')]
	var b strings.Builder
	for i, r := range alias {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
			if i == 0 && r >= 'a' && r <= 'z' {
				r -= 'a' - 'A'
			}
			b.WriteRune(r)
		case r >= '0' && r <= '9' && i > 0, r == '_':
			b.WriteRune(r)
		default:
			return "", fmt.Errorf("版本别名 %q 含无法转为 Go 标识符的字符", alias)
		}
	}
	return b.String() + parts[1] + "_" + parts[2], nil
}

// Generate 在 dir 下为 version 同时生成一对迁移脚手架：${version}_${date}.sql 与
// ${version}_${date}.go（含 Register 骨架）。按需删掉用不到的那个文件即可
// （两者可共存：先 SQL 后 Go，同一事务）。已存在的文件不覆盖。返回创建的文件路径。
func Generate(dir, version, date string) ([]string, error) {
	if !versionFmt.MatchString(version) {
		return nil, fmt.Errorf("版本 %q 不符合 ${{alias}}.${{major}}.${{minor}} 格式（如 chronos.0.2）", version)
	}
	if date == "" {
		date = time.Now().UTC().Format("20060102")
	}
	if matched, _ := regexp.MatchString(`^[0-9]{8}$`, date); !matched {
		return nil, fmt.Errorf("日期 %q 不符合 YYYYMMDD 格式", date)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建目录 %s: %w", dir, err)
	}
	suffix, err := goFuncSuffix(version)
	if err != nil {
		return nil, err
	}

	base := version + "_" + date
	sqlPath := dir + "/" + base + ".sql"
	goPath := dir + "/" + base + ".go"
	sqlTmpl := fmt.Sprintf(`-- %s.sql
-- %s 的 SQL 迁移（主路径）。整文件在单事务内执行，失败自动回滚；幂等书写
-- （IF NOT EXISTS / OR REPLACE / DROP ... IF EXISTS）。
-- 仅需 SQL 迁移时删除同名 .go；两者共存时先执行本文件。
`, base, version)
	if err := writeFileIfAbsent(sqlPath, sqlTmpl); err != nil {
		return nil, err
	}

	goTmpl := fmt.Sprintf(`// %[1]s.go 是 %[2]s 的 Go 迁移（SQL 不便表达的部分；与同名 .sql 可共存，
// 执行顺序：先 SQL 后 Go，同一事务）。仅需 SQL 迁移时删除本文件。
package migration

import (
	"context"
	"database/sql"
)

func init() {
	Register(GoMigration{
		Name: "%[1]s",
		Up:   up%[3]s,
	})
}

// up%[3]s 在 %[2]s 的事务内执行。
func up%[3]s(_ context.Context, _ *sql.Tx) error {
	// TODO: 实现 %[2]s 的 Go 迁移逻辑。
	return nil
}
`, base, version, suffix)
	if err := writeFileIfAbsent(goPath, goTmpl); err != nil {
		return nil, err
	}
	return []string{sqlPath, goPath}, nil
}

// writeFileIfAbsent 写入文件；已存在则报错（脚手架不覆盖手写内容）。
func writeFileIfAbsent(path, content string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("文件已存在，不覆盖: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("写入 %s: %w", path, err)
	}
	return nil
}
