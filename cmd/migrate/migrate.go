// Package migrate 定义 stateflux migrate 子命令：PG 数据库的版本化迁移入口
// （internal/domain/migration 的装配层）。三种用法：
//
//	stateflux migrate                                # 初始化 / 升级到最新版本
//	stateflux migrate --target chronos.0.1           # 升级到指定版本
//	stateflux migrate --target chronos.0.1 --rebuild # 重建 schema 至指定版本（降级流程用）
//	stateflux migrate --generate chronos.0.2         # 生成该版本的 sql + go 迁移脚手架（开发期）
//
// 连接取配置的 pg.dsn（优先级同 serve：默认值 → --config 文件 → STATEFLUX_* 环境变量
// → --pg-dsn flag）；每次成功应用的版本都会在 version 表插入一条 {id, version, date}。
package migrate

import (
	"context"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/migration"
	"github.com/improvtrace/stateflux/pkg/buildinfo"
)

// MigrateDeps 是 migrate 子命令的依赖注入面（Runner / Generator 可替换以便测试；
// nil 用 migration 包实现）。
type MigrateDeps struct {
	// Runner 执行迁移，返回本次应用的版本列表。
	Runner func(ctx context.Context, dsn, searchPath string, opts migration.Options) ([]string, error)
	// Generator 生成迁移脚手架（同名 .sql + .go），返回创建的文件路径。
	Generator func(dir, version, date string) ([]string, error)
}

// NewMigrateCommand returns the migrate subcommand: apply versioned database
// migrations (embedded SQL/Go units ordered by the build/version history) and
// record each applied version in the version table.
func NewMigrateCommand(deps MigrateDeps) *cobra.Command {
	runner := deps.Runner
	if runner == nil {
		runner = migration.Run
	}
	generator := deps.Generator
	if generator == nil {
		generator = migration.Generate
	}

	var configFile, pgDSN, target, generate, dir, date string
	var rebuild bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Initialize or upgrade the PostgreSQL schema (versioned migrations)",
		Long: "Apply versioned PostgreSQL migrations and record each applied " +
			"version in the version table {id, version, date}. Migration units " +
			"(${version}_${date}.sql, optionally with a same-named .go file) are " +
			"embedded in the binary and ordered by the build/version history " +
			"(latest line = current version, e.g. chronos.0.1).\n\n" +
			"Default applies all pending versions up to the embedded latest " +
			"(first run = database initialization). --target stops at a specific " +
			"version. --rebuild drops and recreates the schema first and skips " +
			"version bookkeeping — used by the downgrade flow (upgrade.sh) before " +
			"restoring a data-only dump. --generate scaffolds both the .sql and " +
			"the .go migration files for a new version (development-time, no " +
			"database access).",
		Example: `  # Initialize / upgrade to the latest embedded version
  stateflux migrate --config /usr/local/stateflux/conf/config.yaml

  # Upgrade only up to chronos.0.1
  stateflux migrate --target chronos.0.1

  # Rebuild the schema at chronos.0.1 (downgrade flow; no version rows written)
  stateflux migrate --target chronos.0.1 --rebuild

  # Scaffold both .sql and .go migrations for a new version
  stateflux migrate --generate chronos.0.2`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := cmd.OutOrStdout()

			if generate != "" {
				created, err := generator(dir, generate, date)
				if err != nil {
					return fmt.Errorf("migrate: generate %s: %w", generate, err)
				}
				for _, f := range created {
					fmt.Fprintf(out, "generated: %s\n", f)
				}
				fmt.Fprintf(out, "下一步: 编辑生成的迁移文件（只用 SQL 可删同名 .go，反之亦然），并在 build/version 追加 %s 后构建。\n", generate)
				return nil
			}

			v, err := config.NewViper(configFile)
			if err != nil {
				return err
			}
			if f := cmd.Flags().Lookup("pg-dsn"); f != nil {
				_ = v.BindPFlag("pg.dsn", f)
			}
			cfg, err := config.FromViper(v)
			if err != nil {
				return err
			}

			opts := migration.Options{Target: target, Rebuild: rebuild}
			fmt.Fprintf(out, "migrate: dsn=%s target=%s rebuild=%t (latest=%s)\n",
				maskDSN(cfg.PG.DSN), orDefault(target, buildinfo.Version()), rebuild, buildinfo.Version())
			applied, err := runner(cmd.Context(), cfg.PG.DSN, cfg.PG.SearchPath, opts)
			if err != nil {
				return err
			}
			if len(applied) == 0 {
				fmt.Fprintln(out, "migrate: 已是目标版本，无待应用迁移")
				return nil
			}
			for _, ver := range applied {
				fmt.Fprintf(out, "applied:  %s\n", ver)
			}
			return nil
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&configFile, "config", "", "path to the config file (pg.dsn; yaml/toml/json/etc.; empty = defaults, env and flags only)")
	fs.StringVar(&pgDSN, "pg-dsn", "", "PostgreSQL data source name (overrides config file and env)")
	fs.StringVar(&target, "target", "", "target version to migrate to (empty = latest embedded version)")
	fs.BoolVar(&rebuild, "rebuild", false, "drop and recreate the schema first, then replay migrations to --target without writing version rows (downgrade flow)")
	fs.StringVar(&generate, "generate", "", "scaffold both the .sql and .go migrations for the given version (e.g. chronos.0.2) and exit; no database access")
	fs.StringVar(&dir, "dir", "internal/domain/migration", "output directory for --generate")
	fs.StringVar(&date, "date", "", "override the date suffix (YYYYMMDD) for --generate (default: today UTC)")
	return cmd
}

// orDefault 返回非空值，否则 fallback。
func orDefault(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

// maskDSN 隐藏 DSN 中的密码再打印。
func maskDSN(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "****")
			return u.String()
		}
	}
	return dsn
}
