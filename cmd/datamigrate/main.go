// Command datamigrate 是数据库迁移的独立工具：把内嵌的版本化迁移（migrations/）
// 应用到目标库。目标库不存在时先建库（DSN 里的账号须有 CREATEDB 权限）。服务
// （stateflux）启动前执行；重复执行幂等。
//
//	datamigrate --config stateflux.conf          # 应用全部待执行迁移
//	datamigrate --dry-run --pg-dsn postgres://…  # 只列出待应用的迁移，不执行
//
// --dry-run 不改动数据库：目标库不存在时报错而不建库。迁移逻辑全部在
// migrations 包，本文件只负责参数与输出。
package main

import (
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/migrations"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "datamigrate: "+err.Error())
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var (
		configFile string
		dryRun     bool
	)
	cmd := &cobra.Command{
		Use:   "datamigrate",
		Short: "Apply the embedded schema migrations (migrations/) to a PostgreSQL database",
		Long: "datamigrate applies the embedded, versioned schema migrations to the target " +
			"PostgreSQL database and records each applied file in atlas_schema_revisions. " +
			"Re-running it is a no-op once everything is applied. If the target database " +
			"does not exist, it is created first through the maintenance database postgres " +
			"(the connecting role needs CREATEDB). Run it before starting stateflux; only " +
			"one process should migrate a database at a time.",
		Example: `  # Apply with a config file
  datamigrate --config stateflux.conf

  # List what would be applied, without touching the database
  datamigrate --dry-run --pg-dsn postgres://user:pass@host:5432/stateflux?sslmode=disable`,
		// 出错时不打印 usage：错误信息本身已足够定位；错误统一在 main 里输出。
		SilenceUsage:      true,
		SilenceErrors:     true,
		CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configFile, cmd.Flags())
			if err != nil {
				return err
			}
			if err := cfg.Postgres.Validate(); err != nil {
				return fmt.Errorf("config: invalid: %w", err)
			}
			out := cmd.OutOrStdout()
			mode := "apply"
			if dryRun {
				mode = "dry-run"
			}
			fmt.Fprintf(out, "datamigrate: target database %s (mode: %s)\n", maskDSN(cfg.Postgres.DSN), mode)

			ctx := cmd.Context()
			if dryRun {
				pending, err := migrations.Pending(ctx, cfg.Postgres.DSN)
				if err != nil {
					return err
				}
				switch len(pending) {
				case 0:
					fmt.Fprintln(out, "datamigrate: no pending migrations")
				default:
					fmt.Fprintf(out, "datamigrate: %d pending migration(s) (dry run, not applied)\n", len(pending))
					for _, name := range pending {
						fmt.Fprintf(out, "  %s\n", name)
					}
				}
				return nil
			}

			applied, err := migrations.Migrate(ctx, cfg.Postgres.DSN)
			if err != nil {
				return err
			}
			switch len(applied) {
			case 0:
				fmt.Fprintln(out, "datamigrate: no pending migrations")
			default:
				fmt.Fprintf(out, "datamigrate: applied %d migration(s)\n", len(applied))
				for _, name := range applied {
					fmt.Fprintf(out, "  applied: %s\n", name)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&configFile, "config", "", "path to the ini config file (see config/config.conf.example); explicit flags override the file")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list pending migrations without applying them")
	config.RegisterFlags(cmd.Flags(), "postgres")
	return cmd
}

// maskDSN 隐藏 DSN 中的密码再打印。
func maskDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return dsn
	}
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(u.User.Username(), "xxxx") // 不用 *：URL 转义后会显示成 %2A
	}
	return u.String()
}
