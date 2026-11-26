// Command stateflux 是调度服务的进程入口：装配配置、确认数据库就位，然后运行
// 调度器直到收到 SIGINT/SIGTERM。表结构迁移由独立的 datamigrate 工具负责，
// 服务启动前先执行它。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	schedule "github.com/improvtrace/stateflux"
	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/states"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "stateflux: "+err.Error())
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var (
		configFile string
		logLevel   string
		logFormat  string
	)
	cmd := &cobra.Command{
		Use:   "stateflux",
		Short: "Run the scheduler: cron-triggered, PostgreSQL-backed, Redis-queued tasks",
		Long: "stateflux runs the task scheduler: cron triggers, durable task rows in " +
			"PostgreSQL (task_persistents), node routing and execution over Redis. " +
			"Apply schema changes with datamigrate before starting the service.",
		Example: `  # Run with a config file
  stateflux --config stateflux.conf

  # Run with flags only
  stateflux --pg-dsn postgres://user:pass@host:5432/stateflux?sslmode=disable \
    --dm-address /tmp/pam/dm.sock --dm-service-key <key>

  # Enable debug logs (queue internals, per-row publishing)
  stateflux --config stateflux.conf --log-level debug`,
		// 出错时不打印 usage：错误信息本身已足够定位；错误统一在 main 里输出。
		SilenceUsage:      true,
		SilenceErrors:     true,
		CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := setupLogger(logLevel, logFormat); err != nil {
				return err
			}
			cfg, err := config.Load(configFile, cmd.Flags())
			if err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("config: invalid: %w", err)
			}
			slog.Info("stateflux: starting",
				"redis_addrs", len(cfg.Redis.Addrs),
				"redis_db", cfg.Redis.DB,
				"dm_address", cfg.DM.Address,
				"reconcile_interval", cfg.Schedule.ReconcileInterval,
			)
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("stateflux: scheduler exited with error", "error", err)
				return err
			}
			slog.Info("stateflux: stopped")
			return nil // 收到停止信号，正常退出
		},
	}
	cmd.Flags().StringVar(&configFile, "config", "", "path to the ini config file (see config/config.conf.example); explicit flags override the file")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn or error")
	cmd.Flags().StringVar(&logFormat, "log-format", "text", "log format: text or json")
	config.RegisterFlags(cmd.Flags())
	return cmd
}

// setupLogger 安装进程级 slog logger：级别与格式只作用于本进程，不影响库
// 调用方；JSON 格式供日志采集侧结构化解析。
func setupLogger(level, format string) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("invalid --log-level %q (want debug, info, warn or error)", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	switch format {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return fmt.Errorf("invalid --log-format %q (want text or json)", format)
	}
	slog.SetDefault(slog.New(handler))
	return nil
}

// run 打开数据库并运行调度器，阻塞直到 ctx 取消。
func run(ctx context.Context, cfg config.Config) error {
	client, err := states.Open(ctx, cfg.Postgres.DSN)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	slog.Info("stateflux: scheduler starting")
	err = schedule.Run(ctx, schedule.Config{
		RedisAddrs:        cfg.Redis.Addrs,
		RedisDB:           cfg.Redis.DB,
		DMAddress:         cfg.DM.Address,
		DMServiceKey:      cfg.DM.ServiceKey,
		Repo:              states.NewTaskPersistentRepo(client),
		ReconcileInterval: cfg.Schedule.ReconcileInterval,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	slog.Info("stateflux: scheduler stopped")
	return nil
}
