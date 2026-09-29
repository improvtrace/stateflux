// Package info 定义 stateflux info 子命令：只读查询一份集群视图并打印全部信息
// （上报身份、调度节点、全部节点明细），不启动任何监听。目标经 --cluster-dsn
// 指定（local:// 静态视图 / grpc://host:port / http://host[:port]，参数语义与
// serve 相同）；默认取配置的 cluster.dsn（local://localhost）。查询与格式化
// 实现在 internal/cluster（Query / Describe），与 serve 的消费路径共用同一客户端。
package info

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/pkg/buildinfo"
)

// InfoDeps 是 info 子命令的依赖注入面：Querier 按 DSN 拉取视图；nil 用
// cluster.Query。测试可替换以只验证装配。
type InfoDeps struct {
	// Querier 按集群视图 DSN 拉取一次快照。
	Querier func(ctx context.Context, dsn string) (cluster.Info, error)
}

// NewInfoCommand returns the info subcommand: fetch and print the full cluster
// view for a given --cluster-dsn, then exit.
func NewInfoCommand(deps InfoDeps) *cobra.Command {
	querier := deps.Querier
	if querier == nil {
		querier = cluster.Query
	}
	var configFile string
	cmd := &cobra.Command{
		Use:   "info",
		Short: "Fetch and print the cluster view (read-only)",
		Long: "Fetch one snapshot of the cluster view and print all of it: the identity " +
			"the cluster reports for the caller, the current scheduler (leader) node and " +
			"every node's address, vpc, labels, roles, derived permissions and liveness. " +
			"The target is given by --cluster-dsn (local:// static view, grpc://host:port " +
			"or http://host[:port]; timeout / connect_timeout via URL query, same as " +
			"serve) and defaults to the configured cluster.dsn (local://localhost). " +
			"The first output line is the binary's build info (version / commit / " +
			"builddate / goversion, injected at compile time, same as --version). " +
			"No listener is started.",
		Example: `  # Default: the static local single-node view
  stateflux info

  # Query a mock cluster service
  stateflux info --cluster-dsn grpc://127.0.0.1:9190
  stateflux info --cluster-dsn 'http://127.0.0.1:9191?timeout=2s'

  # Same precedence as serve: config file / env / flag
  stateflux info --config stateflux.yaml`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			v, err := config.NewViper(configFile)
			if err != nil {
				return err
			}
			config.BindInfoFlags(v, cmd.Flags())
			cfg, err := config.FromViper(v)
			if err != nil {
				return err
			}
			// 查询受 DSN 的 timeout 约束（默认 10s），无需信号处理。
			info, err := querier(cmd.Context(), cfg.Cluster.DSN)
			if err != nil {
				return fmt.Errorf("info: cluster-dsn %s: %w", cfg.Cluster.DSN, err)
			}
			// 先输出本二进制的构建信息（version/commit/builddate/goversion，编译注入，
			// 与 --version 同源），再输出集群视图。
			fmt.Fprintf(cmd.OutOrStdout(), "build:     %s\n", buildinfo.String())
			fmt.Fprintln(cmd.OutOrStdout(), cluster.Describe(info))
			return nil
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&configFile, "config", "", "path to the config file (cluster.dsn; yaml/toml/json/etc.; empty = defaults, env and flags only)")
	fs.String("cluster-dsn", "", "cluster view DSN to query: local://, grpc://host:port or http://host[:port] (empty = configured default, local://localhost)")
	return cmd
}
