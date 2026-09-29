// Package cluster 定义 stateflux cluster 子命令：启动外置集群服务的本地单机模拟
// （internal/cluster/mockserver），gRPC 与 HTTP/JSON 双面同时监听，供 stateflux
// serve 以 grpc:// 或 http(s):// DSN 接入单机部署测试（§15.1#3）。命令层只做
// flag/config → mockserver 的装配，全部实现逻辑在 internal/cluster/mockserver。
//
// 配置优先级同 serve：默认值 → 配置文件（--config，cluster.mock_server 段）→
// STATEFLUX_* 环境变量 → 命令行 flag。默认节点身份由主机信息派生（固定），
// 视图查询用 stateflux info。
package cluster

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/internal/cluster/mockserver"
	"github.com/improvtrace/stateflux/internal/config"
)

// ClusterDeps 是 cluster 子命令的依赖注入面：Runner 启动 mock 服务；nil 用
// mockserver.Run。测试可替换以只验证装配。
type ClusterDeps struct {
	// Runner 启动 mock 服务并阻塞至 ctx 结束。
	Runner func(ctx context.Context, cfg mockserver.Config) error
}

// NewClusterCommand returns the cluster subcommand: a local single-node mock of
// the external cluster service that stateflux serve consumes via --cluster-dsn.
// The mock serves GetClusterInfo on both gRPC and HTTP/JSON, plus health checks
// (/healthz, /readyz) and a POST /admin/leader endpoint to switch the leader
// node at runtime for failover drills. The default node identity is derived
// from host information (stable across restarts).
func NewClusterCommand(deps ClusterDeps) *cobra.Command {
	runner := deps.Runner
	if runner == nil {
		runner = mockserver.Run
	}
	var configFile string
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Run a mock external cluster service (gRPC + HTTP) for stateflux serve",
		Long: "Run a local single-node mock of the external cluster service " +
			"(cluster.v1.ClusterService) that stateflux serve consumes via " +
			"--cluster-dsn. The mock serves GetClusterInfo on both gRPC and HTTP/JSON, " +
			"plus health checks (/healthz, /readyz) and a POST " + mockserver.PathAdminLeader +
			" endpoint to switch the leader node at runtime for failover drills (serve " +
			"picks the change up on its next view poll).\n\n" +
			"The default node identity is derived from host information (hostname + " +
			"machine-id) and stays fixed across restarts, so a single-node serve setup " +
			"keeps a stable node ID. There is no built-in replication: multiple " +
			"instances are independent mocks, each serving its own configured view.",
		Example: `  # Default single-node view: host-derived node ID (fixed), primary/leader
  # at 127.0.0.1:9090 (serve's default gRPC address)
  stateflux cluster

  # Point serve at the mock over gRPC (or http://127.0.0.1:9191)
  stateflux serve --cluster-dsn grpc://127.0.0.1:9190

  # Inspect the view served by a running mock
  stateflux info --cluster-dsn grpc://127.0.0.1:9190

  # Explicit nodes (one standby), report node-1 as the caller identity
  stateflux cluster --self node-1 \
      --node id=node-1,address=127.0.0.1:9090,roles=primary,leader=true \
      --node id=node-2,address=127.0.0.1:9092,roles=standby

  # Failover drill: promote node-2 at runtime
  curl -X POST http://127.0.0.1:9191/admin/leader -d '{"node_id":"node-2"}'`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			v, err := config.NewViper(configFile)
			if err != nil {
				return err
			}
			config.BindMockServerFlags(v, cmd.Flags())
			cfg, err := config.FromViper(v)
			if err != nil {
				return err
			}
			mc, err := mockserver.FromConfig(cfg.Cluster.MockServer)
			if err != nil {
				return err
			}

			// SIGINT/SIGTERM 触发优雅退出；mock 无 WAL/DB 等待刷出的资源，
			// 单阶段信号语义即可（serve 的两阶段强退不适用于本命令）。
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			log.Printf("stateflux: [cluster] grpc=%s http=%s self=%s nodes=%d",
				mc.GRPCAddr, mc.HTTPAddr, mc.NodeID, len(mc.Nodes))
			return runner(ctx, mc)
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&configFile, "config", "", "path to the config file (cluster.mock_server section; yaml/toml/json/etc.; empty = defaults, env and flags only)")
	fs.String("grpc-addr", mockserver.DefaultGRPCAddr,
		"gRPC listen address serving cluster.v1.ClusterService")
	fs.String("http-addr", mockserver.DefaultHTTPAddr,
		"HTTP listen address: view JSON, healthz/readyz and "+mockserver.PathAdminLeader)
	fs.StringArray("node", nil,
		`mock node spec, repeatable (comma-separated key=value; labels/roles semicolon-separated): `+
			`id=<id>[,address=<host:port>][,vpc=<vpc>][,labels=<a;b>][,roles=<primary;standby>][,online=<bool>][,leader=<bool>] `+
			`(default: single node at `+mockserver.DefaultNodeAddress+` with host-derived fixed ID `+mockserver.DefaultNodeID()+`)`)
	fs.String("self", "",
		"node ID reported to callers as their own identity (ClusterInfoResponse.node_id); empty = leader at startup")
	return cmd
}
