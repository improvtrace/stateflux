// Command stateflux is the process entry point: it assembles the cobra root
// command and executes it (§8, §15.1#1). Business subcommands live in the
// cmd/stateflux package (serve), cmd/cluster package (mock cluster service),
// cmd/info package (read-only cluster view query) and cmd/migrate package
// (database migration).
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/cmd/cluster"
	"github.com/improvtrace/stateflux/cmd/info"
	"github.com/improvtrace/stateflux/cmd/migrate"
	"github.com/improvtrace/stateflux/cmd/stateflux"
	"github.com/improvtrace/stateflux/pkg/buildinfo"
)

func main() {
	root := &cobra.Command{
		Use:     "stateflux",
		Short:   "Distributed task framework node: task ledger, dispatch, execution and coherence sync",
		Version: buildinfo.Version(),
		// Long 在根命令无子命令语义时展示概览；子命令各自维护长描述。
		Long: "stateflux is a distributed task framework node. It runs a gRPC/HTTP " +
			"server, a task ledger backed by PostgreSQL, Redis-backed dispatch, " +
			"a worker execution runtime and coherence sync controlled by an " +
			"external cluster view.",
		Example: `  # Start a node with a config file
  stateflux serve --config stateflux.yaml

  # Start a static single node with flags only
  stateflux serve --cluster-dsn local:// --grpc-addr 127.0.0.1:9090`,
		SilenceUsage:  true, // 出错时不打印 usage：错误信息本身已足够定位。
		SilenceErrors: true, // 错误统一在 main 里输出，便于包装前缀与控制退出码。
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true, // 隐藏自动生成的 completion 子命令。
		},
	}
	// --version 打印完整构建信息（version=嵌入的 build/version 末行，commit/
	// builddate 编译注入，见 pkg/buildinfo）；help 模板仍用上面的简短 Version。
	root.SetVersionTemplate("stateflux " + buildinfo.String() + "\n")
	// Dependency provider instances are created by this composition layer and
	// injected into the subcommand via its initialization function.
	root.AddCommand(stateflux.NewServeCommand(stateflux.NewServeDeps()))
	// cluster 子命令：本地单机模拟外置集群服务（gRPC + HTTP），供 serve 以
	// grpc:// 或 http(s):// DSN 接入联调；无 wire 依赖，零值注入即可。
	root.AddCommand(cluster.NewClusterCommand(cluster.ClusterDeps{}))
	// info 子命令：只读查询集群视图（默认配置的 cluster.dsn），打印全部信息。
	root.AddCommand(info.NewInfoCommand(info.InfoDeps{}))
	// migrate 子命令：PG 迁移（初始化 / 升级到指定版本 / 重建 / 生成脚手架）。
	root.AddCommand(migrate.NewMigrateCommand(migrate.MigrateDeps{}))

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "stateflux: "+err.Error())
		os.Exit(1)
	}
}
