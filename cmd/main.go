// Command stateflux is the process entry point: it assembles the cobra root
// command and executes it (§8, §15.1#1). Business subcommands (serve) live in
// the cmd/stateflux package.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/improvtrace/stateflux/cmd/stateflux"
)

// version 可以在构建时注入：-ldflags "-X main.version=..."。
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:     "stateflux",
		Short:   "Distributed task framework node: task ledger, dispatch, execution and coherence sync",
		Version: version,
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
	// Dependency provider instances are created by this composition layer and
	// injected into the subcommand via its initialization function.
	root.AddCommand(stateflux.NewServeCommand(stateflux.NewServeDeps()))

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "stateflux: "+err.Error())
		os.Exit(1)
	}
}
