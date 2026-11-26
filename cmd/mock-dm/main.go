// Command mock-dm 是 dm 服务（设备管理）的本地桩：在 unix socket 上提供
// stateflux 依赖的唯一接口 ListClusterNodesStatus，按指定的节点数自动生成集群
// 节点表，供没有真实 dm 的本地与联调环境驱动 dispatch 的节点路由。
//
//	mock-dm --config stateflux.conf --nodes 3   # 复用 stateflux 配置的 [dm] 段
//	mock-dm --dm-address /tmp/mock-dm.sock --dm-service-key change-me --nodes 3
//
// 与真实 dm 一致：监听地址即 socket 路径，请求须携带服务密钥（metadata:
// service_key），缺失或不符一律拒绝。响应携带本节点 ID（node_id）——stateflux
// 启动时以它确定自己的分节点队列，桩的 --node-id 即模拟该身份（默认 node-1）。
// 本地模拟多节点时为每个实例各起一个桩（不同 socket、不同 --node-id）。
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	dmpb "github.com/improvtrace/stateflux/api/dm"
	"github.com/improvtrace/stateflux/config"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "mock-dm: "+err.Error())
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var (
		configFile string
		nodes      int
		nodeID     string
	)
	cmd := &cobra.Command{
		Use:   "mock-dm",
		Short: "Run a local mock of the dm service with an auto-generated cluster node table",
		Long: "mock-dm runs a local stand-in for the dm service (device management) that " +
			"stateflux queries for its cluster node table and its own node ID. It serves " +
			"ListClusterNodesStatus over a unix socket with a table of N generated online " +
			"nodes (node-1 … node-N: node-1 is LEADER, the rest FOLLOWER). The response's " +
			"node_id is --node-id: a stateflux instance started against this socket uses it " +
			"as its own node identity, so run one stub per simulated node (distinct sockets, " +
			"distinct --node-id) to emulate a multi-node cluster.",
		Example: `  # Reuse the [dm] section of the stateflux config file (address + service_key)
  mock-dm --config stateflux.conf --nodes 3

  # Flags only
  mock-dm --dm-address /tmp/mock-dm.sock --dm-service-key change-me --nodes 3`,
		// 出错时不打印 usage：错误信息本身已足够定位；错误统一在 main 里输出。
		SilenceUsage:      true,
		SilenceErrors:     true,
		CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configFile, cmd.Flags())
			if err != nil {
				return err
			}
			if err := cfg.DM.Validate(); err != nil {
				return fmt.Errorf("config: invalid: %w", err)
			}
			if nodes < 1 {
				return fmt.Errorf("mock-dm: --nodes must be positive, got %d", nodes)
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if err := run(ctx, cfg.DM, nodes, nodeID, cmd.OutOrStdout()); err != nil {
				return err
			}
			return nil // 收到停止信号，正常退出
		},
	}
	cmd.Flags().StringVar(&configFile, "config", "", "path to the ini config file (see config/config.conf.example); explicit flags override the file")
	cmd.Flags().IntVar(&nodes, "nodes", 1, "number of generated cluster nodes (node-1 … node-N, all online)")
	cmd.Flags().StringVar(&nodeID, "node-id", "node-1", "node ID this stub reports as its own in ListClusterNodesStatus (must be one of node-1 … node-N)")
	config.RegisterFlags(cmd.Flags(), "dm")
	return cmd
}

// run 在 unix socket 上启动 dm 桩并阻塞直到 ctx 取消；停机即关闭监听，socket
// 文件随之摘除。nodeID 是响应里的本节点 ID，须在生成的节点表内。
func run(ctx context.Context, dm config.DM, nodes int, nodeID string, out io.Writer) error {
	table, err := nodeTable(nodes, nodeID)
	if err != nil {
		return err
	}
	lis, err := listen(dm.Address)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(logRPC))
	dmpb.RegisterServiceServiceServer(srv, &dmService{serviceKey: dm.ServiceKey, table: table})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	fmt.Fprintf(out, "mock-dm: listening on %s with %d node(s), local node %s\n", dm.Address, nodes, nodeID)
	printNodeTable(out, table)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		fmt.Fprintln(out, "mock-dm: shutting down")
		srv.GracefulStop()
		return nil
	}
}

// printNodeTable 打印生成的集群节点表：联调时据此核对 dispatch 的节点路由，
// 本实例身份（响应 node_id）所在行带 (local) 标记。
func printNodeTable(out io.Writer, table *dmpb.ListClusterNodesStatusResponse) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "mock-dm: node table: %d node(s)\n", len(table.Nodes))
	fmt.Fprintln(tw, "  ID\tNAME\tROLE\tONLINE\tIP\tBUSINESS_IP")
	for _, n := range table.Nodes {
		local := ""
		if n.Id == table.NodeId {
			local = "  (local)"
		}
		fmt.Fprintf(tw, "  %s%s\t%s\t%s\t%t\t%s\t%s\n",
			n.Id, local, n.Name, n.Role, n.Online, n.Ip, n.BusinessIp)
	}
	_ = tw.Flush()
}

// listen 在 unix socket 上监听。路径上残留上次进程未清理的 socket 文件（如被
// 强杀）时：仍有实例在监听则照常报错，否则删掉残留文件后重试。
func listen(address string) (net.Listener, error) {
	lis, err := net.Listen("unix", address)
	if err == nil {
		return lis, nil
	}
	if isStaleSocket(address) {
		if rmErr := os.Remove(address); rmErr == nil {
			return net.Listen("unix", address)
		}
	}
	return nil, fmt.Errorf("mock-dm: listen on %s: %w", address, err)
}

// isStaleSocket 判断路径上是否为无人监听的残留 socket 文件：不是 socket（普通
// 文件、目录）或拨得通（仍有实例在监听）都返回 false，不做清理。
func isStaleSocket(address string) bool {
	fi, err := os.Lstat(address)
	if err != nil || fi.Mode()&os.ModeSocket == 0 {
		return false
	}
	conn, err := net.Dial("unix", address)
	if err == nil {
		_ = conn.Close()
		return false
	}
	return true
}

// logRPC 记录每次 RPC 调用：方法、调用方、结果码与耗时。这是联调时的主要
// 观测手段（例如确认 stateflux 是否以正确的 service_key 查询了节点表）；
// 失败调用记 Warn 并带上状态消息。
func logRPC(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	attrs := []any{
		"method", info.FullMethod,
		"peer", peerAddress(ctx),
		"code", status.Code(err).String(),
		"duration_ms", time.Since(start).Milliseconds(),
	}
	if err != nil {
		slog.Warn("mock-dm: rpc failed", append(attrs, "error", status.Convert(err).Message())...)
		return resp, err
	}
	slog.Info("mock-dm: rpc called", attrs...)
	return resp, nil
}

// peerAddress 返回调用方地址；unix socket 的对端通常为抽象地址 "@"。
func peerAddress(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "unknown"
	}
	return p.Addr.String()
}

// fieldServiceKey 是 dm 服务内部接口的鉴权 metadata 键（与 dispatch 的服务密钥
// 拦截器写入的键一致）。
const fieldServiceKey = "service_key"

// dmService 实现 dmpb.ServiceServiceServer：校验服务密钥后返回启动时生成的
// 节点表。
type dmService struct {
	dmpb.UnimplementedServiceServiceServer
	serviceKey string
	table      *dmpb.ListClusterNodesStatusResponse
}

// ListClusterNodesStatus 返回节点表；请求携带的 service_key 缺失或不符时拒绝。
func (s *dmService) ListClusterNodesStatus(ctx context.Context, _ *emptypb.Empty) (*dmpb.ListClusterNodesStatusResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get(fieldServiceKey)) == 0 || md.Get(fieldServiceKey)[0] != s.serviceKey {
		return nil, status.Error(codes.Unauthenticated, "service_key validation failed")
	}
	return s.table, nil
}

// nodeTable 按节点数生成集群节点表（n 须为正，命令入口已校验）：节点 ID 与
// 名称为 node-1 … node-N，首节点为 LEADER、其余为 FOLLOWER，全部在线。
// localID 是响应携带的本节点 ID，须在表内——stateflux 以它作为 claimed_node、
// worker consumerTag 与分节点队列名，不在表内就不会有任何任务被路由到该实例。
func nodeTable(n int, localID string) (*dmpb.ListClusterNodesStatusResponse, error) {
	nodes := make([]*dmpb.ListClusterNodesStatusResponse_Node, n)
	found := false
	for i := range nodes {
		id := fmt.Sprintf("node-%d", i+1)
		role := "FOLLOWER"
		if i == 0 {
			role = "LEADER"
		}
		nodes[i] = &dmpb.ListClusterNodesStatusResponse_Node{
			Id:         id,
			Name:       id,
			Ip:         nodeIP(i + 1),
			BusinessIp: nodeIP(i + 1),
			Role:       role,
			Online:     true,
		}
		if id == localID {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("mock-dm: --node-id %q is not in the generated table (node-1 … node-%d)", localID, n)
	}
	return &dmpb.ListClusterNodesStatusResponse{Nodes: nodes, NodeId: localID}, nil
}

// nodeIP 由节点序号推导回环地址：127.0.0.0/8 内的第 n 个（node-1 → 127.0.0.1，
// node-256 → 127.0.1.0），任意节点数都合法。
func nodeIP(n int) string {
	return fmt.Sprintf("127.%d.%d.%d", n>>16&0xff, n>>8&0xff, n&0xff)
}
