// Package mockserver 提供外置 cluster 服务（cluster.v1.ClusterService）的本地
// 单机模拟：在 gRPC 与 HTTP/JSON 两个面上同时提供 GetClusterInfo，附带头部健康
// 检查与运行期切换 leader 的管理端点，供 stateflux serve 以 grpc:// 或 http(s)://
// DSN 接入单机部署测试（§15.1#3）。真实外部系统的职责（识别调用方身份、维护
// 成员与选举）这里只做静态近似：节点清单来自启动参数，上报身份见 Config.NodeID。
//
// 单机定位：不内置成员同步/共识（多实例各自独立，视图不互通）；默认节点身份由
// 主机信息采集生成（hostNodeID，同一主机恒定），使 serve 单机联调在重启后保持
// 稳定的节点 ID。视图查询请用 stateflux info（internal/cluster.Query）。
package mockserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/server"
)

// 默认监听地址：刻意避开 serve 的默认端口（9090/9091），可与 serve 同机并存。
const (
	// DefaultGRPCAddr gRPC 面默认监听地址。
	DefaultGRPCAddr = "127.0.0.1:9190"
	// DefaultHTTPAddr HTTP 面默认监听地址。
	DefaultHTTPAddr = "127.0.0.1:9191"
	// DefaultNodeAddress 默认节点地址：即 serve 的默认 gRPC 监听地址，使
	// 「stateflux serve --cluster-dsn grpc://127.0.0.1:9190」开箱即得可用视图。
	DefaultNodeAddress = "127.0.0.1:9090"
)

// PathGetClusterInfo 是 HTTP 面的规范路径：与 internal/cluster HTTP 客户端（httpView）
// 请求的 canonical 路径一致（与 gRPC 方法同名），兼容性由往返测试钉死。
const PathGetClusterInfo = "/cluster.v1.ClusterService/GetClusterInfo"

// PathAdminLeader 是切换 leader 的管理端点：POST，JSON 请求体 {"node_id":"..."}，
// 响应为切换后的最新视图（protojson）。供故障切换演练：serve 侧缓存周期拉取
// （默认 3s），归属变化经 electionGate 自然完成控制面接管（§2.1）。
const PathAdminLeader = "/admin/leader"

// NodeSpec 描述模拟集群中的一个节点，字段与 cluster.v1.NodeInfo 一一对应。
type NodeSpec struct {
	// ID 节点唯一标识（必填）。
	ID string
	// Address 节点对外可达的 gRPC 地址（serve 侧转发/分发按此寻址）。
	Address string
	// VPC 节点所属网络域（调度目标节点属性）。
	VPC string
	// Labels 节点匹配标签（分号分隔解析）。
	Labels []string
	// Roles 节点角色，仅 primary / standby（小写归一）。
	Roles []string
	// Online 节点是否在线（默认 true）。
	Online bool
	// Leader 节点是否为 leader（默认 false）。
	Leader bool
}

// ParseNodeSpec 解析节点规格串（cluster 子命令的 --node flag 值）：逗号分隔的
// key=value 字段，labels / roles 以分号分隔（值内不能含逗号与分号），online /
// leader 接受 strconv.ParseBool 语法。id 必填；其余缺省：address 空、online true、
// leader false。key 大小写不敏感；未知字段显式报错，避免拼写错误静默失效。
func ParseNodeSpec(spec string) (NodeSpec, error) {
	n := NodeSpec{Online: true}
	if strings.TrimSpace(spec) == "" {
		return NodeSpec{}, fmt.Errorf("mockserver: empty node spec")
	}
	for _, field := range strings.Split(spec, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			return NodeSpec{}, fmt.Errorf("mockserver: node spec %q: empty field", spec)
		}
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			return NodeSpec{}, fmt.Errorf("mockserver: node spec %q: field %q: want key=value", spec, field)
		}
		var err error
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "id":
			n.ID = val
		case "address":
			n.Address = val
		case "vpc":
			n.VPC = val
		case "labels":
			n.Labels = splitSemicolons(val)
		case "roles":
			if n.Roles, err = parseRoles(spec, val); err != nil {
				return NodeSpec{}, err
			}
		case "online":
			if n.Online, err = parseBool(spec, "online", val); err != nil {
				return NodeSpec{}, err
			}
		case "leader":
			if n.Leader, err = parseBool(spec, "leader", val); err != nil {
				return NodeSpec{}, err
			}
		default:
			return NodeSpec{}, fmt.Errorf("mockserver: node spec %q: unknown field %q (want id,address,vpc,labels,roles,online,leader)", spec, key)
		}
	}
	if n.ID == "" {
		return NodeSpec{}, fmt.Errorf("mockserver: node spec %q: id is required", spec)
	}
	return n, nil
}

// splitSemicolons 按分号切分并去掉空白与空项。
func splitSemicolons(val string) []string {
	var out []string
	for _, item := range strings.Split(val, ";") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// parseRoles 解析并校验角色：仅 primary / standby（小写归一，serve 侧权限推导按
// 精确匹配 primary）。其余取值是配置错误，显式报错。
func parseRoles(spec, val string) ([]string, error) {
	items := splitSemicolons(val)
	if len(items) == 0 {
		return nil, nil
	}
	for i, r := range items {
		switch strings.ToLower(r) {
		case "primary", "standby":
			items[i] = strings.ToLower(r)
		default:
			return nil, fmt.Errorf("mockserver: node spec %q: roles %q: unknown role %q (want primary|standby)", spec, val, r)
		}
	}
	return items, nil
}

// parseBool 解析规格中的布尔字段（strconv.ParseBool 语法：1/t/true/0/f/false）。
func parseBool(spec, key, val string) (bool, error) {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("mockserver: node spec %q: %s=%q: invalid bool", spec, key, val)
	}
	return b, nil
}

// Config 是 mock 服务的装配参数。
type Config struct {
	// GRPCAddr gRPC 面监听地址；空用 DefaultGRPCAddr。
	GRPCAddr string
	// HTTPAddr HTTP 面监听地址；空用 DefaultHTTPAddr。
	HTTPAddr string
	// NodeID 上报给调用方的「本节点身份」（ClusterInfoResponse.node_id）。真实集群
	// 按调用方识别身份，mock 无法区分调用方，故统一上报同一身份：空 = 启动时的
	// leader（无 leader 取首个节点）。mock 无法区分调用方，多个 serve 共用同一
	// mock 会共享身份，适合单节点联调。
	NodeID string
	// Nodes 节点清单；空用 DefaultNodes（主机信息派生的单节点视图）。
	Nodes []NodeSpec
	// ShutdownTimeout 优雅退出预算；<= 0 用 server.DefaultShutdownTimeout。
	ShutdownTimeout time.Duration
}

// DefaultNodeID 返回主机信息派生的默认节点 ID（同一主机恒定，见 hostNodeID）。
func DefaultNodeID() string { return hostNodeID() }

// DefaultNodes 返回默认单节点视图：主机信息派生 ID（固定），primary + leader +
// 在线，地址即 serve 的默认 gRPC 监听地址（与 local:// 的内置节点语义对齐，见
// cluster.DefaultLocalNode）。
func DefaultNodes() []NodeSpec {
	return []NodeSpec{{
		ID:      DefaultNodeID(),
		Address: DefaultNodeAddress,
		Roles:   []string{"primary"},
		Online:  true,
		Leader:  true,
	}}
}

// FromConfig 把 config.Cluster.MockServer 装配为启动 Config：节点规格串在此
// 解析，并完成 Normalize（缺省补全与校验；New 兜底再验，幂等）——命令层日志
// 与测试据此拿到解析后的地址与身份。
func FromConfig(cfg config.MockServer) (Config, error) {
	nodes := make([]NodeSpec, 0, len(cfg.Nodes))
	for _, spec := range cfg.Nodes {
		n, err := ParseNodeSpec(spec)
		if err != nil {
			return Config{}, err
		}
		nodes = append(nodes, n)
	}
	return Config{
		GRPCAddr:        cfg.GRPCAddr,
		HTTPAddr:        cfg.HTTPAddr,
		NodeID:          cfg.Self,
		Nodes:           nodes,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}.Normalize()
}

// Normalize 校验并补全配置（幂等，New 会再次调用兜底）：监听地址与退出预算取
// 缺省；空节点清单退回默认单节点视图。NodeID 为空时解析为启动时的 leader（无
// leader 取首个节点）、非空时必须指向已有节点——serve 以该 ID 作为自身身份参与
// 选举归属判断（§2.1），悬空身份会让控制面永不运行，属配置错误，应在启动时暴露。
func (c Config) Normalize() (Config, error) {
	if c.GRPCAddr == "" {
		c.GRPCAddr = DefaultGRPCAddr
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = DefaultHTTPAddr
	}
	if len(c.Nodes) == 0 {
		c.Nodes = DefaultNodes()
	}
	seen := make(map[string]bool, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.ID == "" {
			return Config{}, fmt.Errorf("mockserver: node with empty id")
		}
		if seen[n.ID] {
			return Config{}, fmt.Errorf("mockserver: duplicate node id %q", n.ID)
		}
		seen[n.ID] = true
	}
	if c.NodeID == "" {
		c.NodeID = c.Nodes[0].ID
		for _, n := range c.Nodes {
			if n.Leader {
				c.NodeID = n.ID
				break
			}
		}
	} else if !seen[c.NodeID] {
		return Config{}, fmt.Errorf("mockserver: self node id %q not in node list", c.NodeID)
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = server.DefaultShutdownTimeout
	}
	return c, nil
}

// service 实现 cluster.v1.ClusterService：快照式只读视图 + 运行期 leader 切换
// （单机直改内存，无外部状态）。
type service struct {
	clusterv1.UnimplementedClusterServiceServer

	mu    sync.RWMutex
	nodes []NodeSpec
	self  string
}

// GetClusterInfo 返回当前视图快照（gRPC 面）。
func (s *service) GetClusterInfo(context.Context, *clusterv1.ClusterInfoRequest) (*clusterv1.ClusterInfoResponse, error) {
	return s.snapshot(), nil
}

// snapshot 在读锁下构建响应副本：调用方（gRPC / HTTP 面）拿到的总是完整一致的一份。
func (s *service) snapshot() *clusterv1.ClusterInfoResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resp := &clusterv1.ClusterInfoResponse{NodeId: s.self}
	for _, n := range s.nodes {
		resp.Nodes = append(resp.Nodes, &clusterv1.NodeInfo{
			NodeId:   n.ID,
			Address:  n.Address,
			Vpc:      n.VPC,
			Labels:   append([]string(nil), n.Labels...),
			Role:     append([]string(nil), n.Roles...),
			Online:   n.Online,
			IsLeader: n.Leader,
		})
	}
	return resp
}

// nodeExists 判断视图当前是否包含某节点（管理端点做 404 校验）。
func (s *service) nodeExists(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// setLeader 直改 leader：清掉全部既有 leader 再标记目标节点，保证快照内 leader
// 唯一（infoFromProto 取首个 is_leader 节点为调度节点）。
func (s *service) setLeader(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for i := range s.nodes {
		if s.nodes[i].ID == nodeID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("mockserver: unknown node %q", nodeID)
	}
	for i := range s.nodes {
		s.nodes[i].Leader = s.nodes[i].ID == nodeID
	}
	return nil
}

// handleGetClusterInfo 是 HTTP 面的 GetClusterInfo：与 httpView 客户端对齐——
// 忽略请求体（客户端固定发空 JSON 对象），响应 protojson 编码。GET 便于浏览器
// 与 curl 直接查看当前视图。
func (s *service) handleGetClusterInfo(w http.ResponseWriter, _ *http.Request) {
	writeProtoJSON(w, http.StatusOK, s.snapshot())
}

// handleSetLeader 是 HTTP 面的 leader 切换端点：请求体 {"node_id":"..."}，
// 成功返回切换后的最新视图，未知节点返回 404。
func (s *service) handleSetLeader(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<10))
	if err != nil {
		http.Error(w, "mockserver: read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, "mockserver: decode body (want {\"node_id\":\"...\"}): "+err.Error(), http.StatusBadRequest)
		return
	}
	if !s.nodeExists(req.NodeID) {
		http.Error(w, fmt.Sprintf("mockserver: unknown node %q", req.NodeID), http.StatusNotFound)
		return
	}
	if err := s.setLeader(req.NodeID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeProtoJSON(w, http.StatusOK, s.snapshot())
}

// writeProtoJSON 以 protojson 输出响应（httpView 客户端的解码格式）。
func writeProtoJSON(w http.ResponseWriter, status int, resp *clusterv1.ClusterInfoResponse) {
	raw, err := protojson.Marshal(resp)
	if err != nil {
		http.Error(w, "mockserver: encode cluster info: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

// Server 是装配后的 mock 集群服务：gRPC 面（ClusterService，含 server reflection
// 便于 grpcurl 排障）+ HTTP 面（健康检查、GetClusterInfo JSON、管理端点）。
// 生命周期由 Run 驱动——复用 internal/server.App 的双监听优雅退出；两个服务面
// 亦可单独取出（GRPCServer / HTTPHandler）嵌入自定义监听或测试。
type Server struct {
	cfg        Config
	service    *service
	grpcServer *grpc.Server
	handler    http.Handler
	health     *server.Health
}

// New 校验并装配 mock 服务。
func New(cfg Config) (*Server, error) {
	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}
	svc := &service{nodes: append([]NodeSpec(nil), cfg.Nodes...), self: cfg.NodeID}

	grpcServer := grpc.NewServer()
	clusterv1.RegisterClusterServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	health := server.NewHealth()
	mux := server.NewHealthMux(health)
	mux.HandleFunc("POST "+PathGetClusterInfo, svc.handleGetClusterInfo)
	// GET 便于浏览器/curl 直接查看当前视图（客户端走 POST）。
	mux.HandleFunc("GET "+PathGetClusterInfo, svc.handleGetClusterInfo)
	mux.HandleFunc("POST "+PathAdminLeader, svc.handleSetLeader)

	return &Server{
		cfg:        cfg,
		service:    svc,
		grpcServer: grpcServer,
		handler:    mux,
		health:     health,
	}, nil
}

// GRPCServer 返回注册好 ClusterService 的 gRPC 服务面。
func (s *Server) GRPCServer() *grpc.Server { return s.grpcServer }

// HTTPHandler 返回 HTTP 服务面（健康检查 + GetClusterInfo + 管理端点）。
func (s *Server) HTTPHandler() http.Handler { return s.handler }

// Health 暴露就绪状态（App.Run 启动完成后转就绪）。
func (s *Server) Health() *server.Health { return s.health }

// SetLeader 切换 leader 节点；未知节点报错。
func (s *Server) SetLeader(nodeID string) error { return s.service.setLeader(nodeID) }

// Run 便捷入口：校验装配 Config 并阻塞运行至 ctx 结束或服务面出错。
func Run(ctx context.Context, cfg Config) error {
	srv, err := New(cfg)
	if err != nil {
		return err
	}
	return srv.Run(ctx)
}

// Run 启动 gRPC 与 HTTP 双监听并阻塞直到 ctx 结束或服务面出错，随后按预算优雅
// 关闭（复用 internal/server.App 的生命周期：/readyz 摘流 → 双面排空 → 逆序清理）。
func (s *Server) Run(ctx context.Context) error {
	return server.NewApp(server.AppOptions{
		Config: config.Config{Server: config.Server{
			GRPCAddr:        s.cfg.GRPCAddr,
			HTTPAddr:        s.cfg.HTTPAddr,
			ShutdownTimeout: s.cfg.ShutdownTimeout,
		}},
		GRPCServer: s.grpcServer,
		HTTPServer: &http.Server{
			Addr:              s.cfg.HTTPAddr,
			Handler:           s.handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
		Health: s.health,
	}).Run(ctx)
}
