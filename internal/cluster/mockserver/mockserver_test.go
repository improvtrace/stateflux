package mockserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/cluster"
	"github.com/improvtrace/stateflux/internal/config"
)

// twoNodeConfig 返回标准两节点视图：node-1 primary/leader、node-2 standby。
func twoNodeConfig() Config {
	return Config{Nodes: []NodeSpec{
		{ID: "node-1", Address: "127.0.0.1:9090", VPC: "vpc-a", Labels: []string{"zone=a"}, Roles: []string{"primary"}, Online: true, Leader: true},
		{ID: "node-2", Address: "127.0.0.1:9092", Roles: []string{"standby"}, Online: true},
	}}
}

func TestParseNodeSpec(t *testing.T) {
	cases := []struct {
		name    string
		spec    string
		want    NodeSpec
		wantErr string
	}{
		{
			name: "full spec",
			spec: "id=node-1,address=10.0.0.1:9090,vpc=vpc-a,labels=zone=a;rack=b,roles=primary,online=false,leader=true",
			want: NodeSpec{ID: "node-1", Address: "10.0.0.1:9090", VPC: "vpc-a", Labels: []string{"zone=a", "rack=b"}, Roles: []string{"primary"}, Online: false, Leader: true},
		},
		{
			name: "id only keeps defaults",
			spec: "id=node-2",
			want: NodeSpec{ID: "node-2", Online: true},
		},
		{
			name: "roles normalized to lowercase",
			spec: "id=node-2,roles=Standby",
			want: NodeSpec{ID: "node-2", Roles: []string{"standby"}, Online: true},
		},
		{name: "empty spec", spec: "", wantErr: "empty node spec"},
		{name: "missing id", spec: "address=127.0.0.1:9090", wantErr: "id is required"},
		{name: "field without value", spec: "id=node-1,leader", wantErr: "want key=value"},
		{name: "unknown field", spec: "id=node-1,rolse=primary", wantErr: "unknown field"},
		{name: "invalid bool", spec: "id=node-1,online=yes", wantErr: "invalid bool"},
		{name: "invalid role", spec: "id=node-1,roles=follower", wantErr: "unknown role"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseNodeSpec(tc.spec)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseNodeSpec(%q) error = %v, want containing %q", tc.spec, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseNodeSpec(%q): %v", tc.spec, err)
			}
			if got.ID != tc.want.ID || got.Address != tc.want.Address || got.VPC != tc.want.VPC ||
				got.Online != tc.want.Online || got.Leader != tc.want.Leader ||
				strings.Join(got.Labels, ";") != strings.Join(tc.want.Labels, ";") ||
				strings.Join(got.Roles, ";") != strings.Join(tc.want.Roles, ";") {
				t.Fatalf("ParseNodeSpec(%q) = %+v, want %+v", tc.spec, got, tc.want)
			}
		})
	}
}

func TestConfigNormalize(t *testing.T) {
	t.Run("self resolves to leader", func(t *testing.T) {
		cfg, err := twoNodeConfig().Normalize()
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if cfg.NodeID != "node-1" {
			t.Fatalf("NodeID = %q, want leader node-1", cfg.NodeID)
		}
	})
	t.Run("self resolves to first node without leader", func(t *testing.T) {
		cfg := Config{Nodes: []NodeSpec{{ID: "node-2", Online: true}, {ID: "node-1", Online: true}}}
		got, err := cfg.Normalize()
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if got.NodeID != "node-2" {
			t.Fatalf("NodeID = %q, want first node node-2", got.NodeID)
		}
	})
	t.Run("listen addresses defaulted", func(t *testing.T) {
		got, err := Config{Nodes: DefaultNodes()}.Normalize()
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if got.GRPCAddr != DefaultGRPCAddr || got.HTTPAddr != DefaultHTTPAddr || got.ShutdownTimeout <= 0 {
			t.Fatalf("normalize defaults = %+v", got)
		}
	})
	t.Run("empty nodes default to host-derived single view", func(t *testing.T) {
		got, err := (Config{}).Normalize()
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if len(got.Nodes) != 1 || got.Nodes[0].ID != DefaultNodeID() || !got.Nodes[0].Leader {
			t.Fatalf("nodes = %+v, want default single leader node", got.Nodes)
		}
		if got.NodeID != DefaultNodeID() {
			t.Fatalf("NodeID = %q, want host-derived %q", got.NodeID, DefaultNodeID())
		}
	})
	t.Run("rejects duplicate ids", func(t *testing.T) {
		cfg := Config{Nodes: []NodeSpec{{ID: "a"}, {ID: "a"}}}
		if _, err := cfg.Normalize(); err == nil || !strings.Contains(err.Error(), "duplicate node id") {
			t.Fatalf("normalize error = %v, want duplicate node id", err)
		}
	})
	t.Run("rejects dangling self", func(t *testing.T) {
		cfg := Config{NodeID: "ghost", Nodes: DefaultNodes()}
		if _, err := cfg.Normalize(); err == nil || !strings.Contains(err.Error(), "not in node list") {
			t.Fatalf("normalize error = %v, want not in node list", err)
		}
	})
}

// TestGRPCViewRoundtrip 经真实的 gRPC 客户端（cluster.NewGRPCView，即 serve 的
// 消费路径）拉取 mock 视图，钉死 gRPC 面与客户端的契约兼容。
func TestGRPCViewRoundtrip(t *testing.T) {
	srv, err := New(twoNodeConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.GRPCServer().Serve(lis) }()
	defer srv.GRPCServer().Stop()

	view, err := cluster.NewGRPCView(config.ClusterOptions{
		Scheme:         config.ClusterSchemeGRPC,
		Host:           lis.Addr().String(),
		Timeout:        2 * time.Second,
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewGRPCView: %v", err)
	}
	defer view.(interface{ Close() error }).Close()

	info, err := view.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertTwoNodeInfo(t, info)
}

// TestHTTPViewRoundtrip 经真实的 HTTP 客户端（cluster.NewHTTPView）拉取 mock 视图，
// 钉死 HTTP 面的规范路径与 protojson 编码兼容。
func TestHTTPViewRoundtrip(t *testing.T) {
	srv, err := New(twoNodeConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()

	view, err := cluster.NewHTTPView(config.ClusterOptions{
		Scheme:         config.ClusterSchemeHTTP,
		Host:           ts.Listener.Addr().String(),
		Timeout:        2 * time.Second,
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewHTTPView: %v", err)
	}

	info, err := view.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertTwoNodeInfo(t, info)
}

// assertTwoNodeInfo 校验两节点视图经客户端转换后的领域信息：身份与调度节点来自
// leader；primary 推导全部服务权限，standby 仅业务操作与任务执行。
func assertTwoNodeInfo(t *testing.T, info cluster.Info) {
	t.Helper()
	if info.NodeID != "node-1" || info.SchedulerNodeID != "node-1" {
		t.Fatalf("NodeID=%q SchedulerNodeID=%q, want node-1/node-1", info.NodeID, info.SchedulerNodeID)
	}
	if len(info.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(info.Nodes))
	}
	n1, ok := info.Node("node-1")
	if !ok || n1.Address != "127.0.0.1:9090" || n1.VPC != "vpc-a" || !n1.Online || !n1.IsLeader ||
		!n1.HasLabel("zone=a") || !n1.HasPermission(cluster.NodePermissionSchedule) {
		t.Fatalf("node-1 = %+v", n1)
	}
	n2, ok := info.Node("node-2")
	if !ok || n2.Address != "127.0.0.1:9092" || n2.IsLeader ||
		n2.HasPermission(cluster.NodePermissionSchedule) || !n2.HasPermission(cluster.NodePermissionExecute) {
		t.Fatalf("node-2 = %+v", n2)
	}
}

func TestAdminSetLeaderOverHTTP(t *testing.T) {
	srv, err := New(twoNodeConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()
	base := ts.URL

	// 切换 leader 到 node-2：返回 200 与切换后的视图。
	resp, err := http.Post(base+PathAdminLeader, "application/json", strings.NewReader(`{"node_id":"node-2"}`))
	if err != nil {
		t.Fatalf("POST /admin/leader: %v", err)
	}
	view := decodeClusterInfo(t, resp)
	if getLeader(t, view) != "node-2" {
		t.Fatalf("leader after switch = %q, want node-2", getLeader(t, view))
	}
	// 上报身份不随 leader 切换变化（构造时已解析固定）。
	if view.GetNodeId() != "node-1" {
		t.Fatalf("reported node_id = %q, want fixed node-1", view.GetNodeId())
	}

	// GET 规范路径应反映同一新视图（node-2 为唯一 leader）。
	resp, err = http.Get(base + PathGetClusterInfo)
	if err != nil {
		t.Fatalf("GET GetClusterInfo: %v", err)
	}
	if getLeader(t, decodeClusterInfo(t, resp)) != "node-2" {
		t.Fatal("GET GetClusterInfo does not reflect the new leader")
	}

	// 未知节点：404；非法请求体：400。
	resp, err = http.Post(base+PathAdminLeader, "application/json", strings.NewReader(`{"node_id":"ghost"}`))
	if err != nil {
		t.Fatalf("POST /admin/leader (ghost): %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ghost node status = %d, want 404", resp.StatusCode)
	}
	resp, err = http.Post(base+PathAdminLeader, "application/json", strings.NewReader(`not-json`))
	if err != nil {
		t.Fatalf("POST /admin/leader (bad body): %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad body status = %d, want 400", resp.StatusCode)
	}
}

// decodeClusterInfo 读取并解码响应为 ClusterInfoResponse（protojson 输出含
// 随机空白，不做字符串断言）。
func decodeClusterInfo(t *testing.T, resp *http.Response) *clusterv1.ClusterInfoResponse {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", resp.StatusCode, raw)
	}
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	out := &clusterv1.ClusterInfoResponse{}
	if err = protojson.Unmarshal(raw, out); err != nil {
		t.Fatalf("decode cluster info: %v", err)
	}
	return out
}

// getLeader 返回视图中唯一的 leader 节点 ID；无 leader 返回空串，多个返回首个。
func getLeader(t *testing.T, resp *clusterv1.ClusterInfoResponse) string {
	t.Helper()
	for _, n := range resp.GetNodes() {
		if n.GetIsLeader() {
			return n.GetNodeId()
		}
	}
	return ""
}

// TestRunLifecycle 覆盖 Run 的完整生命周期：双监听启动、/readyz 就绪、ctx 取消后
// 优雅退出且无错误。
func TestRunLifecycle(t *testing.T) {
	cfg := twoNodeConfig()
	cfg.GRPCAddr = freeAddr(t)
	cfg.HTTPAddr = freeAddr(t)
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + cfg.HTTPAddr + "/readyz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatal("mockserver did not become ready in time")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// freeAddr 预留一个临时端口（取址后立即释放，由 Run 重新绑定）。
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// TestHostNodeIDStable 验证主机派生节点 ID：非空、形态固定（node-<host>-<hash8>，
// host 归一后可含连字符）、多次调用恒定（同一主机跨重启稳定是单机联调的硬要求）。
func TestHostNodeIDStable(t *testing.T) {
	id := DefaultNodeID()
	if id == "" || !strings.HasPrefix(id, "node-") {
		t.Fatalf("host node id = %q, want node- prefixed non-empty", id)
	}
	hash := id[strings.LastIndexByte(id, '-')+1:]
	if len(hash) != 8 {
		t.Fatalf("host node id = %q, want node-<host>-<8-hex> shape", id)
	}
	for _, r := range hash {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			t.Fatalf("hash suffix %q contains non-hex rune %q", hash, r)
		}
	}
	if again := DefaultNodeID(); again != id {
		t.Fatalf("host node id not stable: %q vs %q", id, again)
	}
}

// TestFromConfig 覆盖 config.Cluster.MockServer → mockserver.Config 装配（规格串
// 解析 + 归一化：空清单退回主机派生默认视图）。
func TestFromConfig(t *testing.T) {
	got, err := FromConfig(config.MockServer{
		GRPCAddr: DefaultGRPCAddr,
		HTTPAddr: DefaultHTTPAddr,
		Self:     "node-9",
		Nodes:    []string{"id=node-9,address=127.0.0.1:9090,roles=primary,leader=true"},
	})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	if got.GRPCAddr != DefaultGRPCAddr || got.HTTPAddr != DefaultHTTPAddr {
		t.Fatalf("addrs = %s/%s", got.GRPCAddr, got.HTTPAddr)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "node-9" || got.NodeID != "node-9" {
		t.Fatalf("cfg = %+v", got)
	}

	empty, err := FromConfig(config.MockServer{})
	if err != nil {
		t.Fatalf("FromConfig (empty): %v", err)
	}
	if len(empty.Nodes) != 1 || empty.Nodes[0].ID != DefaultNodeID() || empty.NodeID != DefaultNodeID() {
		t.Fatalf("cfg = %+v, want host-derived default view", empty)
	}
}
