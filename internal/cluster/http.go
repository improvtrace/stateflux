package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/config"
)

// defaultClusterHTTPPath 是 HTTP 集群视图的规范路径（与 gRPC 方法同名，便于网关路由）。
const defaultClusterHTTPPath = "/cluster.v1.ClusterService/GetClusterInfo"

// httpView 经 HTTP/JSON 调用外部 ClusterService（§15.1#3）：适配网关式部署，
// 请求体为空对象、响应体为 protojson 编码的 ClusterInfoResponse。
type httpView struct {
	endpoint string
	path     string
	client   *http.Client
	timeout  time.Duration
}

// NewHTTPView 构造 HTTP 集群视图客户端。
func NewHTTPView(cfg config.Cluster) (View, error) {
	if cfg.Endpoint == "" {
		return nil, errNoEndpoint("http")
	}
	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	path := defaultClusterHTTPPath
	if strings.Contains(endpoint, "/") {
		// 允许 endpoint 直接带上自定义路径（如 http://gw/cluster/info）。
		if idx := strings.Index(endpoint, "://"); idx >= 0 {
			rest := endpoint[idx+3:]
			if slash := strings.Index(rest, "/"); slash >= 0 {
				path = rest[slash:]
				endpoint = endpoint[:idx+3+slash]
			}
		}
	}
	return &httpView{
		endpoint: endpoint,
		path:     path,
		client:   &http.Client{Timeout: cfg.Timeout},
		timeout:  cfg.Timeout,
	}, nil
}

// Get 拉取一次集群信息快照。
func (v *httpView) Get(ctx context.Context) (Info, error) {
	if v.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, v.timeout)
		defer cancel()
	}
	body, err := json.Marshal(map[string]any{})
	if err != nil {
		return Info{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.endpoint+v.path, bytes.NewReader(body))
	if err != nil {
		return Info{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := v.client.Do(req)
	if err != nil {
		return Info{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return Info{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Info{}, fmt.Errorf("cluster: http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	out := &clusterv1.ClusterInfoResponse{}
	if err := protojson.Unmarshal(raw, out); err != nil {
		return Info{}, fmt.Errorf("cluster: decode cluster info: %w", err)
	}
	return infoFromProto(out), nil
}

// Close 释放空闲连接。
func (v *httpView) Close() error {
	v.client.CloseIdleConnections()
	return nil
}
