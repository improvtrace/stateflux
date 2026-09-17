package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	clusterv1 "github.com/improvtrace/stateflux/api/cluster/v1"
	"github.com/improvtrace/stateflux/internal/config"
)

// defaultClusterHTTPPath 是 HTTP 集群视图的规范路径（与 gRPC 方法同名，便于网关路由）。
const defaultClusterHTTPPath = "/cluster.v1.ClusterService/GetClusterInfo"

// httpView 经 HTTP(S)/JSON 调用外部 ClusterService（§15.1#3）：适配网关式部署，
// 对应 dsn：http(s)://host[:port][/path]?timeout=...。请求体为空对象、响应体为
// protojson 编码的 ClusterInfoResponse；path 为空时使用规范路径。
// 连接与调用参数：connect_timeout → TCP 拨号超时，timeout → 单次请求整体预算。
type httpView struct {
	url     string
	client  *http.Client
	timeout time.Duration
}

// NewHTTPView 构造 HTTP(S) 集群视图客户端。
func NewHTTPView(opts config.ClusterOptions) (View, error) {
	if opts.Host == "" {
		return nil, errNoEndpoint(string(opts.Scheme))
	}
	path := "/"
	if idx := strings.Index(opts.Host, "/"); idx >= 0 {
		path = opts.Host[idx:]
		opts.Host = opts.Host[:idx]
	}
	if path == "" || path == "/" {
		path = defaultClusterHTTPPath
	}
	scheme := "http://"
	if opts.Scheme == config.ClusterSchemeHTTPS {
		scheme = "https://"
	}
	// connect_timeout 只约束连接建立；timeout 约束整次调用（含连接与响应读取）。
	transport := &http.Transport{
		DialContext: (&net.Dialer{Timeout: opts.ConnectTimeout}).DialContext,
	}
	return &httpView{
		url:     scheme + opts.Host + path,
		client:  &http.Client{Timeout: opts.Timeout, Transport: transport},
		timeout: opts.Timeout,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.url, bytes.NewReader(body))
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
