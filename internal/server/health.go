package server

import (
	"net/http"
	"sync/atomic"
)

// Health 是进程健康状态：健康检查 HTTP 面据此区分「存活」与「就绪」。
//
//   - /healthz 始终 200：进程存活即通过（供容器 liveness 探针）。
//   - /readyz 仅在 App.Run 完成启动、进入服务态时返回 200；一旦开始优雅退出立即转为
//     503，通知负载均衡/服务发现先行摘除流量，是优雅退出的第一步。
type Health struct {
	ready atomic.Bool
}

// NewHealth 构造健康状态（初始未就绪：服务尚未监听，不应接收流量）。
func NewHealth() *Health { return &Health{} }

// SetReady 设置就绪状态；nil 接收者安全（装配缺省时退化为常驻未就绪的服务面）。
func (h *Health) SetReady(ready bool) {
	if h == nil {
		return
	}
	h.ready.Store(ready)
}

// Ready 报告当前是否就绪。
func (h *Health) Ready() bool {
	if h == nil {
		return false
	}
	return h.ready.Load()
}

// NewHealthMux 构造健康检查路由。
func NewHealthMux(health *Health) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !health.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}
