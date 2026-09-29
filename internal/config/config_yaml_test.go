package config

import (
	"testing"
	"time"
)

// TestShippedConfigYAMLLoads 守护随包分发的 config.yaml（make install 打包来源）：
// 必须经真实装载路径（NewViper + FromViper）完整解码，且关键取值符合「全节点同一份
// 配置、serve 指向本机外置 cluster 服务」的部署模型。
func TestShippedConfigYAMLLoads(t *testing.T) {
	v, err := NewViper("config.yaml")
	if err != nil {
		t.Fatalf("read shipped config.yaml: %v", err)
	}
	cfg, err := FromViper(v)
	if err != nil {
		t.Fatalf("decode shipped config.yaml: %v", err)
	}

	const wantDSN = "grpc://127.0.0.1:9190?timeout=10s&connect_timeout=30s&interval=3s"
	if cfg.Cluster.DSN != wantDSN {
		t.Fatalf("cluster.dsn = %q, want %q", cfg.Cluster.DSN, wantDSN)
	}
	opts, err := ParseClusterDSN(cfg.Cluster.DSN)
	if err != nil {
		t.Fatalf("parse shipped cluster dsn: %v", err)
	}
	if opts.Scheme != ClusterSchemeGRPC || opts.Host != "127.0.0.1:9190" {
		t.Fatalf("cluster dsn options = %+v, want grpc scheme at 127.0.0.1:9190", opts)
	}
	if opts.Timeout != 10*time.Second || opts.ConnectTimeout != 30*time.Second || opts.Interval != 3*time.Second {
		t.Fatalf("cluster dsn timing = timeout=%s connect=%s interval=%s", opts.Timeout, opts.ConnectTimeout, opts.Interval)
	}

	// cluster 子命令消费同一份文件的 mock_server 段：serve 与本机集群服务的约定端口。
	if cfg.Cluster.MockServer.GRPCAddr != "127.0.0.1:9190" || cfg.Cluster.MockServer.HTTPAddr != "127.0.0.1:9191" {
		t.Fatalf("mock_server addrs = %s/%s, want 127.0.0.1:9190/9191",
			cfg.Cluster.MockServer.GRPCAddr, cfg.Cluster.MockServer.HTTPAddr)
	}

	// serve 监听须与 mock 默认节点地址（127.0.0.1:9090）自洽。
	if cfg.Server.GRPCAddr != "127.0.0.1:9090" || cfg.Server.HTTPAddr != "127.0.0.1:9091" {
		t.Fatalf("server addrs = %s/%s, want 127.0.0.1:9090/9091", cfg.Server.GRPCAddr, cfg.Server.HTTPAddr)
	}

	// 各段落时长/数值抽样：防止 YAML 书写形态（带单位字符串等）解码退化。
	if cfg.PG.ConnMaxLifetime != 30*time.Minute || cfg.Redis.DialTimeout != 5*time.Second {
		t.Fatalf("pg/redis durations unexpected: %+v %+v", cfg.PG, cfg.Redis)
	}
	if cfg.Runtime.TickInterval != 100*time.Millisecond || cfg.Runtime.WALMaxBytes != 256<<20 {
		t.Fatalf("runtime values unexpected: tick=%s wal_max_bytes=%d",
			cfg.Runtime.TickInterval, cfg.Runtime.WALMaxBytes)
	}
	if cfg.Dispatch.Timeout != 10*time.Second || cfg.Coherence.SyncInterval != 10*time.Second {
		t.Fatalf("dispatch/coherence durations unexpected: %+v %+v", cfg.Dispatch, cfg.Coherence)
	}
}
