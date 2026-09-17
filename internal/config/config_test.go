package config

import (
	"os"
	"testing"
	"time"
)

func TestFromEnvShutdownTimeout(t *testing.T) {
	t.Setenv("STATEFLUX_SERVER_SHUTDOWN_TIMEOUT", "3s")
	cfg := FromEnv()
	if cfg.Server.ShutdownTimeout != 3*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want 3s", cfg.Server.ShutdownTimeout)
	}
}

func TestDefaultShutdownTimeout(t *testing.T) {
	cfg := Default()
	if cfg.Server.ShutdownTimeout <= 0 {
		t.Fatalf("default ShutdownTimeout = %s, want positive", cfg.Server.ShutdownTimeout)
	}
}

func TestLoadFromConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/stateflux.yaml"
	content := "node:\n  node_id: node-a\nserver:\n  grpc_addr: 0.0.0.0:9100\n  shutdown_timeout: 7s\nruntime:\n  worker_queues: q1,q2\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := NewViper(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := FromViper(v)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.GRPCAddr != "0.0.0.0:9100" ||
		cfg.Server.ShutdownTimeout != 7*time.Second || len(cfg.Runtime.WorkerQueues) != 2 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestParseClusterDSN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		dsn          string
		wantScheme   ClusterScheme
		wantHost     string
		wantTimeout  time.Duration
		wantConnect  time.Duration
		wantInterval time.Duration
		wantNodeID   string
		wantErr      bool
	}{
		{name: "empty defaults to local", dsn: "", wantScheme: ClusterSchemeLocal, wantHost: "localhost",
			wantTimeout: 10 * time.Second, wantConnect: 30 * time.Second, wantInterval: 3 * time.Second},
		{name: "local with params", dsn: "local://localhost?node_id=n1&address=10.0.0.1:9090",
			wantScheme: ClusterSchemeLocal, wantHost: "localhost",
			wantTimeout: 10 * time.Second, wantConnect: 30 * time.Second, wantInterval: 3 * time.Second, wantNodeID: "n1"},
		{name: "unit-less seconds", dsn: "local://localhost?timeout=5",
			wantScheme: ClusterSchemeLocal, wantHost: "localhost",
			wantTimeout: 5 * time.Second, wantConnect: 30 * time.Second, wantInterval: 3 * time.Second},
		{name: "grpc with params", dsn: "grpc://etcd:2379?timeout=1s&connect_timeout=15s&interval=0",
			wantScheme: ClusterSchemeGRPC, wantHost: "etcd:2379",
			wantTimeout: 1 * time.Second, wantConnect: 15 * time.Second, wantInterval: 0},
		{name: "http with path", dsn: "http://gw:8080/cluster/info?timeout=2s",
			wantScheme: ClusterSchemeHTTP, wantHost: "gw:8080",
			wantTimeout: 2 * time.Second, wantConnect: 30 * time.Second, wantInterval: 3 * time.Second},
		{name: "https", dsn: "https://gw/cluster/info",
			wantScheme: ClusterSchemeHTTPS, wantHost: "gw",
			wantTimeout: 10 * time.Second, wantConnect: 30 * time.Second, wantInterval: 3 * time.Second},
		{name: "grpc missing host", dsn: "grpc://", wantErr: true},
		{name: "unknown scheme", dsn: "foo://bar", wantErr: true},
		{name: "bad timeout", dsn: "grpc://h:1?timeout=abc", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, err := ParseClusterDSN(tc.dsn)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseClusterDSN(%q) = %+v, want error", tc.dsn, o)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseClusterDSN(%q): %v", tc.dsn, err)
			}
			if o.Scheme != tc.wantScheme || o.Host != tc.wantHost ||
				o.Timeout != tc.wantTimeout || o.ConnectTimeout != tc.wantConnect || o.Interval != tc.wantInterval {
				t.Fatalf("ParseClusterDSN(%q) = %+v, mismatch", tc.dsn, o)
			}
			if o.Param("node_id") != tc.wantNodeID {
				t.Fatalf("node_id param = %q, want %q", o.Param("node_id"), tc.wantNodeID)
			}
		})
	}
}
