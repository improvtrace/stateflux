package info

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/improvtrace/stateflux/internal/cluster"
)

func TestInfoCommandDefaultsToLocalDSN(t *testing.T) {
	var got string
	var out strings.Builder
	cmd := NewInfoCommand(InfoDeps{Querier: func(_ context.Context, dsn string) (cluster.Info, error) {
		got = dsn
		return cluster.Info{}, nil
	}})
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got != "local://localhost" {
		t.Fatalf("dsn = %q, want configured default local://localhost", got)
	}
	if !strings.Contains(out.String(), "node_id:") {
		t.Fatalf("stdout = %q, want described view", out.String())
	}
	if !strings.Contains(out.String(), "build:     version=") {
		t.Fatalf("stdout = %q, want build info line", out.String())
	}
}

func TestInfoCommandDSNFlagOverrides(t *testing.T) {
	var got string
	cmd := NewInfoCommand(InfoDeps{Querier: func(_ context.Context, dsn string) (cluster.Info, error) {
		got = dsn
		return cluster.Info{}, nil
	}})
	cmd.SetArgs([]string{"--cluster-dsn", "grpc://127.0.0.1:9190?timeout=2s"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got != "grpc://127.0.0.1:9190?timeout=2s" {
		t.Fatalf("dsn = %q, want flag value", got)
	}
}

// TestInfoCommandConfigFilePrecedence 覆盖 默认值 → 配置文件 → flag 的优先级。
func TestInfoCommandConfigFilePrecedence(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "stateflux.yaml")
	yaml := `
cluster:
  dsn: http://10.0.0.5:8080
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "file wins over default", args: []string{"--config", file}, want: "http://10.0.0.5:8080"},
		{name: "flag wins over file", args: []string{"--config", file, "--cluster-dsn", "grpc://127.0.0.1:9190"}, want: "grpc://127.0.0.1:9190"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			cmd := NewInfoCommand(InfoDeps{Querier: func(_ context.Context, dsn string) (cluster.Info, error) {
				got = dsn
				return cluster.Info{}, nil
			}})
			cmd.SetArgs(tc.args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got != tc.want {
				t.Fatalf("dsn = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestInfoCommandErrorPropagates(t *testing.T) {
	sentinel := errors.New("unreachable")
	cmd := NewInfoCommand(InfoDeps{Querier: func(context.Context, string) (cluster.Info, error) {
		return cluster.Info{}, sentinel
	}})
	cmd.SetArgs([]string{"--cluster-dsn", "grpc://127.0.0.1:1?timeout=1s"})
	err := cmd.Execute()
	if err == nil || !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "info:") {
		t.Fatalf("Execute error = %v, want wrapped sentinel", err)
	}
}
