package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/improvtrace/stateflux/internal/domain/migration"
)

// fakeRunner 记录调用参数并返回固定结果。
type fakeRunner struct {
	gotDSN    string
	gotSearch string
	gotOpts   migration.Options
	calls     int
}

func (f *fakeRunner) Run(_ context.Context, dsn, searchPath string, opts migration.Options) ([]string, error) {
	f.calls++
	f.gotDSN, f.gotSearch, f.gotOpts = dsn, searchPath, opts
	return []string{"chronos.0.1"}, nil
}

func TestMigrateDefaultsToLatestTarget(t *testing.T) {
	fr := &fakeRunner{}
	var out strings.Builder
	cmd := NewMigrateCommand(MigrateDeps{Runner: fr.Run})
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--pg-dsn", "postgres://u:secret@127.0.0.1:5432/stateflux?sslmode=disable"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if fr.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", fr.calls)
	}
	if fr.gotOpts.Target != "" || fr.gotOpts.Rebuild {
		t.Fatalf("opts = %+v, want 空 target / 非 rebuild", fr.gotOpts)
	}
	if fr.gotSearch != "public" {
		t.Fatalf("search_path = %q, want public", fr.gotSearch)
	}
	if !strings.Contains(out.String(), "applied:  chronos.0.1") {
		t.Fatalf("stdout = %q, want applied 列表", out.String())
	}
	// DSN 中的密码不得回显。
	if strings.Contains(out.String(), "secret") {
		t.Fatalf("stdout 泄露密码: %q", out.String())
	}
}

func TestMigrateTargetAndRebuildFlags(t *testing.T) {
	fr := &fakeRunner{}
	cmd := NewMigrateCommand(MigrateDeps{Runner: fr.Run})
	cmd.SetArgs([]string{"--target", "chronos.0.1", "--rebuild"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if fr.gotOpts.Target != "chronos.0.1" || !fr.gotOpts.Rebuild {
		t.Fatalf("opts = %+v, want target=chronos.0.1 rebuild=true", fr.gotOpts)
	}
}

func TestMigrateNoPendingIsNoop(t *testing.T) {
	cmd := NewMigrateCommand(MigrateDeps{Runner: func(context.Context, string, string, migration.Options) ([]string, error) {
		return nil, nil
	}})
	var out strings.Builder
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out.String(), "无待应用迁移") {
		t.Fatalf("stdout = %q, want 无待应用迁移", out.String())
	}
}

// TestMigrateGenerateUsesRealGenerator 走真实 Generator（临时目录），验证
// --generate 不访问数据库即可同时产出 sql + go 脚手架。
func TestMigrateGenerateUsesRealGenerator(t *testing.T) {
	dir := t.TempDir()
	var out strings.Builder
	cmd := NewMigrateCommand(MigrateDeps{})
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--generate", "chronos.0.2", "--dir", dir, "--date", "20261001"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out.String(), dir+"/chronos.0.2_20261001.sql") ||
		!strings.Contains(out.String(), dir+"/chronos.0.2_20261001.go") {
		t.Fatalf("stdout = %q, want 同时生成的 sql+go 路径", out.String())
	}
}
