// Package testpg 是测试专用的共享 embedded PostgreSQL 基座：每个测试二进制首次调用
// Start 时拉起一个真实 PG（embedded-postgres 下载官方二进制），验证 SKIP LOCKED、
// 部分唯一索引、数据修改型 CTE 等 PG 专属行为（§12.2）。
package testpg

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

var (
	once     sync.Once
	startErr error
	dsn      string
	pg       *embeddedpostgres.EmbeddedPostgres
	dataDir  string
)

// Start 返回共享 PG 的 DSN（首次调用拉起实例；测试进程结束前调用 Stop）。
func Start() (string, error) {
	once.Do(func() {
		var err error
		dataDir, err = os.MkdirTemp("", "stateflux-pg-*")
		if err != nil {
			startErr = err
			return
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			startErr = err
			return
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		pg = embeddedpostgres.NewDatabase(
			embeddedpostgres.DefaultConfig().
				Username("postgres").Password("postgres").
				Database("stateflux_test").Port(uint32(port)).
				RuntimePath(filepath.Join(dataDir, "data")).
				Logger(io.Discard),
		)
		if err := pg.Start(); err != nil {
			startErr = fmt.Errorf("testpg: start embedded postgres: %w", err)
			return
		}
		dsn = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/stateflux_test?sslmode=disable", port)
	})
	return dsn, startErr
}

// Stop 停止并清理共享实例（TestMain 收尾调用）。
func Stop() {
	if pg != nil {
		_ = pg.Stop()
	}
	if dataDir != "" {
		_ = os.RemoveAll(dataDir)
	}
}

// Open 打开一个已迁移的 store（每测试用例独立雪花节点）。
func Open(t *testing.T) store.Store {
	t.Helper()
	d, err := Start()
	if err != nil {
		t.Fatal(err)
	}
	nodeSeq++
	sf, err := sdk.NewSnowflake(nodeSeq % 1024)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(store.Options{
		DSN: d, Snowflake: sf, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("testpg: open store: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("testpg: migrate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// Truncate 清空全部任务表（用例间隔离）。
func Truncate(t *testing.T) {
	t.Helper()
	d, err := Start()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", d)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`TRUNCATE pending_tasks, schedulable_tasks, processing_tasks,
		completed_tasks, task_payloads, task_results`)
	if err != nil {
		t.Fatal(err)
	}
}

// DB 打开原始连接（用例内直查校验）。
func DB(t *testing.T) *sql.DB {
	t.Helper()
	d, err := Start()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var nodeSeq int64
