//go:build integration

// 集成测试：需要可建库的 PostgreSQL，缺省连 examples/maintain.sh 部署的实例，
// 可用环境变量 STATEFLUX_TEST_PG_DSN 指向其他实例的维护库（DSN 里的角色须有
// CREATEDB 权限）；不可达时 Skip。每个用例各自新建一次性库，用完即删。运行：
//
//	go test -tags=integration -v ./migrations/
package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	pq "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultTestPG 是 examples/maintain.sh 部署的 PostgreSQL 的维护库 DSN。
const defaultTestPG = "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"

// TestMigrateCreatesMissingDatabase 目标库不存在：Migrate 建库并应用全部迁移，
// 重复执行幂等。
func TestMigrateCreatesMissingDatabase(t *testing.T) {
	admin := testAdmin(t)
	name, dsn := testDatabase(t, admin)
	require.False(t, databaseExists(t, admin, name), "前置：目标库尚不存在")

	dir, err := Dir()
	require.NoError(t, err)
	files, err := dir.Files()
	require.NoError(t, err)
	want := make([]string, len(files))
	for i, f := range files {
		want[i] = f.Name()
	}

	applied, err := Migrate(context.Background(), dsn)
	require.NoError(t, err)
	assert.Equal(t, want, applied, "新库应应用全部迁移")
	assert.True(t, databaseExists(t, admin, name), "目标库应已创建")

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	assert.True(t, tableExists(t, db, "public.task_persistents"), "迁移建出的表应存在")

	// 库已存在：不再有待应用项，重复执行无动作
	applied, err = Migrate(context.Background(), dsn)
	require.NoError(t, err)
	assert.Empty(t, applied)
	pending, err := Pending(context.Background(), dsn)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// TestPendingKeepsMissingDatabase Pending 不改动数据库：目标库不存在时报错
// 且不建库。
func TestPendingKeepsMissingDatabase(t *testing.T) {
	admin := testAdmin(t)
	name, dsn := testDatabase(t, admin)

	_, err := Pending(context.Background(), dsn)
	require.Error(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("database %q does not exist", name))
	assert.ErrorContains(t, err, "Migrate creates it")
	assert.False(t, databaseExists(t, admin, name))
}

// TestMigrateKeepsOtherErrors 目标库不存在但连接失败另有原因（如密码错误）
// 时，不建库，错误原样返回。
func TestMigrateKeepsOtherErrors(t *testing.T) {
	admin := testAdmin(t)
	name, dsn := testDatabase(t, admin)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.User = url.UserPassword(u.User.Username(), "wrong-password")

	_, err = Migrate(context.Background(), u.String())
	require.Error(t, err)
	assert.ErrorContains(t, err, "password authentication failed")
	assert.False(t, databaseExists(t, admin, name))
}

// testPG 返回 PostgreSQL 维护库 DSN。
func testPG() string {
	if v := os.Getenv("STATEFLUX_TEST_PG_DSN"); v != "" {
		return v
	}
	return defaultTestPG
}

// testAdmin 连接维护库；PostgreSQL 不可达时跳过用例。
func testAdmin(t *testing.T) *sql.DB {
	t.Helper()
	admin, err := sql.Open("postgres", testPG())
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Skipf("PostgreSQL 不可达，请先执行 examples/maintain.sh install: %v", err)
	}
	return admin
}

// testDatabase 生成一个一次性库的库名与 DSN（库此时尚不存在），并在用例结束
// 后删除（建没建过都删）。
func testDatabase(t *testing.T, admin *sql.DB) (name, dsn string) {
	t.Helper()
	u, err := url.Parse(testPG())
	require.NoError(t, err)
	name = fmt.Sprintf("stateflux_migrate_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	u.Path, u.RawPath = "/"+name, ""
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+pq.QuoteIdentifier(name))
		assert.NoError(t, err, "删除测试库 %s", name)
	})
	return name, u.String()
}

// databaseExists 判断库是否存在。
func databaseExists(t *testing.T, admin *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, admin.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists))
	return exists
}

// tableExists 判断所连数据库里是否有某张表（传 schema 限定的名字，如
// public.task_persistents）。information_schema 只看得到当前库，因此 db 须
// 连到目标库。
func tableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
	return exists
}
