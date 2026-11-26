package migrations

import (
	"context"
	"net/url"
	"testing"

	pq "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMaintenanceDSN 断言维护库 DSN 沿用目标库 DSN 的实例、角色与连接参数，
// 只把库换成维护库；目标库名取 URL 解码后的路径（与 pq 解析 DSN 的方式一致）。
func TestMaintenanceDSN(t *testing.T) {
	tests := []struct {
		dsn string
		// wantName 是目标库名；wantAdmin 为空时只断言实例、角色与查询参数沿用
		wantName  string
		wantAdmin string
	}{
		{
			dsn:       "postgres://postgres:postgres@127.0.0.1:5432/stateflux?sslmode=disable",
			wantName:  "stateflux",
			wantAdmin: "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable",
		},
		{
			dsn:       "postgres://u:p@db.internal:5432/stateflux",
			wantName:  "stateflux",
			wantAdmin: "postgres://u:p@db.internal:5432/postgres",
		},
		{
			dsn:       "postgres://u:p@[::1]:5432/stateflux?sslmode=disable",
			wantName:  "stateflux",
			wantAdmin: "postgres://u:p@[::1]:5432/postgres?sslmode=disable",
		},
		{
			// 密码含需转义的字符：重建 URL 后凭据保持可还原
			dsn:      "postgres://u:p%40ss%3Aw%2Frd@db:5432/stateflux?sslmode=disable",
			wantName: "stateflux",
		},
		{
			// 路径经 URL 解码：与 pq 取库名的方式一致
			dsn:      "postgres://u:p@db:5432/%73tateflux",
			wantName: "stateflux",
		},
		{
			// 查询参数原样保留（不重新编码）
			dsn:      "postgres://u:p@db:5432/stateflux?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fca.pem",
			wantName: "stateflux",
		},
		{
			// 未指定库名：库名为空，由 createDatabase 报错
			dsn:      "postgres://u:p@db:5432?sslmode=disable",
			wantName: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			name, adminDSN, err := maintenanceDSN(tt.dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, name)

			if tt.wantAdmin != "" {
				assert.Equal(t, tt.wantAdmin, adminDSN)
				return
			}
			want, err := url.Parse(tt.dsn)
			require.NoError(t, err)
			got, err := url.Parse(adminDSN)
			require.NoError(t, err)
			assert.Equal(t, want.User, got.User)
			assert.Equal(t, want.Host, got.Host)
			assert.Equal(t, want.RawQuery, got.RawQuery)
			assert.Equal(t, "/"+maintenanceDB, got.Path)
		})
	}
}

// TestCreateDatabaseWithoutName 断言 DSN 未指定库名时不建库而是报错（不到
// 连接数据库那一步）。
func TestCreateDatabaseWithoutName(t *testing.T) {
	err := createDatabase(context.Background(), "postgres://u:p@db.invalid:5432?sslmode=disable")
	assert.ErrorContains(t, err, "has no database name")
}

// TestIsMissingDatabase 断言只有「数据库不存在」这一错误码被识别为需要建库。
func TestIsMissingDatabase(t *testing.T) {
	assert.False(t, isMissingDatabase(nil))
	assert.False(t, isMissingDatabase(assert.AnError))
	assert.True(t, isMissingDatabase(&pq.Error{Code: missingDatabase}))
	// 账本表不存在等其它错误不触发建库
	assert.False(t, isMissingDatabase(&pq.Error{Code: undefinedTable}))
}
