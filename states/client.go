package states

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	pq "github.com/lib/pq"

	"github.com/improvtrace/stateflux/states/ent"
	"github.com/improvtrace/stateflux/states/ent/taskpersistent"
)

// Open 按 DSN 连接 PostgreSQL，确认连接可达且调度表已就位后返回 ent 客户端，
// 调用方负责 Close。DSN 采用 URL 形式，如 postgres://user:pass@host:5432/db?sslmode=disable。
//
// 表结构由独立的 datamigrate 工具迁移，服务启动时只做校验：未迁移就立即失败，
// 而不是运行期每轮对账反复报错。
func Open(ctx context.Context, dsn string) (*ent.Client, error) {
	slog.Info("states: connecting to PostgreSQL")
	client, err := ent.Open(dialect.Postgres, dsn)
	if err != nil {
		return nil, fmt.Errorf("states: connect to PostgreSQL: %w", err)
	}
	if err := ready(ctx, client); err != nil {
		_ = client.Close()
		return nil, err
	}
	slog.Info("states: database ready")
	return client, nil
}

// NewClient 基于已有的 *sql.DB 构造 ent 客户端，连接的生命周期归调用方；不做
// 就绪检查（迁移尚未执行时也能构造）。
func NewClient(db *sql.DB) *ent.Client {
	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, db)))
}

// ready 用对调度表的一次查询同时确认数据库可达且表已存在。
func ready(ctx context.Context, client *ent.Client) error {
	if _, err := client.TaskPersistent.Query().Select(taskpersistent.FieldID).Limit(1).All(ctx); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "42P01" {
			return errors.New("states: task_persistents table is missing, run datamigrate first")
		}
		return fmt.Errorf("states: query task_persistents: %w", err)
	}
	return nil
}
