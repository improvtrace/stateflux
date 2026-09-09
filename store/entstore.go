package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx 作为 database/sql 驱动
	"github.com/lib/pq"                // 仅用 pq.Array 构造数组参数（驱动注册无副作用）

	entsql "entgo.io/ent/dialect/sql"

	"github.com/improvtrace/stateflux/sdk"
	ent "github.com/improvtrace/stateflux/store/ent"
)

// Options 默认实现构造参数。
type Options struct {
	// DSN PostgreSQL 连接串（pgx stdlib 驱动）。
	DSN string
	// Snowflake 任务 ID 生成器（必填；单节点实例）。
	Snowflake *sdk.Snowflake
	// MaxTxRows 单事务行数上限（防长事务，§5.1；默认 5000）。
	MaxTxRows int
	// MaxOpenConns / MaxIdleConns 连接池。
	MaxOpenConns int
	MaxIdleConns int
	// Logger 结构化日志（默认 slog.Default()）。
	Logger *slog.Logger
}

// pgStore 阶段集合逻辑接口的默认实现（§3.1）：PG 四阶段表 + payload 分离 + 结果集。
// 生命周期阶段由所在表表达；关键并发路径以原生 SQL 下沉，常规查询走 ent API。
type pgStore struct {
	db     *sql.DB
	client *ent.Client
	sf     *sdk.Snowflake
	log    *slog.Logger

	maxTxRows int
	migrateMu sync.Mutex
}

// Open 打开默认实现。不自动迁移；显式调用 Migrate（或由 cmd 装配决定）。
func Open(opts Options) (Store, error) {
	if opts.DSN == "" {
		return nil, fmt.Errorf("store: dsn is required")
	}
	if opts.Snowflake == nil {
		return nil, fmt.Errorf("store: snowflake is required")
	}
	if opts.MaxTxRows <= 0 {
		opts.MaxTxRows = 5000
	}
	db, err := sql.Open("pgx", opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: open pg: %w", err)
	}
	if opts.MaxOpenConns > 0 {
		db.SetMaxOpenConns(opts.MaxOpenConns)
	}
	if opts.MaxIdleConns > 0 {
		db.SetMaxIdleConns(opts.MaxIdleConns)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	drv := entsql.OpenDB(dialectPostgres, db)
	client := ent.NewClient(ent.Driver(drv))
	return &pgStore{
		db:        db,
		client:    client,
		sf:        opts.Snowflake,
		log:       log,
		maxTxRows: opts.MaxTxRows,
	}, nil
}

// dialectPostgres ent 方言常量。
const dialectPostgres = "postgres"

// Close 释放连接。
func (s *pgStore) Close() error { return s.client.Close() }

// migrateDDL 幂等键局部唯一索引 + NOTIFY 触发器（§3.1/§5.1）。
// ent 注解不支持谓词索引，局部唯一性（仅非空幂等键、三张非终态表）以此原生 DDL 补建。
// NOTIFY 用语句级触发器：批量创建只发一次通知；payload 留空（仅唤醒信号，不承载事实）。
var migrateDDL = []string{
	`CREATE UNIQUE INDEX IF NOT EXISTS pending_tasks_idempotency_uq
		ON pending_tasks (idempotency_key) WHERE idempotency_key <> ''`,
	`CREATE UNIQUE INDEX IF NOT EXISTS schedulable_tasks_idempotency_uq
		ON schedulable_tasks (idempotency_key) WHERE idempotency_key <> ''`,
	`CREATE UNIQUE INDEX IF NOT EXISTS processing_tasks_idempotency_uq
		ON processing_tasks (idempotency_key) WHERE idempotency_key <> ''`,
	`CREATE OR REPLACE FUNCTION stateflux_notify_new_task() RETURNS trigger AS $fn$
	BEGIN
		PERFORM pg_notify('stateflux_new_task', '');
		RETURN NULL;
	END;
	$fn$ LANGUAGE plpgsql`,
	`DROP TRIGGER IF EXISTS stateflux_new_task_trigger ON pending_tasks`,
	`CREATE TRIGGER stateflux_new_task_trigger AFTER INSERT ON pending_tasks
		FOR EACH STATEMENT EXECUTE FUNCTION stateflux_notify_new_task()`,
}

// Migrate 装配存储：ent 迁移建表 + 幂等键局部唯一索引 + NOTIFY 触发器（全部幂等）。
// 自定义 DDL 在 PG advisory lock 保护下执行——并发 Migrate（多实例/多 goroutine 同时启动）
// 否则会触发 PG "tuple concurrently updated"。
func (s *pgStore) Migrate(ctx context.Context) error {
	s.migrateMu.Lock()
	defer s.migrateMu.Unlock()
	if err := s.client.Schema.Create(ctx); err != nil {
		return fmt.Errorf("store: ent migrate: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin migrate tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // 提交后 rollback 是 no-op
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(721584392017)`); err != nil {
		return fmt.Errorf("store: migrate advisory lock: %w", err)
	}
	for _, ddl := range migrateDDL {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("store: bootstrap ddl: %w (ddl: %.80s)", err, ddl)
		}
	}
	return tx.Commit()
}

// ---- 公共辅助 ----

// validateNewTask 创建入参校验与归一（§5.1）：payload 必须 JSON（jsonb 列契约，§3.1），
// 大 payload（>64KB）不入库由业务方写对象存储后传引用。
func validateNewTask(item *sdk.NewTask) error {
	if item.Type == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidTask)
	}
	if len(item.Payload) > 0 && !json.Valid(item.Payload) {
		return fmt.Errorf("%w: payload must be valid json", ErrInvalidTask)
	}
	if !item.Priority.Normalize().Valid() {
		return fmt.Errorf("%w: invalid priority %q", ErrInvalidTask, item.Priority)
	}
	if !item.ExecMode.Normalize().Valid() {
		return fmt.Errorf("%w: invalid exec_mode %q", ErrInvalidTask, item.ExecMode)
	}
	if item.Callback != nil {
		if err := item.Callback.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidTask, err)
		}
	}
	return nil
}

// jsonBytes 把 jsonb 参数统一为 string（pgx stdlib 对 []byte 走 bytea 编码，
// 显式 string + $n::jsonb 消除歧义）。nil 返回 SQL NULL。
func jsonBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// normalizeJSONB 保证落库结果是合法 JSON：handler 结果允许任意字节，非 JSON 时包一层字符串。
func normalizeJSONB(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		return b
	}
	out, _ := json.Marshal(string(b))
	return out
}

// stageColumns 四阶段表共享的核心列（与 stageMixin 一致，顺序即扫描顺序）。
const stageColumns = `id, type, priority, exec_mode, run_at, timeout_ms, max_attempts, attempts,
	owner_node, error, idempotency_key, batch_id, callback, parent_task_id, created_at, updated_at`

// stageColumnsNoUpdated 不含 updated_at 的核心列（挪行时需单点控制 updated_at 语义的语句使用）。
const stageColumnsNoUpdated = `id, type, priority, exec_mode, run_at, timeout_ms, max_attempts, attempts,
	owner_node, error, idempotency_key, batch_id, callback, parent_task_id, created_at`

// scanTask 扫描一行核心列（不含 payload）。
func scanTask(scan func(dest ...any) error) (*sdk.Task, error) {
	t := &sdk.Task{}
	var callback []byte
	var runAt, createdAt, updatedAt time.Time
	if err := scan(
		&t.ID, &t.Type, &t.Priority, &t.ExecMode, &runAt, &t.TimeoutMS, &t.MaxAttempts,
		&t.Attempts, &t.OwnerNode, &t.Error, &t.IdempotencyKey, &t.BatchID,
		&callback, &t.ParentTaskID, &createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}
	t.RunAt = runAt
	t.CreatedAt = createdAt
	t.UpdatedAt = updatedAt
	t.Priority = sdk.Priority(t.Priority)
	t.ExecMode = sdk.ExecMode(t.ExecMode)
	if len(callback) > 0 {
		spec, err := sdk.UnmarshalCallback(callback)
		if err != nil {
			return nil, fmt.Errorf("store: unmarshal callback: %w", err)
		}
		t.Callback = spec
	}
	return t, nil
}

// pqArray 数组参数构造（lib/pq 的 Valuer，与 pgx stdlib 兼容）。
func pqArray[T any](v []T) any { return pq.Array(v) }

// errUniqueViolation PostgreSQL 唯一约束冲突（并发幂等插入竞态的统一出口）。
const errUniqueViolation = "23505"

// queryBuilder 位置参数 SQL 构造器（$1、$2…），供批量 INSERT/CTE 拼接。
type queryBuilder struct {
	buf  []byte
	args []any
}

func newQueryBuilder() *queryBuilder { return &queryBuilder{} }

// WriteString 追加 SQL 片段。
func (b *queryBuilder) WriteString(s string) { b.buf = append(b.buf, s...) }

// arg 追加一个位置参数，返回占位符。
func (b *queryBuilder) arg(v any) string {
	b.args = append(b.args, v)
	return fmt.Sprintf("$%d", len(b.args))
}

// writeArg 追加一个位置参数并直接写入占位符。
func (b *queryBuilder) writeArg(v any) { b.WriteString(b.arg(v)) }

// cast 追加一个带显式类型转换的位置参数（jsonb 等需要消除驱动类型歧义的列），返回完整片段。
func (b *queryBuilder) cast(v any, typ string) string {
	return b.arg(v) + "::" + typ
}

// writeCast 追加一个带显式类型转换的位置参数并直接写入。
func (b *queryBuilder) writeCast(v any, typ string) { b.WriteString(b.cast(v, typ)) }

// comma 在多行 VALUES/多条件拼接间补逗号。
func (b *queryBuilder) comma(first bool) {
	if !first {
		b.WriteString(",")
	}
}

// String 返回拼好的 SQL。
func (b *queryBuilder) String() string { return string(b.buf) }
