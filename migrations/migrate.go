package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"time"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlclient"

	// Atlas 的 PostgreSQL 驱动在包初始化时向 sqlclient 注册自己，必须显式引入；
	// pq 注册 database/sql 的 "postgres" 驱动，pq.Array 用于账本数组列。
	_ "ariga.io/atlas/sql/postgres"
	pq "github.com/lib/pq"
)

const (
	// revisionsTable 是迁移账本表：每个迁移文件一行，记录已应用的语句数与校验和。
	revisionsTable = "atlas_schema_revisions"
	// migrationOperator 是写入账本的执行方标识。
	migrationOperator = "stateflux"
	// defaultSchema 是 DSN 未指定 search_path 时的迁移目标 schema。
	defaultSchema = "public"
	// undefinedTable 是 PostgreSQL 的「表不存在」错误码。
	undefinedTable = "42P01"
	// missingDatabase 是 PostgreSQL 的「数据库不存在」错误码。
	missingDatabase = "3D000"
	// maintenanceDB 是建库时连接的维护库：CREATE DATABASE 只能在目标库之外执行。
	maintenanceDB = "postgres"
)

// revisionsTableDDL 是账本表的建表语句，列与 Atlas 的 migrate.Revision 一一对应。
const revisionsTableDDL = `CREATE TABLE IF NOT EXISTS atlas_schema_revisions (
	version          TEXT PRIMARY KEY,
	description      TEXT   NOT NULL,
	type             BIGINT NOT NULL,
	applied          BIGINT NOT NULL,
	total            BIGINT NOT NULL,
	executed_at      TIMESTAMPTZ NOT NULL,
	execution_time   BIGINT NOT NULL,
	error            TEXT   NOT NULL DEFAULT '',
	error_stmt       TEXT   NOT NULL DEFAULT '',
	hash             TEXT   NOT NULL DEFAULT '',
	partial_hashes   TEXT[] NOT NULL DEFAULT '{}',
	operator_version TEXT   NOT NULL DEFAULT ''
)`

// revisionsQuery 是账本读取列，顺序与 scanRevision 一致。
const revisionsQuery = `SELECT version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version FROM atlas_schema_revisions`

// Dir 加载内嵌的迁移目录（本包嵌入的 *.sql 与 atlas.sum），并校验其与
// atlas.sum 一致。
func Dir() (migrate.Dir, error) {
	dir := &migrate.MemDir{}
	entries, err := fs.ReadDir(FS, ".")
	if err != nil {
		return nil, fmt.Errorf("migrations: read migration directory: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := FS.ReadFile(e.Name())
		if err != nil {
			return nil, fmt.Errorf("migrations: read migration file %s: %w", e.Name(), err)
		}
		if err := dir.WriteFile(e.Name(), data); err != nil {
			return nil, fmt.Errorf("migrations: load migration file %s: %w", e.Name(), err)
		}
	}
	if err := migrate.Validate(dir); err != nil {
		return nil, fmt.Errorf("migrations: migration directory does not match atlas.sum: %w", err)
	}
	return dir, nil
}

// Pending 返回尚未应用到 dsn 所指数据库的迁移文件名（按应用顺序）。不改动
// 数据库——账本表不存在时也不会创建，目标库不存在时也不建库而是报错。
func Pending(ctx context.Context, dsn string) ([]string, error) {
	client, _, ex, err := open(ctx, dsn)
	if err != nil {
		if isMissingDatabase(err) {
			// 只保留底层 pq 错误：open 的「连接数据库」前缀在此已无意义
			var pqErr *pq.Error
			_ = errors.As(err, &pqErr) // isMissingDatabase 已确认错误链中有 *pq.Error
			return nil, fmt.Errorf("migrations: target database does not exist (Pending does not modify the database; Migrate creates it): %w", pqErr)
		}
		return nil, err
	}
	defer func() { _ = client.Close() }()
	files, err := pendingFiles(ctx, ex)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = f.Name()
	}
	return names, nil
}

// Migrate 执行版本化迁移：把迁移目录中尚未应用的迁移文件按序应用到 dsn
// 所指的数据库，返回本次应用的文件名（无待应用文件时为空）。每个文件在单
// 事务内执行，语句与账本记录同生共死；已应用的文件跳过，重复执行幂等。
//
// dsn 所指的数据库不存在时，先以 DSN 中的同一角色连接维护库 postgres 建库
// （角色须有 CREATEDB 权限），再执行迁移。
//
// 首次执行要求数据库为空（目标 schema 下只允许存在账本表），否则 Atlas 判定
// 数据库不干净而拒绝——已有表需要接管时应人工核对后写入账本基线。迁移不加
// 跨进程锁，由单一进程执行。
func Migrate(ctx context.Context, dsn string) ([]string, error) {
	client, dir, ex, err := openTarget(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close() }()
	if _, err := client.DB.ExecContext(ctx, revisionsTableDDL); err != nil {
		return nil, fmt.Errorf("migrations: bootstrap revisions table: %w", err)
	}

	files, err := pendingFiles(ctx, ex)
	if err != nil {
		return nil, err
	}
	var applied []string
	for _, f := range files {
		if err := applyFile(ctx, client, dir, f); err != nil {
			return applied, fmt.Errorf("migrations: apply migration %s: %w", f.Name(), err)
		}
		applied = append(applied, f.Name())
	}
	return applied, nil
}

// open 连接目标库并返回迁移执行器；不改动数据库（账本表由 Migrate 创建）。
func open(ctx context.Context, dsn string) (*sqlclient.Client, migrate.Dir, *migrate.Executor, error) {
	dir, err := Dir()
	if err != nil {
		return nil, nil, nil, err
	}
	u, err := withSchema(dsn)
	if err != nil {
		return nil, nil, nil, err
	}
	client, err := sqlclient.Open(ctx, u)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("migrations: connect to database: %w", err)
	}
	ex, err := migrate.NewExecutor(client.Driver, dir, revisions{client.DB}, migrate.WithOperatorVersion(migrationOperator))
	if err != nil {
		_ = client.Close()
		return nil, nil, nil, fmt.Errorf("migrations: build migration executor: %w", err)
	}
	return client, dir, ex, nil
}

// openTarget 打开 Migrate 的目标库：目标库不存在时先建库再重连，其余连接
// 错误原样返回。
func openTarget(ctx context.Context, dsn string) (*sqlclient.Client, migrate.Dir, *migrate.Executor, error) {
	client, dir, ex, err := open(ctx, dsn)
	if err == nil || !isMissingDatabase(err) {
		return client, dir, ex, err
	}
	if err := createDatabase(ctx, dsn); err != nil {
		return nil, nil, nil, err
	}
	return open(ctx, dsn)
}

// createDatabase 创建 dsn 所指的数据库：以 DSN 中的同一实例与角色连接维护库
// 执行 CREATE DATABASE，建库角色即库的属主。CREATE DATABASE 不能在目标库内、
// 也不能在事务内执行。
func createDatabase(ctx context.Context, dsn string) error {
	name, adminDSN, err := maintenanceDSN(dsn)
	if err != nil {
		return err
	}
	if name == "" {
		return errors.New("migrations: target database does not exist and the DSN has no database name; cannot create it")
	}
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return fmt.Errorf("migrations: build maintenance connection: %w", err)
	}
	defer func() { _ = admin.Close() }()
	// CREATE DATABASE 不支持参数化，标识符经 pq.QuoteIdentifier 转义
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+pq.QuoteIdentifier(name)); err != nil {
		return fmt.Errorf("migrations: create database %s (via maintenance database %s): %w", name, maintenanceDB, err)
	}
	return nil
}

// maintenanceDSN 由目标库 DSN 推出建库用的维护库 DSN，并返回目标库名：实例、
// 角色与连接参数（sslmode 等）均沿用目标库 DSN，只把库换成维护库。
func maintenanceDSN(dsn string) (name, adminDSN string, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", fmt.Errorf("migrations: parse DSN: %w", err)
	}
	// 与 pq 解析 DSN 的方式一致：库名取 URL 解码后的路径
	name = strings.TrimPrefix(u.Path, "/")
	u.Path, u.RawPath = "/"+maintenanceDB, ""
	return name, u.String(), nil
}

// pendingFiles 返回待应用的迁移文件；没有时返回空切片。
func pendingFiles(ctx context.Context, ex *migrate.Executor) ([]migrate.File, error) {
	files, err := ex.Pending(ctx)
	if errors.Is(err, migrate.ErrNoPendingFiles) {
		return nil, nil
	}
	var dirty *migrate.NotCleanError
	if errors.As(err, &dirty) {
		// Atlas 的原因里只列它看到的第一张表（往往就是账本表本身），补充说明
		// 真正的判定条件。
		return nil, fmt.Errorf("migrations: target schema is not empty, Atlas refused to migrate (%s); the first migration requires an empty database with only the revisions table, adopting an existing database requires a manually verified baseline: %w", dirty.Reason, err)
	}
	if err != nil {
		return nil, fmt.Errorf("migrations: compute pending migrations: %w", err)
	}
	return files, nil
}

// applyFile 在单事务内执行一个迁移文件：语句与账本记录共用同一事务，失败
// 整体回滚（文件不被记为部分应用）。
func applyFile(ctx context.Context, client *sqlclient.Client, dir migrate.Dir, f migrate.File) error {
	tx, err := client.Tx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	ex, err := migrate.NewExecutor(tx.Driver, dir, revisions{tx.Tx}, migrate.WithOperatorVersion(migrationOperator))
	if err != nil {
		return err
	}
	if err := ex.Execute(ctx, f); err != nil {
		return err
	}
	return tx.Commit()
}

// withSchema 保证 DSN 带有 search_path（缺省 public）：Atlas 以它界定迁移的
// 目标 schema——判空检查只看该 schema，账本表也建在该 schema 下。DSN 须为 URL
// 形式。
func withSchema(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("migrations: parse DSN: %w", err)
	}
	q := u.Query()
	if q.Get("search_path") == "" {
		q.Set("search_path", defaultSchema)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// revisions 在给定连接上读写迁移账本。ent 依赖的 Atlas 发行版只含迁移执行器、
// 不含账本读写实现，故按其 RevisionReadWriter 接口在此实现。
type revisions struct {
	schema.ExecQuerier
}

// Ident 返回账本表标识（Atlas 的判空检查据此豁免本表）。
func (r revisions) Ident() *migrate.TableIdent {
	return &migrate.TableIdent{Name: revisionsTable}
}

// ReadRevisions 按版本升序返回全部账本记录；账本表尚未创建时视为没有记录。
func (r revisions) ReadRevisions(ctx context.Context) ([]*migrate.Revision, error) {
	rows, err := r.QueryContext(ctx, revisionsQuery+` ORDER BY version`)
	if err != nil {
		if isUndefinedTable(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var revs []*migrate.Revision
	for rows.Next() {
		rev, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		revs = append(revs, rev)
	}
	return revs, rows.Err()
}

// ReadRevision 按版本取单条，不存在返回 migrate.ErrRevisionNotExist。
func (r revisions) ReadRevision(ctx context.Context, version string) (*migrate.Revision, error) {
	rows, err := r.QueryContext(ctx, revisionsQuery+` WHERE version = $1`, version)
	if err != nil {
		if isUndefinedTable(err) {
			return nil, migrate.ErrRevisionNotExist
		}
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, migrate.ErrRevisionNotExist
	}
	return scanRevision(rows)
}

// WriteRevision 按版本覆盖写一条账本记录。
func (r revisions) WriteRevision(ctx context.Context, rev *migrate.Revision) error {
	hashes := rev.PartialHashes
	if hashes == nil {
		hashes = []string{}
	}
	_, err := r.ExecContext(ctx, `INSERT INTO atlas_schema_revisions
	(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (version) DO UPDATE SET
	description = EXCLUDED.description, type = EXCLUDED.type, applied = EXCLUDED.applied,
	total = EXCLUDED.total, executed_at = EXCLUDED.executed_at, execution_time = EXCLUDED.execution_time,
	error = EXCLUDED.error, error_stmt = EXCLUDED.error_stmt, hash = EXCLUDED.hash,
	partial_hashes = EXCLUDED.partial_hashes, operator_version = EXCLUDED.operator_version`,
		rev.Version, rev.Description, rev.Type, rev.Applied, rev.Total,
		rev.ExecutedAt, rev.ExecutionTime.Milliseconds(), rev.Error, rev.ErrorStmt,
		rev.Hash, pq.Array(hashes), rev.OperatorVersion,
	)
	return err
}

// DeleteRevision 按版本删除一条账本记录。
func (r revisions) DeleteRevision(ctx context.Context, version string) error {
	_, err := r.ExecContext(ctx, `DELETE FROM atlas_schema_revisions WHERE version = $1`, version)
	return err
}

// scanRevision 读取当前行；execution_time 在账本中以毫秒存储。
func scanRevision(rows *sql.Rows) (*migrate.Revision, error) {
	var (
		rev migrate.Revision
		ms  int64
	)
	if err := rows.Scan(
		&rev.Version, &rev.Description, &rev.Type, &rev.Applied, &rev.Total,
		&rev.ExecutedAt, &ms, &rev.Error, &rev.ErrorStmt,
		&rev.Hash, pq.Array(&rev.PartialHashes), &rev.OperatorVersion,
	); err != nil {
		return nil, err
	}
	rev.ExecutionTime = time.Duration(ms) * time.Millisecond
	return &rev, nil
}

// isUndefinedTable 判断错误是否为 PostgreSQL 的「表不存在」。
func isUndefinedTable(err error) bool {
	var e *pq.Error
	return errors.As(err, &e) && e.Code == undefinedTable
}

// isMissingDatabase 判断错误是否为 PostgreSQL 的「数据库不存在」。
func isMissingDatabase(err error) bool {
	var e *pq.Error
	return errors.As(err, &e) && e.Code == missingDatabase
}
