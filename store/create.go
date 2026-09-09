package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/improvtrace/stateflux/sdk"
)

// pendingRow 一条待插入的任务（任务行 + 分离 payload 行同事务写入，§5.1）。
type pendingRow struct {
	idx     int // 在本次 chunk 的 items 中的下标
	id      int64
	payload []byte
}

// CreatePending 创建入集（§5.1，批处理）：单事务多行 INSERT pending_tasks + task_payloads；
// 幂等键冲突跳过插入、返回已存在 task_id（两模式幂等语义一致）。
// 并发路径原生下沉：INSERT ... ON CONFLICT (idempotency_key) WHERE idempotency_key <> ” DO NOTHING，
// 空幂等键行不受唯一性约束。只写 PG，不触碰 Redis——创建与调度彻底解耦。
func (s *pgStore) CreatePending(ctx context.Context, items []sdk.NewTask) ([]CreatedTask, error) {
	if len(items) == 0 {
		return nil, ErrEmptyBatch
	}
	for i := range items {
		if err := validateNewTask(&items[i]); err != nil {
			return nil, err
		}
	}
	out := make([]CreatedTask, len(items))
	for start := 0; start < len(items); start += s.maxTxRows {
		end := min(start+s.maxTxRows, len(items))
		if err := s.createChunk(ctx, items[start:end], out[start:end]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *pgStore) createChunk(ctx context.Context, items []sdk.NewTask, out []CreatedTask) error {
	now := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // 提交后 rollback 是 no-op

	// 预查幂等键：非终态三表任一命中即视为已存在（§3.1 幂等键唯一性覆盖非终态三表）。
	keys := make([]string, 0, len(items))
	for i := range items {
		if items[i].IdempotencyKey != "" {
			keys = append(keys, items[i].IdempotencyKey)
		}
	}
	existing := make(map[string]int64, len(keys))
	if len(keys) > 0 {
		q := `SELECT idempotency_key, id FROM pending_tasks WHERE idempotency_key = ANY($1)
			UNION ALL SELECT idempotency_key, id FROM schedulable_tasks WHERE idempotency_key = ANY($1)
			UNION ALL SELECT idempotency_key, id FROM processing_tasks WHERE idempotency_key = ANY($1)`
		rows, err := tx.QueryContext(ctx, q, pq.Array(keys))
		if err != nil {
			return fmt.Errorf("store: lookup idempotency keys: %w", err)
		}
		for rows.Next() {
			var key string
			var id int64
			if err := rows.Scan(&key, &id); err != nil {
				rows.Close()
				return fmt.Errorf("store: scan idempotency key: %w", err)
			}
			existing[key] = id
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("store: iterate idempotency keys: %w", err)
		}
		rows.Close()
	}

	// 生成任务 ID 并组装待插行。
	var toInsert []pendingRow
	for i := range items {
		item := &items[i]
		if item.IdempotencyKey != "" {
			if id, ok := existing[item.IdempotencyKey]; ok {
				out[i] = CreatedTask{TaskID: id, Duplicate: true}
				continue
			}
		}
		id, err := s.sf.Next()
		if err != nil {
			return fmt.Errorf("store: generate task id: %w", err)
		}
		toInsert = append(toInsert, pendingRow{idx: i, id: id, payload: item.Payload})
	}
	if len(toInsert) > 0 {
		if err := insertPendingRows(ctx, tx, toInsert, items, now); err != nil {
			return err
		}
	}

	// 为实际插入成功的行写 payload（同事务）。
	// 带幂等键的行重新解析权威 ID：本事务插入成功 → 自己的 ID；并发竞态被跳过 → 对方的 ID。
	var inserted []pendingRow
	for _, row := range toInsert {
		item := &items[row.idx]
		if item.IdempotencyKey == "" {
			inserted = append(inserted, row)
			out[row.idx] = CreatedTask{TaskID: row.id}
			continue
		}
		var id int64
		q := `SELECT id FROM pending_tasks WHERE idempotency_key = $1`
		if err := tx.QueryRowContext(ctx, q, item.IdempotencyKey).Scan(&id); err != nil {
			return fmt.Errorf("store: resolve idempotent id: %w", err)
		}
		if id == row.id {
			inserted = append(inserted, row)
			out[row.idx] = CreatedTask{TaskID: id}
		} else {
			out[row.idx] = CreatedTask{TaskID: id, Duplicate: true}
		}
	}
	if len(inserted) > 0 {
		if err := insertPayloadRows(ctx, tx, inserted); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// insertPendingRows 多行 INSERT pending_tasks（幂等键冲突跳过）。
func insertPendingRows(ctx context.Context, tx *sql.Tx, rows []pendingRow, items []sdk.NewTask, now time.Time) error {
	sb := newQueryBuilder()
	sb.WriteString(`INSERT INTO pending_tasks (id, type, priority, exec_mode, run_at, timeout_ms,
		max_attempts, attempts, owner_node, error, idempotency_key, batch_id, callback, parent_task_id,
		created_at, updated_at) VALUES `)
	for i := range rows {
		row := &rows[i]
		item := &items[row.idx]
		runAt := item.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		timeout := item.TimeoutMS
		if timeout == 0 {
			timeout = int64(sdk.DefaultTimeoutMS)
		}
		maxAttempts := item.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = sdk.DefaultMaxAttempts
		}
		cb, err := item.Callback.Marshal()
		if err != nil {
			return fmt.Errorf("store: marshal callback: %w", err)
		}
		sb.comma(i == 0)
		sb.WriteString("(")
		sb.writeArg(row.id) // id
		sb.WriteString(",")
		sb.writeArg(item.Type) // type
		sb.WriteString(",")
		sb.writeArg(string(item.Priority.Normalize())) // priority
		sb.WriteString(",")
		sb.writeArg(string(item.ExecMode.Normalize())) // exec_mode
		sb.WriteString(",")
		sb.writeArg(runAt) // run_at
		sb.WriteString(",")
		sb.writeArg(timeout) // timeout_ms
		sb.WriteString(",")
		sb.writeArg(maxAttempts) // max_attempts
		sb.WriteString(",")
		sb.writeArg(int64(0)) // attempts
		sb.WriteString(",")
		sb.writeArg("") // owner_node
		sb.WriteString(",")
		sb.writeArg("") // error
		sb.WriteString(",")
		sb.writeArg(item.IdempotencyKey) // idempotency_key
		sb.WriteString(",")
		sb.writeArg(item.BatchID) // batch_id
		sb.WriteString(",")
		sb.writeCast(jsonBytes(cb), "jsonb") // callback
		sb.WriteString(",")
		sb.writeArg(item.ParentTaskID) // parent_task_id
		sb.WriteString(",")
		sb.writeArg(now) // created_at
		sb.WriteString(",")
		sb.writeArg(now) // updated_at
		sb.WriteString(")")
	}
	sb.WriteString(` ON CONFLICT (idempotency_key) WHERE idempotency_key <> '' DO NOTHING`)
	if _, err := tx.ExecContext(ctx, sb.String(), sb.args...); err != nil {
		return fmt.Errorf("store: insert pending rows: %w", err)
	}
	return nil
}

// insertPayloadRows 多行 INSERT task_payloads。payload 列 NOT NULL：空 payload 落 JSON null。
func insertPayloadRows(ctx context.Context, tx *sql.Tx, rows []pendingRow) error {
	sb := newQueryBuilder()
	sb.WriteString(`INSERT INTO task_payloads (task_id, payload, created_at) VALUES `)
	for i := range rows {
		p := normalizeJSONB(rows[i].payload)
		if len(p) == 0 {
			p = []byte("null")
		}
		sb.comma(i == 0)
		sb.WriteString("(")
		sb.writeArg(rows[i].id)
		sb.WriteString(",")
		sb.writeCast(jsonBytes(p), "jsonb")
		sb.WriteString(",")
		sb.writeArg(time.Now())
		sb.WriteString(")")
	}
	if _, err := tx.ExecContext(ctx, sb.String(), sb.args...); err != nil {
		return fmt.Errorf("store: insert payload rows: %w", err)
	}
	return nil
}
