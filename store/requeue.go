package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// requeueSQL 对账重置（§6.3 R1/R3）：
//   - moved：attempt 匹配的 processing 行（fencing token 防与终态回写/新认领竞态）；
//   - requeued：attempts < max_attempts → 挪回 schedulable（attempt 不变，run_at = 退避后时间，
//     约束已通过不重评，§6.3）；
//   - dead：attempts >= max_attempts → completed{dead}（R3），payload 合并 + task_results 写入。
const requeueSQL = `WITH v(id, attempt, run_at, error) AS (
	VALUES %ROWS%
), moved AS (
	DELETE FROM processing_tasks p USING v
	WHERE p.id = v.id AND p.attempts = v.attempt
	RETURNING p.id, p.type, p.priority, p.exec_mode, p.run_at, p.timeout_ms, p.max_attempts,
		p.attempts, p.owner_node, p.idempotency_key, p.batch_id, p.callback, p.parent_task_id,
		p.created_at, v.run_at AS v_run_at, v.error AS v_error
), requeued AS (
	INSERT INTO schedulable_tasks (` + stageColumns + `)
	SELECT m.id, m.type, m.priority, m.exec_mode, m.v_run_at, m.timeout_ms, m.max_attempts,
		m.attempts, '', '', m.idempotency_key, m.batch_id, m.callback, m.parent_task_id,
		m.created_at, now()
	FROM moved m
	WHERE m.attempts < m.max_attempts::bigint
	ON CONFLICT (id) DO NOTHING
	RETURNING id
), dead AS (
	INSERT INTO completed_tasks (` + stageColumns + `, outcome, payload, completed_at)
	SELECT m.id, m.type, m.priority, m.exec_mode, m.run_at, m.timeout_ms, m.max_attempts,
		m.attempts, m.owner_node, m.v_error, m.idempotency_key, m.batch_id, m.callback,
		m.parent_task_id, m.created_at, now(), 'dead', COALESCE(tp.payload, 'null'::jsonb), now()
	FROM moved m LEFT JOIN task_payloads tp ON tp.task_id = m.id
	WHERE m.attempts >= m.max_attempts::bigint
	ON CONFLICT (id) DO NOTHING
	RETURNING id
), dead_results AS (
	INSERT INTO task_results (task_id, outcome, attempt, result, error, completed_at)
	SELECT m.id, 'dead', m.attempts, NULL, m.v_error, now()
	FROM moved m
	WHERE m.attempts >= m.max_attempts::bigint
	ON CONFLICT (task_id) DO NOTHING
	RETURNING task_id
), del_payload AS (
	DELETE FROM task_payloads tp USING moved m
	WHERE tp.task_id = m.id AND m.attempts >= m.max_attempts::bigint
)
SELECT
	(SELECT count(*)::int FROM requeued) AS requeued,
	(SELECT count(*)::int FROM dead) AS dead,
	((SELECT count(*)::int FROM moved) - (SELECT count(*)::int FROM requeued) - (SELECT count(*)::int FROM dead)) AS skipped`

// Requeue 对账重置（§6.3 R1/R3）：processing → schedulable（run_at = now + 退避）；
// attempts 耗尽 → completed{dead}（payload 合并 + task_results 同事务）。
func (s *pgStore) Requeue(ctx context.Context, entries []RequeueEntry) (*RequeueReport, error) {
	if len(entries) == 0 {
		return nil, ErrEmptyBatch
	}
	report := &RequeueReport{}
	now := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // 提交后 rollback 是 no-op

	for start := 0; start < len(entries); start += finalizeChunk {
		end := min(start+finalizeChunk, len(entries))
		var requeued, dead, skipped int
		if err := s.execRequeueChunk(ctx, tx, entries[start:end], now, &requeued, &dead, &skipped); err != nil {
			return nil, err
		}
		report.Requeued += requeued
		report.Dead += dead
		report.Skipped += skipped
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit requeue: %w", err)
	}
	return report, nil
}

func (s *pgStore) execRequeueChunk(ctx context.Context, tx *sql.Tx, entries []RequeueEntry, now time.Time, requeued, dead, skipped *int) error {
	sb := newQueryBuilder()
	var values strings.Builder
	for i := range entries {
		if i > 0 {
			values.WriteString(",")
		}
		e := &entries[i]
		runAt := e.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		values.WriteString("(" + sb.arg(e.TaskID) + "::bigint, " +
			sb.arg(e.Attempt) + "::bigint, " +
			sb.arg(runAt) + "::timestamptz, " +
			sb.arg(e.Error) + "::text)")
	}
	sqlText := strings.Replace(requeueSQL, "%ROWS%", values.String(), 1)
	if err := tx.QueryRowContext(ctx, sqlText, sb.args...).Scan(requeued, dead, skipped); err != nil {
		return fmt.Errorf("store: requeue: %w", err)
	}
	return nil
}
