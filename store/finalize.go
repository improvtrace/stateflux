package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/improvtrace/stateflux/sdk"
)

// finalizeTerminalSQL 终态搬移（§5.5 终态事务的第一段）：
//   - v：结果值表（attempt 是 fencing token，只匹配 attempts 相等的 processing 行）；
//   - moved：DELETE processing RETURNING —— attempt 匹配才生效，重复搬移无副作用；
//   - ins_completed：挪入 completed，payload 从 task_payloads 合并入行（COALESCE 兜底）；
//   - ins_results：task_results 一次性写入，ON CONFLICT DO NOTHING（冲突即重复归集、跳过）；
//   - del_payload：payload 分离行删除。
//
// 返回 moved 行的派生信息（回调规格、outcome、attempt、result、error）供 Go 侧回调派生。
const finalizeTerminalSQL = `WITH v(id, attempt, outcome, result, error, completed_at) AS (
	VALUES %ROWS%
), moved AS (
	DELETE FROM processing_tasks p USING v
	WHERE p.id = v.id AND p.attempts = v.attempt
	RETURNING p.id, p.type, p.priority, p.exec_mode, p.run_at, p.timeout_ms, p.max_attempts,
		p.attempts, p.owner_node, p.idempotency_key, p.batch_id, p.callback, p.parent_task_id,
		p.created_at,
		v.outcome AS v_outcome, v.result AS v_result, v.error AS v_error, v.completed_at AS v_completed_at
), ins_completed AS (
	INSERT INTO completed_tasks (` + stageColumns + `, outcome, payload, completed_at)
	SELECT m.id, m.type, m.priority, m.exec_mode, m.run_at, m.timeout_ms, m.max_attempts,
		m.attempts, m.owner_node, m.v_error, m.idempotency_key, m.batch_id, m.callback,
		m.parent_task_id, m.created_at, m.v_completed_at,
		m.v_outcome, COALESCE(tp.payload, 'null'::jsonb), m.v_completed_at
	FROM moved m LEFT JOIN task_payloads tp ON tp.task_id = m.id
	ON CONFLICT (id) DO NOTHING
	RETURNING id
), ins_results AS (
	INSERT INTO task_results (task_id, outcome, attempt, result, error, completed_at)
	SELECT m.id, m.v_outcome, m.attempts, m.v_result, m.v_error, m.v_completed_at
	FROM moved m
	ON CONFLICT (task_id) DO NOTHING
	RETURNING task_id
), del_payload AS (
	DELETE FROM task_payloads tp USING moved m WHERE tp.task_id = m.id
)
SELECT m.id, m.callback, m.v_outcome, m.v_result, m.v_error, m.attempts
FROM moved m`

// finalizeRetrySQL 重试路由（§5.5 重试路径）：processing → schedulable（attempt 不变，
// run_at = 退避后时间；约束已通过不重评）。attempts 耗尽的行直接路由 completed{dead}
// （R3 同款，同事务完成 payload 合并 + task_results），调用方无需感知 max_attempts。
const finalizeRetrySQL = `WITH v(id, attempt, run_at, error) AS (
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
SELECT m.id FROM moved m
LEFT JOIN requeued r ON r.id = m.id
WHERE r.id IS NOT NULL`

// finalizeChunk 单条 SQL 的 VALUES 行数上限（防超长语句）。
const finalizeChunk = 500

// Finalize 终态批处理（§5.5）：整批单事务。每条 entry 按 Action 路由——
// 终态搬移（processing → completed + payload 合并 + task_results 一次性写入 + 回调派生，
// 全部同一事务 → 全框架只有一条 PG 终态写路径）或重试回 schedulable。
// attempt 匹配才生效，重复归集/陈旧结果跳过（幂等）。
func (s *pgStore) Finalize(ctx context.Context, entries []TerminalEntry) (*FinalizeReport, error) {
	if len(entries) == 0 {
		return nil, ErrEmptyBatch
	}
	report := &FinalizeReport{Applied: make(map[int64]EntryStatus, len(entries))}
	now := time.Now()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // 提交后 rollback 是 no-op

	// 路由拆分（批内去重 + chunk 防超长 SQL）。
	seen := make(map[int64]struct{}, len(entries))
	var terminal, retry []TerminalEntry
	for i := range entries {
		e := &entries[i]
		if _, dup := seen[e.TaskID]; dup {
			continue
		}
		seen[e.TaskID] = struct{}{}
		switch e.Action {
		case TerminalActionRetry:
			retry = append(retry, *e)
		default:
			if !e.Outcome.Terminal() || e.Outcome == sdk.OutcomeDead {
				return nil, fmt.Errorf("store: invalid terminal outcome %q for task %d", e.Outcome, e.TaskID)
			}
			terminal = append(terminal, *e)
		}
	}

	// 终态段：批量搬移 + 结果写入，返回回调派生所需信息。
	var cbParents []callbackParent
	for start := 0; start < len(terminal); start += finalizeChunk {
		end := min(start+finalizeChunk, len(terminal))
		parents, err := s.execTerminalMove(ctx, tx, terminal[start:end], now)
		if err != nil {
			return nil, err
		}
		cbParents = append(cbParents, parents...)
	}
	for _, e := range terminal {
		report.Applied[e.TaskID] = EntryStatusSkipped
	}
	for _, p := range cbParents {
		report.Applied[p.taskID] = EntryStatusApplied
	}

	// 重试段：批量挪回 schedulable。
	for start := 0; start < len(retry); start += finalizeChunk {
		end := min(start+finalizeChunk, len(retry))
		requeued, err := s.execRetryMove(ctx, tx, retry[start:end], now)
		if err != nil {
			return nil, err
		}
		for _, id := range requeued {
			report.Applied[id] = EntryStatusApplied
		}
	}
	for _, e := range retry {
		if _, ok := report.Applied[e.TaskID]; !ok {
			report.Applied[e.TaskID] = EntryStatusSkipped
		}
	}

	// 回调派生段（§5.8）：与父任务终态回写同一事务；幂等键 = parent:attempt:outcome，
	// 归集重拉导致的重复回写不会重复派生；执行节点无需任务创建权限。
	if len(cbParents) > 0 {
		derived, err := s.deriveCallbacks(ctx, tx, cbParents, now)
		if err != nil {
			return nil, err
		}
		report.Derived = derived
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit finalize: %w", err)
	}
	return report, nil
}

// callbackParent 回调派生的注入源（§5.8：注入源 = 本事务刚写入的 task_results 行）。
type callbackParent struct {
	taskID   int64
	attempt  int64
	outcome  sdk.Outcome
	result   []byte
	errMsg   string
	callback *sdk.CallbackSpec
}

// execTerminalMove 执行终态搬移段，返回需要做回调派生判定的 moved 行。
func (s *pgStore) execTerminalMove(ctx context.Context, tx *sql.Tx, entries []TerminalEntry, now time.Time) ([]callbackParent, error) {
	sb := newQueryBuilder()
	values := buildTerminalValues(entries, sb)
	sqlText := strings.Replace(finalizeTerminalSQL, "%ROWS%", values, 1)
	rows, err := tx.QueryContext(ctx, sqlText, sb.args...)
	if err != nil {
		return nil, fmt.Errorf("store: terminal move: %w", err)
	}
	defer rows.Close()
	var parents []callbackParent
	for rows.Next() {
		var id, attempt int64
		var outcome string
		var callback, result []byte
		var errMsg string
		if err := rows.Scan(&id, &callback, &outcome, &result, &errMsg, &attempt); err != nil {
			return nil, fmt.Errorf("store: scan moved: %w", err)
		}
		p := callbackParent{
			taskID:  id,
			attempt: attempt,
			outcome: sdk.Outcome(outcome),
			result:  result,
			errMsg:  errMsg,
		}
		if len(callback) > 0 {
			if spec, err := sdk.UnmarshalCallback(callback); err == nil {
				p.callback = spec
			}
		}
		parents = append(parents, p)
	}
	return parents, rows.Err()
}

// execRetryMove 执行重试段，返回成功挪回 schedulable 的 task_id（attempts 耗尽的行
// 由 SQL 直接路由 dead，不返回）。
func (s *pgStore) execRetryMove(ctx context.Context, tx *sql.Tx, entries []TerminalEntry, now time.Time) ([]int64, error) {
	sb := newQueryBuilder()
	var values strings.Builder
	for i := range entries {
		if i > 0 {
			values.WriteString(",")
		}
		runAt := entries[i].RetryRunAt
		if runAt.IsZero() {
			runAt = now
		}
		values.WriteString("(" + sb.arg(entries[i].TaskID) + "::bigint, " +
			sb.arg(entries[i].Attempt) + "::bigint, " +
			sb.arg(runAt) + "::timestamptz, " +
			sb.arg(entries[i].Error) + "::text)")
	}
	sqlText := strings.Replace(finalizeRetrySQL, "%ROWS%", values.String(), 1)
	rows, err := tx.QueryContext(ctx, sqlText, sb.args...)
	if err != nil {
		return nil, fmt.Errorf("store: retry move: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan requeued: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// buildTerminalValues 拼接终态 VALUES 行：(id::bigint, attempt::bigint, outcome::text,
// result::jsonb, error::text, completed_at::timestamptz), ...
func buildTerminalValues(entries []TerminalEntry, sb *queryBuilder) string {
	var values strings.Builder
	for i := range entries {
		if i > 0 {
			values.WriteString(",")
		}
		e := &entries[i]
		completedAt := e.CompletedAt
		if completedAt.IsZero() {
			completedAt = time.Now()
		}
		values.WriteString("(" + sb.arg(e.TaskID) + "::bigint, " +
			sb.arg(e.Attempt) + "::bigint, " +
			sb.arg(string(e.Outcome)) + "::text, " +
			sb.cast(jsonBytes(normalizeJSONB(e.Result)), "jsonb") + ", " +
			sb.arg(e.Error) + "::text, " +
			sb.arg(completedAt) + "::timestamptz)")
	}
	return values.String()
}

// deriveCallbacks 回调派生（§5.8）：OnSuccess 将父任务结果注入回调任务参数；
// OnError 将错误信息注入回调任务参数。回调任务幂等键 = parent:attempt:outcome。
func (s *pgStore) deriveCallbacks(ctx context.Context, tx *sql.Tx, parents []callbackParent, now time.Time) ([]int64, error) {
	var rows []pendingRow
	var tasks []sdk.NewTask
	for i := range parents {
		p := &parents[i]
		spec := p.callback
		if spec == nil {
			continue
		}
		var child *sdk.CallbackSpec
		if p.outcome == sdk.OutcomeSucceeded {
			child = spec.OnSuccess
		} else {
			child = spec.OnError
		}
		if child == nil {
			continue
		}
		nt := sdk.NewTask{
			Type:           child.Type,
			Payload:        injectCallbackParams(child.PayloadTemplate, p),
			ExecMode:       sdk.ExecAsync, // 派生任务默认异步投递（结果无需及时回执）
			RunAt:          now,
			IdempotencyKey: fmt.Sprintf("%d:%d:%s", p.taskID, p.attempt, p.outcome),
			ParentTaskID:   p.taskID,
			Callback:       child, // 嵌套规格覆盖链式场景（深度上限在创建时校验）
		}
		id, err := s.sf.Next()
		if err != nil {
			return nil, fmt.Errorf("store: generate derived task id: %w", err)
		}
		rows = append(rows, pendingRow{idx: len(tasks), id: id, payload: nt.Payload})
		tasks = append(tasks, nt)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if err := insertPendingRows(ctx, tx, rows, tasks, now); err != nil {
		return nil, err
	}
	if err := insertPayloadRows(ctx, tx, rows); err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.id)
	}
	return ids, nil
}

// injectCallbackParams 参数注入（§5.8，照搬 Machinery 核心）：
//   - OnSuccess：模板 JSON 对象注入 "parent_result" 字段（父任务 result 解析为 JSON，
//     解析失败则保留原字符串）；模板非对象时以 {"parent_result": ...} 整体替换。
//   - OnError：同理注入 "parent_error" 字段（错误摘要字符串）。
func injectCallbackParams(template []byte, p *callbackParent) []byte {
	obj := map[string]any{}
	if len(template) > 0 && json.Valid(template) {
		_ = json.Unmarshal(template, &obj)
		if obj == nil {
			obj = map[string]any{}
		}
	}
	if p.outcome == sdk.OutcomeSucceeded {
		var parsed any
		if len(p.result) > 0 && json.Unmarshal(p.result, &parsed) == nil {
			obj["parent_result"] = parsed
		} else {
			obj["parent_result"] = string(p.result)
		}
	} else {
		obj["parent_error"] = p.errMsg
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return template
	}
	return out
}
