package store

import (
	"context"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/sdk"
)

// claimSQL 认领挪行（§9.1 关键设计决策：查询与迁移合并为单条 SQL）：
//   - claimed：按优先级 + run_at 选批，FOR UPDATE SKIP LOCKED 防重复认领（并发调度安全），
//     DELETE RETURNING 同时完成「查」与「挪出」；
//   - ins：INSERT processing，attempts +1（fencing token，§14.3），owner_node 记调度节点；
//   - 最终 JOIN task_payloads 内联返回（构建 TaskMessage 用，执行侧零 PG 读）。
const claimSQL = `WITH claimed AS (
	DELETE FROM schedulable_tasks
	WHERE id IN (
		SELECT id FROM schedulable_tasks
		WHERE run_at <= $1::timestamptz
		ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END,
			run_at, id
		LIMIT $2::int
		FOR UPDATE SKIP LOCKED
	)
	RETURNING ` + stageColumns + `
), ins AS (
	INSERT INTO processing_tasks (` + stageColumnsNoUpdated + `, updated_at)
	SELECT c.id, c.type, c.priority, c.exec_mode, c.run_at, c.timeout_ms, c.max_attempts,
		c.attempts + 1, $3::text, '', c.idempotency_key, c.batch_id, c.callback,
		c.parent_task_id, c.created_at, $1::timestamptz
	FROM claimed c
	RETURNING ` + stageColumnsNoUpdated + `, updated_at
)
SELECT i.id, i.type, i.priority, i.exec_mode, i.run_at, i.timeout_ms, i.max_attempts, i.attempts,
	i.owner_node, i.error, i.idempotency_key, i.batch_id, i.callback, i.parent_task_id,
	i.created_at, i.updated_at, pl.payload
FROM ins i
LEFT JOIN task_payloads pl ON pl.task_id = i.id
ORDER BY CASE i.priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END, i.run_at, i.id`

// 注：终 SELECT 读 ins RETURNING 而非 processing_tasks——PG 数据修改型 CTE 的主查询与
// 子语句共享同一快照，重读目标表看不到本语句插入的行；task_payloads 未被本语句修改，
// 快照内可见，join 安全。

// Claim 认领挪行（§5.3）：schedulable → processing，查询与迁移原子完成，天然防重复认领。
// 返回的任务 attempts 已 +1，owner_node = 调度节点；任务内联 payload。
func (s *pgStore) Claim(ctx context.Context, nodeID string, limit int) ([]*sdk.Task, error) {
	if limit <= 0 {
		limit = 1
	}
	if nodeID == "" {
		nodeID = "unknown-scheduler"
	}
	now := time.Now()
	rows, err := s.db.QueryContext(ctx, claimSQL, now, limit, nodeID)
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	defer rows.Close()
	var out []*sdk.Task
	for rows.Next() {
		t := &sdk.Task{}
		var callback, payload []byte
		var runAt, createdAt, updatedAt time.Time
		if err := rows.Scan(
			&t.ID, &t.Type, &t.Priority, &t.ExecMode, &runAt, &t.TimeoutMS, &t.MaxAttempts,
			&t.Attempts, &t.OwnerNode, &t.Error, &t.IdempotencyKey, &t.BatchID,
			&callback, &t.ParentTaskID, &createdAt, &updatedAt, &payload,
		); err != nil {
			return nil, fmt.Errorf("store: scan claimed: %w", err)
		}
		t.RunAt, t.CreatedAt, t.UpdatedAt = runAt, createdAt, updatedAt
		t.Priority = sdk.Priority(t.Priority)
		t.ExecMode = sdk.ExecMode(t.ExecMode)
		if len(callback) > 0 {
			if spec, err := sdk.UnmarshalCallback(callback); err == nil {
				t.Callback = spec
			}
		}
		t.Payload = payload
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate claimed: %w", err)
	}
	return out, nil
}
