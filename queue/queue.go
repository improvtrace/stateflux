// Package queue 承载 Redis 视图（§3.2）：异步就绪队列（LIST 按 priority 三级拆分）、
// inprocess 共识集合（ZSET + HASH，原子操作 + 归属校验）、终态墓碑、执行节点容量上报。
//
// Redis 是可重建的实时视图，key 全部带 TTL（§1.2.1）；inprocess 集合的成员语义 =
// 已被认领、但尚未在 PG 落终态的任务（执行中 + 执行完但结果未归集，§14.2）。
// 集合操作全部原子且带归属校验：注册检查墓碑与已有成员（重复投递/陈旧副本被拒绝），
// 续约与移除要求 node_id + attempt 匹配——僵尸节点无法覆盖新尝试的记录。
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/improvtrace/stateflux/sdk"
)

// Redis key 布局（§3.2）。
const (
	QueuePrefix     = "stateflux:queue:"           // + {pri} → LIST
	InprocessKey    = "stateflux:inprocess"        // ZSET：member=task_id，score=租约到期(ms)
	InprocessDetail = "stateflux:inprocess:detail" // HASH：member → {node_id, attempt, started_at}
	TombstonePrefix = "stateflux:done:"            // + task_id → STRING 终态墓碑
	NodePrefix      = "stateflux:node:"            // + node_id → HASH 容量上报
)

// QueueKey 就绪队列 key。
func QueueKey(p sdk.Priority) string { return QueuePrefix + string(p.Normalize()) }

// TombstoneKey 终态墓碑 key。
func TombstoneKey(taskID int64) string { return TombstonePrefix + fmt.Sprint(taskID) }

// NodeKey 节点容量上报 key。
func NodeKey(nodeID string) string { return NodePrefix + nodeID }

// QueuePriorities BRPOP 消费的优先级顺序：high → normal → low（§5.3）。
var QueuePriorities = []sdk.Priority{sdk.PriorityHigh, sdk.PriorityNormal, sdk.PriorityLow}

// RejectKind 注册拒绝原因（§6.2 双防线）。
type RejectKind string

const (
	RejectTombstone RejectKind = "tombstone" // 已终态后旧副本才被消费
	RejectAttempt   RejectKind = "attempt"   // 同一/更新 attempt 已注册（陈旧副本）
)

// Member inprocess 详情（detail HASH 的 JSON 值）。
type Member struct {
	NodeID    string `json:"node_id"`
	Attempt   int64  `json:"attempt"`
	StartedAt int64  `json:"started_at"` // unix ms
}

// registerScript 注册（§3.2 共识语义核心）：
//   - 墓碑命中 → 拒绝（终态后旧副本才被消费）；
//   - 同一 attempt 已注册 → 拒绝（重复投递）；更新的 attempt 已注册 → 拒绝（陈旧副本）；
//   - 更小 attempt 已注册 → 新尝试接管（旧执行者已死，对账重置后重新认领）。
var registerScript = redis.NewScript(`
local tombstone = redis.call('EXISTS', KEYS[3])
if tombstone == 1 then
	return {0, 'tombstone'}
end
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
	local ok, e = pcall(cjson.decode, existing)
	if ok and e.attempt ~= nil then
		local old = tonumber(e.attempt)
		local new = tonumber(ARGV[3])
		if old >= new then
			return {0, 'attempt'}
		end
	end
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[5]), ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[6])
return {1, 'ok'}
`)

// renewScript 续约：node_id + attempt 归属校验，通过则推进租约到期分值。
var renewScript = redis.NewScript(`
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if not existing then return 0 end
local ok, e = pcall(cjson.decode, existing)
if not ok or not e then return 0 end
if tostring(e.node_id) ~= tostring(ARGV[2]) or tonumber(e.attempt) ~= tonumber(ARGV[3]) then
	return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[5]), ARGV[1])
return 1
`)

// removeScript 移除（Ack 后移出 inprocess，§5.5）：要求归属匹配，
// 僵尸节点的归集后移除被拒（不能移除新尝试的记录）。
var removeScript = redis.NewScript(`
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if not existing then return 0 end
local ok, e = pcall(cjson.decode, existing)
if not ok or not e then
	redis.call('ZREM', KEYS[1], ARGV[1])
	redis.call('HDEL', KEYS[2], ARGV[1])
	return 1
end
if tostring(e.node_id) ~= tostring(ARGV[2]) or tonumber(e.attempt) ~= tonumber(ARGV[3]) then
	return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`)

// forceRemoveScript 强制移除（R2 幽灵清理，§6.3）：不校验归属——PG 已终态/不存在时移除必然正确。
var forceRemoveScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`)

// Queue Redis 视图操作入口。并发安全（go-redis 连接池）。
type Queue struct {
	rdb redis.UniversalClient

	// 脚本 SHA 预加载：pipeline 模式下 go-redis 不做 NOSCRIPT→EVAL 回退，
	// 首次使用前 EVAL LOAD 保证 EVALSHA 命中（构造时 Redis 未就绪则懒重试）。
	mu     sync.Mutex
	loaded bool
}

// 脚本清单（预加载用）。
var allScripts = []*redis.Script{registerScript, renewScript, removeScript, forceRemoveScript}

// New 构造 Queue。
func New(rdb redis.UniversalClient) *Queue { return &Queue{rdb: rdb} }

// ensureScripts 预加载 Lua 脚本（幂等；失败后下次调用重试）。
func (q *Queue) ensureScripts(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.loaded {
		return nil
	}
	for _, s := range allScripts {
		if err := s.Load(ctx, q.rdb).Err(); err != nil {
			return fmt.Errorf("queue: load script: %w", err)
		}
	}
	q.loaded = true
	return nil
}

// Client 暴露底层客户端（executor 消费循环 BRPOP 直用）。
func (q *Queue) Client() redis.UniversalClient { return q.rdb }

// RegisterResult 注册结果。
type RegisterResult struct {
	OK       bool
	RejectOf RejectKind // 拒绝原因（OK == false 时有效）
}

// Register 注册 inprocess 成员（执行前，§5.4）。lease 为租约时长（score = now + lease）。
// 被拒绝则消息是陈旧副本或他人持有——无害丢弃（§6.2）。
func (q *Queue) Register(ctx context.Context, taskID, attempt int64, nodeID string, lease time.Duration) (RegisterResult, error) {
	if err := q.ensureScripts(ctx); err != nil {
		return RegisterResult{}, err
	}
	now := time.Now().UnixMilli()
	detail, err := json.Marshal(Member{NodeID: nodeID, Attempt: attempt, StartedAt: now})
	if err != nil {
		return RegisterResult{}, fmt.Errorf("queue: marshal member: %w", err)
	}
	res, err := registerScript.Run(ctx, q.rdb,
		[]string{InprocessKey, InprocessDetail, TombstoneKey(taskID)},
		fmt.Sprint(taskID), nodeID, fmt.Sprint(attempt),
		fmt.Sprint(now), fmt.Sprint(lease.Milliseconds()), string(detail),
	).Slice()
	if err != nil {
		return RegisterResult{}, fmt.Errorf("queue: register: %w", err)
	}
	ok, _ := res[0].(int64)
	kind, _ := res[1].(string)
	return RegisterResult{OK: ok == 1, RejectOf: RejectKind(kind)}, nil
}

// Renew 续约（执行中周期续租，§5.4）。归属不匹配（僵尸节点）返回 false。
func (q *Queue) Renew(ctx context.Context, taskID, attempt int64, nodeID string, lease time.Duration) (bool, error) {
	if err := q.ensureScripts(ctx); err != nil {
		return false, err
	}
	now := time.Now().UnixMilli()
	n, err := renewScript.Run(ctx, q.rdb,
		[]string{InprocessKey, InprocessDetail},
		fmt.Sprint(taskID), nodeID, fmt.Sprint(attempt),
		fmt.Sprint(now), fmt.Sprint(lease.Milliseconds()),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("queue: renew: %w", err)
	}
	return n == 1, nil
}

// Remove 移出 inprocess（归集 Ack 后，§5.5）。归属不匹配返回 false（僵尸节点被拒）。
func (q *Queue) Remove(ctx context.Context, taskID, attempt int64, nodeID string) (bool, error) {
	if err := q.ensureScripts(ctx); err != nil {
		return false, err
	}
	n, err := removeScript.Run(ctx, q.rdb,
		[]string{InprocessKey, InprocessDetail},
		fmt.Sprint(taskID), nodeID, fmt.Sprint(attempt),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("queue: remove: %w", err)
	}
	return n == 1, nil
}

// ForceRemove 强制移除（R2 幽灵清理）。批量为多 task_id 执行。
func (q *Queue) ForceRemove(ctx context.Context, taskIDs []int64) error {
	if len(taskIDs) == 0 {
		return nil
	}
	if err := q.ensureScripts(ctx); err != nil {
		return err
	}
	pipe := q.rdb.Pipeline()
	for _, id := range taskIDs {
		forceRemoveScript.Run(ctx, pipe,
			[]string{InprocessKey, InprocessDetail},
			fmt.Sprint(id),
		)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("queue: force remove: %w", err)
	}
	return nil
}

// SetTombstones 终态墓碑（§6.2）：TTL ≥ grace（默认 90s），自动回收。
// TTL 覆盖 grace 而非仅对账周期——旧副本在墓碑过期后才被消费的双防线失效窗口被堵住。
func (q *Queue) SetTombstones(ctx context.Context, taskIDs []int64, ttl time.Duration) error {
	if len(taskIDs) == 0 {
		return nil
	}
	pipe := q.rdb.Pipeline()
	for _, id := range taskIDs {
		pipe.Set(ctx, TombstoneKey(id), "1", ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("queue: set tombstones: %w", err)
	}
	return nil
}

// HasTombstone 查询墓碑（测试与排障用）。
func (q *Queue) HasTombstone(ctx context.Context, taskID int64) (bool, error) {
	n, err := q.rdb.Exists(ctx, TombstoneKey(taskID)).Result()
	if err != nil {
		return false, fmt.Errorf("queue: has tombstone: %w", err)
	}
	return n == 1, nil
}

// Push 异步投递（调度侧，§5.3）：pipeline LPUSH queue:{pri}，消息体 = proto TaskMessage。
func (q *Queue) Push(ctx context.Context, messages map[sdk.Priority][][]byte) error {
	if len(messages) == 0 {
		return nil
	}
	pipe := q.rdb.Pipeline()
	for pri, msgs := range messages {
		if len(msgs) == 0 {
			continue
		}
		key := QueueKey(pri)
		args := make([]any, 0, len(msgs))
		for _, m := range msgs {
			args = append(args, m)
		}
		pipe.LPush(ctx, key, args...)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("queue: push: %w", err)
	}
	return nil
}

// Depths 各优先级队列深度（LLEN，§6.4 反压与观测）。
func (q *Queue) Depths(ctx context.Context) (map[sdk.Priority]int64, error) {
	pipe := q.rdb.Pipeline()
	cmds := make(map[sdk.Priority]*redis.IntCmd, len(QueuePriorities))
	for _, pri := range QueuePriorities {
		cmds[pri] = pipe.LLen(ctx, QueueKey(pri))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("queue: depths: %w", err)
	}
	out := make(map[sdk.Priority]int64, len(cmds))
	for pri, cmd := range cmds {
		out[pri] = cmd.Val()
	}
	return out, nil
}

// InprocessSize 共识集合大小（ZCARD，观测 §6.5）。
func (q *Queue) InprocessSize(ctx context.Context) (int64, error) {
	n, err := q.rdb.ZCard(ctx, InprocessKey).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: inprocess size: %w", err)
	}
	return n, nil
}

// InprocessEntry 集合条目（R2 幽灵清理与观测用）。
type InprocessEntry struct {
	TaskID  int64
	LeaseAt time.Time // score = 租约到期时间
	Member  Member
}

// ListInprocess 列出集合条目（可选过滤：仅未过期）。
func (q *Queue) ListInprocess(ctx context.Context) ([]InprocessEntry, error) {
	pairs, err := q.rdb.ZRangeWithScores(ctx, InprocessKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: list inprocess: %w", err)
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(pairs))
	for _, p := range pairs {
		ids = append(ids, p.Member.(string))
	}
	details, err := q.rdb.HMGet(ctx, InprocessDetail, ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: list inprocess details: %w", err)
	}
	out := make([]InprocessEntry, 0, len(pairs))
	for i, p := range pairs {
		var taskID int64
		if _, err := fmt.Sscanf(p.Member.(string), "%d", &taskID); err != nil {
			continue
		}
		e := InprocessEntry{
			TaskID:  taskID,
			LeaseAt: time.UnixMilli(int64(p.Score)),
		}
		if i < len(details) && details[i] != nil {
			if raw, ok := details[i].(string); ok {
				var m Member
				if json.Unmarshal([]byte(raw), &m) == nil {
					e.Member = m
				}
			}
		}
		out = append(out, e)
	}
	return out, nil
}

// ReportCapacity 执行节点容量上报（§3.2 stateflux:node:{node_id}，free_slots）。
// 整 key TTL 防节点死亡后残留；key 本身是易失实时视图，丢失由下次上报恢复。
func (q *Queue) ReportCapacity(ctx context.Context, nodeID string, freeSlots int64, ttl time.Duration) error {
	key := NodeKey(nodeID)
	pipe := q.rdb.Pipeline()
	pipe.HSet(ctx, key, "free_slots", freeSlots, "updated_at", time.Now().UnixMilli())
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("queue: report capacity: %w", err)
	}
	return nil
}

// FreeSlots 查询节点剩余容量（同步任务选节点用，§5.3）。
func (q *Queue) FreeSlots(ctx context.Context, nodeID string) (int64, bool, error) {
	v, err := q.rdb.HGet(ctx, NodeKey(nodeID), "free_slots").Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("queue: free slots: %w", err)
	}
	var n int64
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return 0, false, nil
	}
	return n, true, nil
}

// ClearAll 清空本框架全部 key（测试与 R4 重建前用）。
func (q *Queue) ClearAll(ctx context.Context) error {
	keys := []string{InprocessKey, InprocessDetail}
	for _, pri := range QueuePriorities {
		keys = append(keys, QueueKey(pri))
	}
	if err := q.rdb.Del(ctx, keys...).Err(); err != nil && err != redis.Nil {
		return fmt.Errorf("queue: clear: %w", err)
	}
	for _, pattern := range []string{TombstonePrefix + "*", NodePrefix + "*"} {
		iter := q.rdb.Scan(ctx, 0, pattern, 100).Iterator()
		batch := make([]string, 0, 100)
		for iter.Next(ctx) {
			batch = append(batch, iter.Val())
			if len(batch) >= 100 {
				if err := q.rdb.Del(ctx, batch...).Err(); err != nil {
					return fmt.Errorf("queue: clear %s: %w", pattern, err)
				}
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			if err := q.rdb.Del(ctx, batch...).Err(); err != nil {
				return fmt.Errorf("queue: clear %s: %w", pattern, err)
			}
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("queue: scan %s: %w", pattern, err)
		}
	}
	return nil
}
