package cacheview

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisView 是 View 的 Redis 实现（§15.1#9）：所有键带前缀，使用 {hashtag} 保证
// 多键操作（Purge 的 SCAN、路由快照）在 Redis Cluster 下尽量同槽。
type RedisView struct {
	client redis.UniversalClient
	opts   Options
}

// NewRedis 构造 Redis 视图；client 为 nil 时返回错误（装配期尽早失败）。
func NewRedis(client redis.UniversalClient, opts Options) (*RedisView, error) {
	if client == nil {
		return nil, errors.New("cacheview: nil redis client")
	}
	return &RedisView{client: client, opts: opts.withDefaults()}, nil
}

func (v *RedisView) taskKey(id int64) string {
	return v.opts.Prefix + "{task}:" + strconv.FormatInt(id, 10)
}
func (v *RedisView) dedupeKey(key string) string { return v.opts.Prefix + "{dedupe}:" + key }
func (v *RedisView) inflightKey(node string) string {
	return v.opts.Prefix + "{inflight}:" + node
}
func (v *RedisView) routeKey(queue string) string { return v.opts.Prefix + "{route}:" + queue }

// SetTaskState 写入任务视图（HSET + EXPIRE，单管道）。
func (v *RedisView) SetTaskState(ctx context.Context, st TaskState) error {
	if st.UpdatedAt.IsZero() {
		st.UpdatedAt = time.Now()
	}
	key := v.taskKey(st.TaskID)
	pipe := v.client.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"task_id":    st.TaskID,
		"attempt":    st.Attempt,
		"state":      string(st.State),
		"node_id":    st.NodeID,
		"queue":      st.Queue,
		"error":      st.Error,
		"updated_at": st.UpdatedAt.UnixMilli(),
	})
	pipe.Expire(ctx, key, v.opts.TaskTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// GetTaskState 读取任务视图。
func (v *RedisView) GetTaskState(ctx context.Context, taskID int64) (TaskState, bool, error) {
	res, err := v.client.HGetAll(ctx, v.taskKey(taskID)).Result()
	if err != nil {
		return TaskState{}, false, err
	}
	if len(res) == 0 {
		return TaskState{}, false, nil
	}
	st := TaskState{
		TaskID:  parseI64(res["task_id"], taskID),
		Attempt: parseI64(res["attempt"], 0),
		State:   State(res["state"]),
		NodeID:  res["node_id"],
		Queue:   res["queue"],
		Error:   res["error"],
	}
	if ms := parseI64(res["updated_at"], 0); ms > 0 {
		st.UpdatedAt = time.UnixMilli(ms)
	}
	return st, true, nil
}

// DeleteTaskState 删除任务视图。
func (v *RedisView) DeleteTaskState(ctx context.Context, taskID int64) error {
	return v.client.Del(ctx, v.taskKey(taskID)).Err()
}

// ClaimDedupe 用 SET NX EX 实现入口去重：写入成功（首次）返回 true。
func (v *RedisView) ClaimDedupe(ctx context.Context, key string) (bool, error) {
	return v.client.SetNX(ctx, v.dedupeKey(key), time.Now().UnixMilli(), v.opts.DedupeTTL).Result()
}

// ReleaseDedupe 删除去重标记。
func (v *RedisView) ReleaseDedupe(ctx context.Context, key string) error {
	return v.client.Del(ctx, v.dedupeKey(key)).Err()
}

// IncrInflight 在途计数 +1。
func (v *RedisView) IncrInflight(ctx context.Context, nodeID string) (int64, error) {
	key := v.inflightKey(nodeID)
	pipe := v.client.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, v.opts.TaskTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

// DecrInflight 在途计数 -1（不落负值）。
func (v *RedisView) DecrInflight(ctx context.Context, nodeID string) (int64, error) {
	n, err := v.client.Decr(ctx, v.inflightKey(nodeID)).Result()
	if err != nil {
		return 0, err
	}
	if n < 0 {
		_ = v.client.Set(ctx, v.inflightKey(nodeID), 0, v.opts.TaskTTL).Err()
		return 0, nil
	}
	return n, nil
}

// Inflight 读取在途计数。
func (v *RedisView) Inflight(ctx context.Context, nodeID string) (int64, error) {
	n, err := v.client.Get(ctx, v.inflightKey(nodeID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}

// SetQueueRoute 写入队列归属。
func (v *RedisView) SetQueueRoute(ctx context.Context, queue, nodeID string) error {
	return v.client.Set(ctx, v.routeKey(queue), nodeID, v.opts.RouteTTL).Err()
}

// GetQueueRoute 读取队列归属。
func (v *RedisView) GetQueueRoute(ctx context.Context, queue string) (string, bool, error) {
	node, err := v.client.Get(ctx, v.routeKey(queue)).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return node, true, nil
}

// QueueRoutes 扫描全部队列路由（best-effort：SCAN 期间的新增可能缺失）。
func (v *RedisView) QueueRoutes(ctx context.Context) (map[string]string, error) {
	pattern := v.opts.Prefix + "{route}:*"
	out := map[string]string{}
	iter := v.client.Scan(ctx, 0, pattern, 256).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		node, err := v.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		out[key[len(v.opts.Prefix+"{route}:"):]] = node
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Purge 删除前缀下全部视图（R2）：SCAN + 分批 DEL，避免 KEYS 阻塞。
func (v *RedisView) Purge(ctx context.Context, prefix string) (int64, error) {
	if prefix == "" {
		prefix = v.opts.Prefix
	}
	var deleted int64
	iter := v.client.Scan(ctx, 0, prefix+"*", 512).Iterator()
	batch := make([]string, 0, 512)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := v.client.Del(ctx, batch...).Result()
		deleted += n
		batch = batch[:0]
		return err
	}
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		if len(batch) >= 512 {
			if err := flush(); err != nil {
				return deleted, err
			}
		}
	}
	if err := iter.Err(); err != nil {
		return deleted, err
	}
	if err := flush(); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func parseI64(s string, fallback int64) int64 {
	if s == "" {
		return fallback
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}
