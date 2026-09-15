package worker

import (
	"context"
	"sort"

	"github.com/improvtrace/stateflux/internal/domain/cacheview"
)

// QueueSource 把 coherence 物化的「队列 → 节点」映射适配为 worker.QueueSource（§15.1#4）：
// 执行节点只消费分配给自己、且出现在视图中的队列。视图为空（尚未收到快照）时回落到静态
// 配置，保证单机与启动早期仍能消费；视图不可用时返回错误，worker 保留既有订阅（§6.1）。
type QueueSource struct {
	view     cacheview.View
	fallback []string
}

// NewQueueSource 构造队列来源适配器。
func NewQueueSource(view cacheview.View, fallback []string) *QueueSource {
	return &QueueSource{view: view, fallback: append([]string(nil), fallback...)}
}

// AssignedQueues 实现 worker.QueueSource。
func (s *QueueSource) AssignedQueues(ctx context.Context, nodeID string) ([]string, error) {
	if s.view == nil {
		return s.fallback, nil
	}
	routes, err := s.view.QueueRoutes(ctx)
	if err != nil {
		return nil, err
	}
	if len(routes) == 0 {
		return s.fallback, nil
	}
	out := make([]string, 0, len(routes))
	for queue, node := range routes {
		if node == nodeID {
			out = append(out, queue)
		}
	}
	sort.Strings(out)
	return out, nil
}
