package redis

import (
	"context"
	"sync"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// subscription 是 Redis 各形态共用的订阅句柄：Close 幂等，Close 后等待回调循环退出。
type subscription struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func newSubscription(cancel context.CancelFunc, done chan struct{}) *subscription {
	return &subscription{cancel: cancel, done: done}
}

// Close 实现 channel.Subscription。
func (s *subscription) Close() error {
	s.once.Do(func() {
		s.cancel()
		<-s.done
	})
	return nil
}

var _ channel.Subscription = (*subscription)(nil)
