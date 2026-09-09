package sdk

import (
	"errors"
	"math/rand/v2"
	"time"
)

// NonRetryableError 业务标记的不可重试错误（§5.5）：handler 返回被 errors.As 识别为
// NonRetryableError 的错误时，任务终态为 failed，不进入重试路径（不再消耗重试预算）。
type NonRetryableError struct {
	Err error
}

// Error 实现 error。
func (e *NonRetryableError) Error() string {
	if e.Err == nil {
		return "non-retryable"
	}
	return e.Err.Error()
}

// Unwrap 支持errors.Is / errors.As 链。
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable 把任意错误包装为不可重试错误。
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		return err
	}
	return &NonRetryableError{Err: err}
}

// IsNonRetryable 判断错误链上是否带不可重试标记。
func IsNonRetryable(err error) bool {
	var nre *NonRetryableError
	return errors.As(err, &nre)
}

// Backoff 重试退避（§6.3 R1）：capped exponential + full jitter（AWS 模式，参照
// client-go workqueue）。默认初值 500ms、上限 1000s；attempt 为当前已执行次数（从 1 起）。
// 无 jitter 的重试会在故障恢复后形成同步重试风暴，正好打在刚恢复的调度节点上。
func Backoff(attempt int64) time.Duration {
	const (
		base = 500 * time.Millisecond
		cap  = 1000 * time.Second
	)
	if attempt < 1 {
		attempt = 1
	}
	// base * 2^(attempt-1)，封顶 cap；位移防溢出。
	d := base
	for i := int64(1); i < attempt; i++ {
		d *= 2
		if d >= cap {
			d = cap
			break
		}
	}
	if d > cap {
		d = cap
	}
	jitter := rand.Int64N(int64(d) + 1)
	return time.Duration(jitter)
}
