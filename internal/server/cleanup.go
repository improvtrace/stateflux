package server

import (
	"log"
	"time"
)

// DefaultCleanupTimeout 是单个装配资源释放步骤的等待上限。
const DefaultCleanupTimeout = 5 * time.Second

// boundedCleanup 包装一个资源释放函数：命名、限时、记录耗时。
//
// wire 生成的清理链按构造逆序执行各 provider 返回的释放函数；若某个资源（DB 连接池、
// 监听连接等）的 Close 因底层阻塞而长时间不返回，会拖住整个进程退出。这里给每一步
// 独立的等待上限：超时即放弃该步并告警，继续释放后续资源，由进程退出回收。
func boundedCleanup(name string, timeout time.Duration, fn func()) func() {
	if fn == nil {
		return func() {}
	}
	if timeout <= 0 || timeout > DefaultCleanupTimeout {
		// 每步不超过 DefaultCleanupTimeout：个别资源慢不该挤占后续资源的释放时间；
		// 整体预算由调用方（App / main）另行约束。
		timeout = DefaultCleanupTimeout
	}
	return func() {
		log.Printf("server: cleanup %s started", name)
		start := time.Now()
		done := make(chan struct{})
		go func() {
			defer close(done)
			fn()
		}()
		select {
		case <-done:
			log.Printf("server: cleanup %s done in %s", name, time.Since(start).Round(time.Millisecond))
		case <-time.After(timeout):
			log.Printf("server: cleanup %s exceeded %s; abandoning and continuing shutdown", name, timeout)
		}
	}
}
