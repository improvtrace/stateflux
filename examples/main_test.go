//go:build integration

// 场景测试入口：需要 PostgreSQL 与 Redis（部署见 maintain.sh，环境变量见
// README.md），运行：
//
//	go test -tags=integration -v ./examples/
package examples_test

import (
	"testing"

	"github.com/improvtrace/stateflux/examples"
	"github.com/improvtrace/stateflux/examples/scenarios/failure"
	"github.com/improvtrace/stateflux/examples/scenarios/forward"
	"github.com/improvtrace/stateflux/examples/scenarios/smoke"
	"github.com/improvtrace/stateflux/examples/scenarios/startup_recovery"
	"github.com/improvtrace/stateflux/examples/scenarios/timer"
)

// Test 场景清单，顺序执行；每 case 前 Reset 清场（TRUNCATE + FLUSHDB），
// 可用 -run 'Test/<场景名>' 单跑。新增 case：在 scenarios/ 下建子包实现
// Test(h, t)，并登记到本清单。
func Test(t *testing.T) {
	h := examples.Setup(t)
	for _, s := range []*examples.Scenario{
		{Name: "冒烟-Run生命周期", Test: smoke.Test},
		{Name: "定时触发", Test: timer.Test},
		{Name: "失败重试与终态收敛", Test: failure.Test},
		{Name: "启动恢复-重派未完成行", Test: startup_recovery.Test},
		{Name: "持久化转发-leader落表并重发", Test: forward.Test},
	} {
		if s.Disable {
			continue
		}
		t.Run(s.Name, func(t *testing.T) {
			h.Reset(t)
			s.Test(h, t)
		})
	}
}
