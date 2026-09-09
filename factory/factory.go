// Package factory 实现 TaskFactory（§5.7）：period/cron 是任务的生成逻辑，不引入新数据
// 模型——按计划周期性生成一次性任务实例，走「创建 → 调度 → 执行 → 归集」既有全链路。
// 语义采用 Vault 轮转器的生产验证范式：
//
//   - next = last_success + period 现算（cron 模式为 schedule.Next(last_success)），
//     不持久化绝对时间点；
//   - 错过窗口即跳过、不补跑：生成的是「周期性意图」而非积压债务；
//   - 重试超限的工厂条目冻结为孤儿并显式告警，停止自动生成、等待人工修复后重新注册；
//   - 运行位置与调度节点同进程（单调度者原则）；故障切换后新调度节点从 PG 重建生成状态。
//
// 幂等生成：实例幂等键 = factory:{name}:{next.Unix()}——进程重启/故障切换后重算的 next
// 与已生成实例命中同一幂等键，store 创建去重，不重复生成。
package factory

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/improvtrace/stateflux/config"
	"github.com/improvtrace/stateflux/obs"
	"github.com/improvtrace/stateflux/sdk"
	"github.com/improvtrace/stateflux/store"
)

// Factory 周期任务生成器（仅调度节点运行，§2.1/§5.7）。
type Factory struct {
	store   store.Store
	cfg     config.FactoryConfig
	metrics *obs.Metrics
	log     *slog.Logger

	entries []compiledEntry
}

type compiledEntry struct {
	def      config.FactoryEntry
	schedule cron.Schedule // cron 模式（period 模式为 nil）
	frozen   bool          // 孤儿冻结（重试超限 dead，§5.7）
}

// New 构造工厂并编译条目（cron 表达式非法立即报错）。
func New(s store.Store, cfg config.FactoryConfig, metrics *obs.Metrics, log *slog.Logger) (*Factory, error) {
	cfg.ApplyDefaults()
	if log == nil {
		log = slog.Default()
	}
	if metrics == nil {
		metrics, _ = obs.New()
	}
	f := &Factory{store: s, cfg: cfg, metrics: metrics, log: log}
	for _, def := range cfg.Entries {
		e := compiledEntry{def: def}
		if def.Period <= 0 && def.CronSpec == "" {
			return nil, fmt.Errorf("factory: entry %q: period or cron is required", def.Name)
		}
		if def.CronSpec != "" {
			schedule, err := cron.ParseStandard(def.CronSpec)
			if err != nil {
				return nil, fmt.Errorf("factory: entry %q: bad cron %q: %w", def.Name, def.CronSpec, err)
			}
			e.schedule = schedule
		}
		f.entries = append(f.entries, e)
	}
	return f, nil
}

// Run 阻塞运行生成循环，直到 ctx 取消。
func (f *Factory) Run(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		f.tick(ctx)
	}
}

func (f *Factory) tick(ctx context.Context) {
	now := time.Now()
	for i := range f.entries {
		e := &f.entries[i]
		if e.frozen {
			continue // 孤儿冻结：停止自动生成，等待人工修复后重新注册（§5.7）
		}
		f.tickEntry(ctx, e, now)
	}
}

func (f *Factory) tickEntry(ctx context.Context, e *compiledEntry, now time.Time) {
	def := e.def
	prefix := "factory:" + def.Name + ":"

	// 孤儿判定：该条目生成的任务出现过 dead（重试超限）→ 冻结 + 告警。
	dead, err := f.store.HasDeadByBatchPrefix(ctx, prefix)
	if err != nil {
		f.log.WarnContext(ctx, "factory: has dead check failed", "entry", def.Name, "err", err)
		return
	}
	if dead {
		e.frozen = true
		f.log.ErrorContext(ctx, "factory: entry FROZEN as orphan (dead task detected), "+
			"fix and re-register the entry", "entry", def.Name)
		return
	}

	// next 现算：不持久化绝对时间点（§5.7）。
	last, ok, err := f.store.LatestSuccessAt(ctx, prefix)
	if err != nil {
		f.log.WarnContext(ctx, "factory: latest success lookup failed", "entry", def.Name, "err", err)
		return
	}
	var next time.Time
	switch {
	case e.schedule != nil:
		if ok {
			next = e.schedule.Next(last) // 从上次成功起排下一次（错过窗口即跳过，不补跑）
		} else {
			next = e.schedule.Next(now) // 首次：从现在起排
		}
	default:
		if ok {
			next = last.Add(def.Period) // next = last_success + period
		} else {
			next = now // 首次：立即生成一个实例
		}
	}

	// 迟到上限（可选 window）：错过窗口即跳过、不补跑。
	if def.Window > 0 && now.After(next.Add(def.Window)) {
		f.log.WarnContext(ctx, "factory: missed window, skip",
			"entry", def.Name, "next", next.UnixMilli(), "now", now.UnixMilli())
		return
	}
	if now.Before(next) {
		return // 尚未到期
	}

	// 生成一次性任务实例：幂等键 = factory:{name}:{next.Unix()}
	//（重启/故障切换后重算 next 命中同一键 → 不重复生成；§5.7 不持久化时间点的补齐手段）。
	nt := sdk.NewTask{
		Type:           def.TaskType,
		Payload:        def.Payload,
		Priority:       sdk.Priority(def.Priority),
		ExecMode:       sdk.ExecMode(def.ExecMode),
		RunAt:          now,
		TimeoutMS:      def.TimeoutMS,
		MaxAttempts:    def.MaxAttempts,
		IdempotencyKey: prefix + fmt.Sprint(next.Unix()),
		BatchID:        prefix + fmt.Sprint(now.UnixMilli()),
	}
	created, err := f.store.CreatePending(ctx, []sdk.NewTask{nt})
	if err != nil {
		f.log.WarnContext(ctx, "factory: generate instance failed", "entry", def.Name, "err", err)
		return
	}
	f.log.InfoContext(ctx, "factory: instance generated",
		"entry", def.Name, "task_id", created[0].TaskID, "next", next.UnixMilli())
}
