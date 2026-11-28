// Package sayhello 是内置的演示任务：每 30 秒发一句问候语，作为「一目录一
// 任务」的在库样板——新增业务任务即照此结构（Resource 常量 + Payload +
// Spec + Handler），再在根包 NewSpecs 加一行。问候只在执行时写日志：非
// Durable 定时任务直发不过表、不进对账，丢了由下个周期自然覆盖（DESIGN.md
// §3.1 / D2）。
package sayhello

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/improvtrace/stateflux/tasks"
)

// Resource 是任务资源名：全局唯一，路由 header AttributeResource，分节点
// 队列即 queue:sayhello/<node>。
const Resource = "sayhello"

// Cron 每 30 秒触发一次：亚分钟节奏用 robfig/cron 的 @every 描述符表达
// （五段表达式最小粒度为 1 分钟）。
const Cron = "@every 30s"

// 缺省问候语与其按发布约定（单 string Arg 承载 JSON）编码的固定参数，
// 两者须一致（sayhello_test 校验）。
const (
	defaultGreeting = "hello from stateflux"
	defaultPayload  = `{"greeting":"hello from stateflux"}`
)

// Payload 是问候参数。
type Payload struct {
	Greeting string `json:"greeting"`
}

// Handle 执行一次问候：写一行 Info 日志即完成「发送」。taskID 恒为 0——
// 非 Durable 直发没有表行。
func Handle(payload string, taskID int64) error {
	var p Payload
	if payload != "" {
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return fmt.Errorf("sayhello: decode payload: %w", err)
		}
	}
	if p.Greeting == "" {
		p.Greeting = defaultGreeting
	}
	slog.Info("sayhello: greeting sent", "greeting", p.Greeting, "task_id", taskID)
	return nil
}

// Spec 返回任务定义：非 Durable（不落 task_persistents）、单并发足够；
// Enabled 显式开启（TaskSpec.Enabled 零值即禁用）。本任务无注入依赖，不经
// wire ProviderSet，由根包 NewSpecs 直接枚举。
func Spec() *tasks.TaskSpec {
	return &tasks.TaskSpec{
		Resource:    Resource,
		Enabled:     true,
		Concurrency: 1,
		Handler:     Handle,
		Payload:     json.RawMessage(defaultPayload),
		Cron:        Cron,
	}
}
