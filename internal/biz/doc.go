// Package biz 实现 api/stateflux/task/v1 契约的服务端业务：Execute 同步执行编排与 ResultEvent
// 结果上报（经 eventbus/channel 发布）。gRPC 传输由 worker 托管，biz 经 server 装配注入，
// 与 worker/scheduler 相互不 import。
package biz
