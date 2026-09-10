// Package dispatch 是调度↔执行传输层：task/v1 gRPC 客户端池与统一结果缓冲
// （ResultSink）。biz/scheduler/worker 相互零依赖，跨运行时协作仅经本包与
// api/stateflux/task/v1 契约传递。
package dispatch
