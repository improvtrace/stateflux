// Package dispatch 是调度侧分发器：按任务行记录的 channel 名从 eventbus 注册表选实现、选
// 执行节点，并把 ResultSink（统一结果缓冲）收到的响应适配为 ResultEvent 送入同一归集入口
// （§5.3、§5.5）。biz/runtime/worker 相互零依赖，跨运行时协作仅经 api/stateflux/task/v1
// 契约与 eventbus/channel 传递。
package dispatch
