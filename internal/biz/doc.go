// Package biz 是 api 契约服务端实现的归组目录：每个子包对齐一个 api 域，
// 便于按契约定位实现，也避免单包过大。
//
//   - task/       对应 api/stateflux/task/v1（ExecutorService）
//   - worker/     对应 api/stateflux/worker/v1（CapabilityService）
//   - coherence/  对应 api/stateflux/coherence/v1（CoherenceService）
//   - dispatch/   对应 api/dispatch/v1（DispatchService）
//
// api/stateflux/forward/v1（ForwardService）由 internal/forward 实现，不在本目录。
// gRPC 传输由 internal/server 托管，各子包与 worker/scheduler 相互不 import（§8）。
package biz
