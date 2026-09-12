// Package server 是编排层：装配 biz（task/v1 服务端业务）、控制面运行时（scheduler /
// collector / reconcile）与执行侧运行时（worker），注入 eventbus/channel 实现、domain 仓储
// 与 obs，并托管 Handler 注册点（§1.2.8、§8）。运行语义不落在这里，只在装配与启停。
package server
