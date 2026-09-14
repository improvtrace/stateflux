// Package reconcile 实现 R1–R4 对账（全抖动退避），是全流程收敛性的兜底：通道丢失、Redis
// 灾难都只导致 processing 在 grace 后重置重投，而不依赖任何通道的可靠性或
// Redis 状态（§6.2）。仅调度节点运行。
package reconcile
