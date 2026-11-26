// Package metrics 定义 stateflux 的指标契约：每个指标的稳定名称、类型、单位、
// 标签集合、Help 文案、所属组件与落地优先级。
//
// 本包只做定义，不采集也不导出指标。埋点实现（cron/listener、publisher、
// dispatcher、reconciler、worker 与 repo）应引用这里的名称与标签常量，避免
// 代码与文档漂移；完整的中文说明、采集点、PromQL 与告警建议见同目录
// README.md 与各组件文档。
package metrics

// Namespace 是 stateflux 全部指标共用的前缀。
const Namespace = "stateflux"

// Kind 是指标类型，与 Prometheus 的类型一一对应。
type Kind string

const (
	// Counter 单调递增，进程重启后归零。
	Counter Kind = "counter"
	// Gauge 当前值，可升可降。
	Gauge Kind = "gauge"
	// Histogram 观测值分布，配合 _bucket/_sum/_count 使用。
	Histogram Kind = "histogram"
)

// Priority 是落地优先级，用于分批埋点与告警建设：
//   - P0：核心链路与错误，第一批必须接入并配置告警；
//   - P1：定位问题的关键上下文，第二批接入；
//   - P2：容量分析与深度诊断，按需接入。
type Priority string

const (
	P0 Priority = "P0"
	P1 Priority = "P1"
	P2 Priority = "P2"
)

// 标签键。标签值域是有限枚举或有限基数（节点、资源、队列、阶段等），
// 禁止把 task_id、UUID、payload、错误原文等无界值作为标签。
const (
	LabelNode      = "node"      // 集群节点 ID（stateflux_runtime_info、workers、dispatch_routed_total）
	LabelResource  = "resource"  // 任务资源名（= tasks.TaskSpec.Resource）
	LabelQueue     = "queue"     // 分节点队列名 queue:<resource>/<node>
	LabelHandler   = "handler"   // 事件处理器名（Listener.Handler.Name）
	LabelTrigger   = "trigger"   // 触发来源：on_demand / periodic / cron / recovery
	LabelResult    = "result"    // 结果：ok / error / succeeded / failed / retried / created / duplicate
	LabelReason    = "reason"    // 失败或重派原因，取值随指标定义
	LabelStage     = "stage"     // 处理阶段：create / claim / factory / dispatch / ...
	LabelOutcome   = "outcome"   // 终态/收敛结果：succeeded / failed / dead / requeued
	LabelOperation = "operation" // 数据库或账本操作名
	LabelMethod    = "method"    // dm RPC 方法名
	LabelState     = "state"     // 节点在线状态：online / offline
	LabelEvent     = "event"     // 调度事件类型：cron
	LabelKind      = "kind"      // 任务种类：durable / stateless
)

// 单位的统一写法。计数型 Counter 不带单位后缀，Unit 记作 1。
const (
	UnitCount    = "1"
	UnitSeconds  = "seconds"
	UnitTasks    = "tasks"
	UnitRows     = "rows"
	UnitNodes    = "nodes"
	UnitWorkers  = "workers"
	UnitMessages = "messages"
)

// 组件名，用于文档分组与实现归属。
const (
	ComponentScheduler  = "scheduler"
	ComponentPublisher  = "publisher"
	ComponentDispatch   = "dispatch"
	ComponentReconciler = "reconciler"
	ComponentExecution  = "execution"
	ComponentState      = "state"
	ComponentRuntime    = "runtime"
)

// Metric 描述一个指标。Name 是稳定契约：一经发布不可重命名，语义变化应新增
// 指标并标记旧指标废弃。
type Metric struct {
	Name      string   // 指标全名，含 stateflux_ 前缀
	Kind      Kind     // counter / gauge / histogram
	Unit      string   // 单位，见 Unit* 常量
	Labels    []string // 标签键集合，可为空
	Help      string   // 英文 Help 文案（Prometheus/OTel 的 description）
	Component string   // 所属组件
	Priority  Priority // 落地优先级
}

// 指标名称常量。埋点代码只引用这些常量，不写裸字符串。
const (
	// 定时触发（CronScheduler / Listener）。
	CronRegisteredTasks        = Namespace + "_cron_registered_tasks"
	CronTicksTotal             = Namespace + "_cron_ticks_total"
	CronLockAcquiredTotal      = Namespace + "_cron_lock_acquired_total"
	CronLockSkippedTotal       = Namespace + "_cron_lock_skipped_total"
	CronLockErrorsTotal        = Namespace + "_cron_lock_errors_total"
	CronEventsEmittedTotal     = Namespace + "_cron_events_emitted_total"
	CronEventsDroppedTotal     = Namespace + "_cron_events_dropped_total"
	CronEventHandleSeconds     = Namespace + "_cron_event_handle_seconds"
	CronEventErrorsTotal       = Namespace + "_cron_event_errors_total"
	ListenerEventsTotal        = Namespace + "_listener_events_total"
	ListenerHandlerPanicsTotal = Namespace + "_listener_handler_panics_total"

	// 发布通道（Publisher）。
	PublishBatchesTotal        = Namespace + "_publish_batches_total"
	PublishBatchSeconds        = Namespace + "_publish_batch_seconds"
	PublishBatchRows           = Namespace + "_publish_batch_rows"
	PublishTasksTotal          = Namespace + "_publish_tasks_total"
	PublishRowsClaimedTotal    = Namespace + "_publish_rows_claimed_total"
	PublishRowsDispatchedTotal = Namespace + "_publish_rows_dispatched_total"
	PublishRowsRequeuedTotal   = Namespace + "_publish_rows_requeued_total"
	PublishErrorsTotal         = Namespace + "_publish_errors_total"

	// 发布器（Dispatcher / 节点表 / dm）。
	DispatchAttemptsTotal           = Namespace + "_dispatch_attempts_total"
	DispatchSeconds                 = Namespace + "_dispatch_seconds"
	DispatchErrorsTotal             = Namespace + "_dispatch_errors_total"
	DispatchRoutedTotal             = Namespace + "_dispatch_routed_total"
	DispatchCandidateNodes          = Namespace + "_dispatch_candidate_nodes"
	DispatchNodeTableRefreshesTotal = Namespace + "_dispatch_node_table_refreshes_total"
	DispatchNodeTableRefreshSeconds = Namespace + "_dispatch_node_table_refresh_seconds"
	DispatchNodeTableNodes          = Namespace + "_dispatch_node_table_nodes"
	DispatchNodeTableAgeSeconds     = Namespace + "_dispatch_node_table_age_seconds"
	DMRequestsTotal                 = Namespace + "_dm_requests_total"
	DMRequestSeconds                = Namespace + "_dm_request_seconds"
	DMConnectErrorsTotal            = Namespace + "_dm_connect_errors_total"

	// 对账（Reconciler）。
	ReconcileRoundsTotal         = Namespace + "_reconcile_rounds_total"
	ReconcileRoundSeconds        = Namespace + "_reconcile_round_seconds"
	ReconcileInflightRows        = Namespace + "_reconcile_inflight_rows"
	ReconcileRowsTimedOutTotal   = Namespace + "_reconcile_rows_timed_out_total"
	ReconcileSettledTotal        = Namespace + "_reconcile_settled_total"
	ReconcileErrorsTotal         = Namespace + "_reconcile_errors_total"
	ReconcileBackendStateSeconds = Namespace + "_reconcile_backend_state_seconds"

	// 执行（handler / worker / queue）。
	TaskExecutionsTotal         = Namespace + "_task_executions_total"
	TaskExecutionSeconds        = Namespace + "_task_execution_seconds"
	TaskWritebackErrorsTotal    = Namespace + "_task_writeback_errors_total"
	Workers                     = Namespace + "_workers"
	WorkerConcurrency           = Namespace + "_worker_concurrency"
	WorkerErrorsTotal           = Namespace + "_worker_errors_total"
	QueueMessagesReceivedTotal  = Namespace + "_queue_messages_received_total"
	QueueMessagesProcessedTotal = Namespace + "_queue_messages_processed_total"
	QueueRetriesTotal           = Namespace + "_queue_retries_total"
	QueuePanicsTotal            = Namespace + "_queue_panics_total"
	QueuePendingMessages        = Namespace + "_queue_pending_messages"
	QueueDelayedMessages        = Namespace + "_queue_delayed_messages"
	QueueBrokerErrorsTotal      = Namespace + "_queue_broker_errors_total"

	// 状态存储（task_persistents / ledger）。
	TaskRowsCreatedTotal  = Namespace + "_task_rows_created_total"
	TaskRowsClaimedTotal  = Namespace + "_task_rows_claimed_total"
	TaskRowsTerminalTotal = Namespace + "_task_rows_terminal_total"
	TaskRowsRequeuedTotal = Namespace + "_task_rows_requeued_total"
	DBOperationsTotal     = Namespace + "_db_operations_total"
	DBOperationSeconds    = Namespace + "_db_operation_seconds"
	LedgerOperationsTotal = Namespace + "_ledger_operations_total"

	// 进程与依赖（runtime）。
	Up               = Namespace + "_up"
	StartTimeSeconds = Namespace + "_start_time_seconds"
	RuntimeInfo      = Namespace + "_runtime_info"
	PostgresUp       = Namespace + "_postgres_up"
	RedisUp          = Namespace + "_redis_up"
)

// catalog 是全部指标的唯一清单。新增指标：先在名称常量区登记，再在此追加
// 一条定义，最后在同目录组件文档补齐语义与采集点。
var catalog = []Metric{
	// ---- 定时触发 ----
	{
		Name: CronRegisteredTasks, Kind: Gauge, Unit: UnitTasks,
		Help:      "Number of cron-triggered task resources registered in this process.",
		Component: ComponentScheduler, Priority: P1,
	},
	{
		Name: CronTicksTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Cron ticks fired by the scheduler before the single-node lock check.",
		Component: ComponentScheduler, Priority: P0,
	},
	{
		Name: CronLockAcquiredTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Cron ticks that acquired the single-node lock and emitted an event.",
		Component: ComponentScheduler, Priority: P0,
	},
	{
		Name: CronLockSkippedTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Cron ticks skipped because another node held the single-node lock.",
		Component: ComponentScheduler, Priority: P1,
	},
	{
		Name: CronLockErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Cron ticks that failed to acquire the single-node lock because of a Redis error.",
		Component: ComponentScheduler, Priority: P0,
	},
	{
		Name: CronEventsEmittedTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Scheduler events accepted into the listener buffer.",
		Component: ComponentScheduler, Priority: P1,
	},
	{
		Name: CronEventsDroppedTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Scheduler events dropped because the listener buffer was full.",
		Component: ComponentScheduler, Priority: P0,
	},
	{
		Name: CronEventHandleSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelResource, LabelResult},
		Help:      "Time spent handling one cron event, including batch generation and publishing.",
		Component: ComponentScheduler, Priority: P1,
	},
	{
		Name: CronEventErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource, LabelStage},
		Help:      "Cron event handling failures by stage.",
		Component: ComponentScheduler, Priority: P0,
	},
	{
		Name: ListenerEventsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelEvent},
		Help:      "Events dispatched by the listener event loop.",
		Component: ComponentScheduler, Priority: P1,
	},
	{
		Name: ListenerHandlerPanicsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelHandler},
		Help:      "Panics recovered while dispatching an event to a handler.",
		Component: ComponentScheduler, Priority: P1,
	},

	// ---- 发布通道 ----
	{
		Name: PublishBatchesTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelTrigger, LabelResult},
		Help:      "Publish batches handled by the publisher, by trigger and result.",
		Component: ComponentPublisher, Priority: P1,
	},
	{
		Name: PublishBatchSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelTrigger},
		Help:      "Time spent handling one publish batch.",
		Component: ComponentPublisher, Priority: P1,
	},
	{
		Name: PublishBatchRows, Kind: Histogram, Unit: UnitRows, Labels: []string{LabelTrigger},
		Help:      "Number of task rows handled in one publish batch.",
		Component: ComponentPublisher, Priority: P2,
	},
	{
		Name: PublishTasksTotal, Kind: Counter, Unit: UnitTasks, Labels: []string{LabelResource, LabelKind, LabelResult},
		Help:      "Tasks published by the publisher, by task kind and result.",
		Component: ComponentPublisher, Priority: P0,
	},
	{
		Name: PublishRowsClaimedTotal, Kind: Counter, Unit: UnitRows,
		Help:      "Task rows claimed from pending in publish batches.",
		Component: ComponentPublisher, Priority: P0,
	},
	{
		Name: PublishRowsDispatchedTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource},
		Help:      "Task rows successfully published to the task queue.",
		Component: ComponentPublisher, Priority: P0,
	},
	{
		Name: PublishRowsRequeuedTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource, LabelReason},
		Help:      "Task rows returned to pending after a publish failure, by reason.",
		Component: ComponentPublisher, Priority: P0,
	},
	{
		Name: PublishErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelStage},
		Help:      "Publisher failures by processing stage.",
		Component: ComponentPublisher, Priority: P0,
	},

	// ---- 发布器与节点表 ----
	{
		Name: DispatchAttemptsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Signatures handed to the dispatcher.",
		Component: ComponentDispatch, Priority: P0,
	},
	{
		Name: DispatchSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelResource, LabelResult},
		Help:      "Time spent routing and publishing one signature.",
		Component: ComponentDispatch, Priority: P0,
	},
	{
		Name: DispatchErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource, LabelReason},
		Help:      "Dispatch failures by reason.",
		Component: ComponentDispatch, Priority: P0,
	},
	{
		Name: DispatchRoutedTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource, LabelNode},
		Help:      "Signatures routed to a node queue, by target node.",
		Component: ComponentDispatch, Priority: P1,
	},
	{
		Name: DispatchCandidateNodes, Kind: Histogram, Unit: UnitNodes, Labels: []string{LabelResource},
		Help:      "Number of online nodes that satisfied the routing constraints.",
		Component: ComponentDispatch, Priority: P2,
	},
	{
		Name: DispatchNodeTableRefreshesTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResult},
		Help:      "Node table refreshes triggered by cache expiry, by result.",
		Component: ComponentDispatch, Priority: P1,
	},
	{
		Name: DispatchNodeTableRefreshSeconds, Kind: Histogram, Unit: UnitSeconds,
		Help:      "Time spent fetching the cluster node table from the dm service.",
		Component: ComponentDispatch, Priority: P2,
	},
	{
		Name: DispatchNodeTableNodes, Kind: Gauge, Unit: UnitNodes, Labels: []string{LabelState},
		Help:      "Nodes in the last fetched node table, by online state.",
		Component: ComponentDispatch, Priority: P1,
	},
	{
		Name: DispatchNodeTableAgeSeconds, Kind: Gauge, Unit: UnitSeconds,
		Help:      "Age of the node table cache at the last routing decision.",
		Component: ComponentDispatch, Priority: P2,
	},
	{
		Name: DMRequestsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelMethod, LabelResult},
		Help:      "Requests sent to the dm service, by method and result.",
		Component: ComponentDispatch, Priority: P1,
	},
	{
		Name: DMRequestSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelMethod},
		Help:      "Time spent per dm request.",
		Component: ComponentDispatch, Priority: P1,
	},
	{
		Name: DMConnectErrorsTotal, Kind: Counter, Unit: UnitCount,
		Help:      "Failed connections to the dm service.",
		Component: ComponentDispatch, Priority: P1,
	},

	// ---- 对账 ----
	{
		Name: ReconcileRoundsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelTrigger, LabelResult},
		Help:      "Reconciliation rounds completed, by trigger and result.",
		Component: ComponentReconciler, Priority: P0,
	},
	{
		Name: ReconcileRoundSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelTrigger},
		Help:      "Time spent in one reconciliation round.",
		Component: ComponentReconciler, Priority: P1,
	},
	{
		Name: ReconcileInflightRows, Kind: Gauge, Unit: UnitRows,
		Help:      "Inflight rows returned by the last reconciliation scan, capped by the batch size.",
		Component: ComponentReconciler, Priority: P1,
	},
	{
		Name: ReconcileRowsTimedOutTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource},
		Help:      "Inflight rows that exceeded their execution budget and were settled.",
		Component: ComponentReconciler, Priority: P0,
	},
	{
		Name: ReconcileSettledTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource, LabelOutcome},
		Help:      "Rows settled by reconciliation, by outcome.",
		Component: ComponentReconciler, Priority: P0,
	},
	{
		Name: ReconcileErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelStage},
		Help:      "Reconciliation failures by stage.",
		Component: ComponentReconciler, Priority: P0,
	},
	{
		Name: ReconcileBackendStateSeconds, Kind: Histogram, Unit: UnitSeconds,
		Help:      "Time spent querying the queue backend for a task terminal state.",
		Component: ComponentReconciler, Priority: P2,
	},

	// ---- 执行 ----
	{
		Name: TaskExecutionsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource, LabelResult},
		Help:      "Handler executions, by resource and result.",
		Component: ComponentExecution, Priority: P0,
	},
	{
		Name: TaskExecutionSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelResource, LabelResult},
		Help:      "Handler execution duration, by resource and result.",
		Component: ComponentExecution, Priority: P0,
	},
	{
		Name: TaskWritebackErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Failures while writing a successful execution back to task_persistents.",
		Component: ComponentExecution, Priority: P0,
	},
	{
		Name: Workers, Kind: Gauge, Unit: UnitWorkers, Labels: []string{LabelResource, LabelNode},
		Help:      "Workers launched by this process, one per resource and node.",
		Component: ComponentExecution, Priority: P1,
	},
	{
		Name: WorkerConcurrency, Kind: Gauge, Unit: UnitWorkers, Labels: []string{LabelResource},
		Help:      "Configured consumer concurrency per resource.",
		Component: ComponentExecution, Priority: P2,
	},
	{
		Name: WorkerErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelResource},
		Help:      "Workers that exited with an error.",
		Component: ComponentExecution, Priority: P1,
	},
	{
		Name: QueueMessagesReceivedTotal, Kind: Counter, Unit: UnitMessages, Labels: []string{LabelQueue},
		Help:      "Messages delivered to a worker queue.",
		Component: ComponentExecution, Priority: P1,
	},
	{
		Name: QueueMessagesProcessedTotal, Kind: Counter, Unit: UnitMessages, Labels: []string{LabelQueue, LabelResult},
		Help:      "Messages processed by a worker, by result.",
		Component: ComponentExecution, Priority: P0,
	},
	{
		Name: QueueRetriesTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelQueue},
		Help:      "Task executions retried by the queue framework.",
		Component: ComponentExecution, Priority: P0,
	},
	{
		Name: QueuePanicsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelQueue},
		Help:      "Task panics recovered by the queue framework.",
		Component: ComponentExecution, Priority: P0,
	},
	{
		Name: QueuePendingMessages, Kind: Gauge, Unit: UnitMessages, Labels: []string{LabelQueue},
		Help:      "Messages waiting in a Redis queue, sampled on scrape or on a fixed interval.",
		Component: ComponentExecution, Priority: P1,
	},
	{
		Name: QueueDelayedMessages, Kind: Gauge, Unit: UnitMessages, Labels: []string{LabelQueue},
		Help:      "Messages scheduled in the delayed set, sampled on scrape or on a fixed interval.",
		Component: ComponentExecution, Priority: P1,
	},
	{
		Name: QueueBrokerErrorsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelOperation},
		Help:      "Redis broker operation failures by operation.",
		Component: ComponentExecution, Priority: P2,
	},

	// ---- 状态存储 ----
	{
		Name: TaskRowsCreatedTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource, LabelResult},
		Help:      "Task rows created by the publisher, by result.",
		Component: ComponentState, Priority: P1,
	},
	{
		Name: TaskRowsClaimedTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource},
		Help:      "Task rows claimed from pending.",
		Component: ComponentState, Priority: P1,
	},
	{
		Name: TaskRowsTerminalTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource, LabelOutcome},
		Help:      "Task rows written to a terminal outcome.",
		Component: ComponentState, Priority: P1,
	},
	{
		Name: TaskRowsRequeuedTotal, Kind: Counter, Unit: UnitRows, Labels: []string{LabelResource, LabelReason},
		Help:      "Task rows returned to pending for redispatch, by reason.",
		Component: ComponentState, Priority: P1,
	},
	{
		Name: DBOperationsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelOperation, LabelResult},
		Help:      "task_persistents database operations, by operation and result.",
		Component: ComponentState, Priority: P0,
	},
	{
		Name: DBOperationSeconds, Kind: Histogram, Unit: UnitSeconds, Labels: []string{LabelOperation},
		Help:      "task_persistents database operation latency.",
		Component: ComponentState, Priority: P0,
	},
	{
		Name: LedgerOperationsTotal, Kind: Counter, Unit: UnitCount, Labels: []string{LabelOperation, LabelResult},
		Help:      "Redis ledger operations, by operation and result.",
		Component: ComponentState, Priority: P0,
	},

	// ---- 进程与依赖 ----
	{
		Name: Up, Kind: Gauge, Unit: UnitCount,
		Help:      "1 when the stateflux process is running.",
		Component: ComponentRuntime, Priority: P0,
	},
	{
		Name: StartTimeSeconds, Kind: Gauge, Unit: UnitSeconds,
		Help:      "Unix timestamp of process start.",
		Component: ComponentRuntime, Priority: P1,
	},
	{
		Name: RuntimeInfo, Kind: Gauge, Unit: UnitCount, Labels: []string{LabelNode},
		Help:      "Static identity of this process; the value is always 1.",
		Component: ComponentRuntime, Priority: P1,
	},
	{
		Name: PostgresUp, Kind: Gauge, Unit: UnitCount,
		Help:      "1 when the PostgreSQL connection passes a health probe, 0 otherwise.",
		Component: ComponentRuntime, Priority: P0,
	},
	{
		Name: RedisUp, Kind: Gauge, Unit: UnitCount,
		Help:      "1 when the Redis connection passes a health probe, 0 otherwise.",
		Component: ComponentRuntime, Priority: P0,
	},
}

// Catalog 返回全部指标定义的副本；调用方修改返回值（含标签切片）不会影响
// 包内清单。
func Catalog() []Metric {
	out := make([]Metric, len(catalog))
	for i, m := range catalog {
		m.Labels = append([]string(nil), m.Labels...)
		out[i] = m
	}
	return out
}

// Get 按指标全名查询定义。
func Get(name string) (Metric, bool) {
	for _, m := range catalog {
		if m.Name == name {
			m.Labels = append([]string(nil), m.Labels...)
			return m, true
		}
	}
	return Metric{}, false
}
