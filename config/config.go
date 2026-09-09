// Package config 承载 stateflux 全部配置与观测注入（§8）：各角色包只依赖注入的 Meter，
// 不感知导出协议；OTel MeterProvider/OTLP Exporter 在此统一装配（§6.5）。
package config

import (
	"time"
)

// Priority 队列三级拆分默认值（§14.5）。
const QueuePriorities = "high,normal,low"

// PostgresConfig 任务表组（唯一权威存储）连接配置。
type PostgresConfig struct {
	DSN string // 例如 postgres://user:pass@127.0.0.1:5432/stateflux?sslmode=disable
	// MaxOpenConns / MaxIdleConns 连接池。
	MaxOpenConns int
	MaxIdleConns int
	// Automigrate 启动时执行 ent 自动迁移 + NOTIFY 触发器装配（生产可关闭，改由迁移工具执行）。
	Automigrate bool
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *PostgresConfig) ApplyDefaults() { c.applyDefaults() }

func (c *PostgresConfig) applyDefaults() {
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 20
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 5
	}
}

// RedisConfig Redis 视图（队列 + inprocess 集合）连接配置。
type RedisConfig struct {
	Addrs    []string
	Username string
	Password string
	DB       int
}

// ClusterConfig 集群视图配置（§2.1）。
type ClusterConfig struct {
	// NodeID 本节点唯一 ID。static 模式下必填。
	NodeID string
	// Address 本节点 gRPC 服务地址（供集群其他节点调用 Execute/Collect）。
	Address string
	// Capabilities 本节点能力标签（同步任务选节点匹配用，§5.3）。
	Capabilities []string
	// Mode: "static"（单机/开发模式，全部角色赋给本进程）或 "external"（外部选举服务 RPC）。
	Mode string
	// External 外部选举服务配置（Mode == external 时使用）。
	External ExternalClusterConfig
	// RefreshInterval 节点信息刷新周期（external 模式）。
	RefreshInterval time.Duration
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *ClusterConfig) ApplyDefaults() { c.applyDefaults() }

func (c *ClusterConfig) applyDefaults() {
	if c.Mode == "" {
		c.Mode = ClusterModeStatic
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = 5 * time.Second
	}
}

const (
	// ClusterModeStatic 单机/开发模式：全部角色赋给本进程，一个进程闭环（§2.1）。
	ClusterModeStatic = "static"
	// ClusterModeExternal 接入外部选举/成员服务。
	ClusterModeExternal = "external"
)

// ExternalClusterConfig 外部选举/成员服务的 RPC 客户端配置（§7 cluster.proto）。
type ExternalClusterConfig struct {
	Endpoint string // ClusterService gRPC 地址
	// Insecure 省略 TLS（内网部署）。
	Insecure bool
}

// SchedulerConfig 调度角色配置（§5.2/§5.3、§10 默认参数）。
type SchedulerConfig struct {
	// TickInterval 调度主循环定时兜底 tick（默认 100ms，§10）。
	TickInterval time.Duration
	// ClaimBatch 每 tick 认领上限（默认 500，§10）。
	ClaimBatch int
	// SyncDispatchPool 同步分发池 goroutine 上限（默认 512，§10）。
	SyncDispatchPool int
	// MaxQueueDepth 单队列容量上限（反压阈值，默认 10k，§6.4/§10）。
	MaxQueueDepth int64
	// PromoterInterval 约束晋升 tick（默认 200ms，§5.2/§10）。
	PromoterInterval time.Duration
	// PromoteBatch 晋升扫描批量。
	PromoteBatch int
	// TypeConcurrency per-type 全局并发上限（内置约束，按 processing 在途计数判定，§5.2）。
	TypeConcurrency map[string]int
	// QueueDepthInterval 队列深度采样周期（用于自适应认领与指标）。
	QueueDepthInterval time.Duration
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *SchedulerConfig) ApplyDefaults() { c.applyDefaults() }

func (c *SchedulerConfig) applyDefaults() {
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ClaimBatch == 0 {
		c.ClaimBatch = 500
	}
	if c.SyncDispatchPool == 0 {
		c.SyncDispatchPool = 512
	}
	if c.MaxQueueDepth == 0 {
		c.MaxQueueDepth = 10000
	}
	if c.PromoterInterval == 0 {
		c.PromoterInterval = 200 * time.Millisecond
	}
	if c.PromoteBatch == 0 {
		c.PromoteBatch = 1000
	}
	if c.QueueDepthInterval == 0 {
		c.QueueDepthInterval = 100 * time.Millisecond
	}
}

// ExecutorConfig 执行角色配置（§5.4、§10）。
type ExecutorConfig struct {
	// Concurrency 并发槽（同时执行的任务数上限，free_slots 上报依据）。
	Concurrency int
	// LeaseTTL / LeaseRenewInterval 租约与续约周期（默认 30s / 10s，§10）。
	LeaseTTL           time.Duration
	LeaseRenewInterval time.Duration
	// BRPOPTimeout 消费循环阻塞超时（影响优雅退出延迟）。
	BRPOPTimeout time.Duration
	// WAL 执行侧结果 WAL（§5.4）。
	WAL WALConfig
	// CapacityReportInterval free_slots 上报周期。
	CapacityReportInterval time.Duration
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *ExecutorConfig) ApplyDefaults() { c.applyDefaults() }

func (c *ExecutorConfig) applyDefaults() {
	if c.Concurrency == 0 {
		c.Concurrency = 64
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.LeaseRenewInterval == 0 {
		c.LeaseRenewInterval = 10 * time.Second
	}
	if c.BRPOPTimeout == 0 {
		c.BRPOPTimeout = time.Second
	}
	if c.CapacityReportInterval == 0 {
		c.CapacityReportInterval = 5 * time.Second
	}
	c.WAL.applyDefaults()
}

// WALConfig 执行侧结果 WAL 配置（§5.4）：append-only 磁盘日志、Ack 水位截断、重启重放、
// 高水位反压。WAL 是非权威派生状态：文件损坏/丢失 → 条目作废，由对账 R1 重跑兜底。
type WALConfig struct {
	// Dir WAL 目录。
	Dir string
	// MaxEntries 高水位条数（默认 10k，§10）；超过暂停 BRPOP。
	MaxEntries int64
	// MaxBytes 高水位字节数（默认 256MB，§10）。
	MaxBytes int64
	// SegmentBytes 单段文件大小上限（滚动）。
	SegmentBytes int64
	// FlushInterval WAL 刷盘周期（0 = 每条直接刷）。
	FlushInterval time.Duration
}

func (c *WALConfig) applyDefaults() {
	if c.MaxEntries == 0 {
		c.MaxEntries = 10000
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 256 << 20
	}
	if c.SegmentBytes == 0 {
		c.SegmentBytes = 64 << 20
	}
}

// CollectorConfig 归集角色配置（§5.5、§10）。
type CollectorConfig struct {
	// PullInterval 拉取周期（默认 500ms，§10）。
	PullInterval time.Duration
	// FlushCount / FlushInterval 缓冲双阈值（默认 200 条或 1s，§5.5）。
	FlushCount    int
	FlushInterval time.Duration
	// PullBatch 单节点单次拉取上限。
	PullBatch int
	// TombstoneTTL 终态墓碑 TTL（默认 90s，≥ grace，§6.2/§10）。
	TombstoneTTL time.Duration
	// MaxCallbackDepth 已在 sdk.MaxCallbackDepth 固定。
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *CollectorConfig) ApplyDefaults() { c.applyDefaults() }

func (c *CollectorConfig) applyDefaults() {
	if c.PullInterval == 0 {
		c.PullInterval = 500 * time.Millisecond
	}
	if c.FlushCount == 0 {
		c.FlushCount = 200
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
	if c.PullBatch == 0 {
		c.PullBatch = 500
	}
	if c.TombstoneTTL == 0 {
		c.TombstoneTTL = 90 * time.Second
	}
}

// ReconcileConfig 对账角色配置（§6.3、§10）。
type ReconcileConfig struct {
	// Interval 对账周期（默认 30s）。
	Interval time.Duration
	// Grace 滞留判定线（默认 90s）。墓碑 TTL 必须覆盖 grace（§6.2）。
	Grace time.Duration
	// Batch 单轮重置批量上限。
	Batch int
	// RebuildOnStart 启动时执行 R4 全量重建（Redis 丢失后的人工/自动恢复开关，§6.3）。
	RebuildOnStart bool
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *ReconcileConfig) ApplyDefaults() { c.applyDefaults() }

func (c *ReconcileConfig) applyDefaults() {
	if c.Interval == 0 {
		c.Interval = 30 * time.Second
	}
	if c.Grace == 0 {
		c.Grace = 90 * time.Second
	}
	if c.Batch == 0 {
		c.Batch = 1000
	}
}

// APIConfig API 角色配置（§5.1/§5.6）。
type APIConfig struct {
	// Listen gRPC 监听地址（业务接入点 TaskService/AdminService）。
	Listen string
	// LongPollInterval 长轮询 GetResults 的服务端轮询间隔。
	LongPollInterval time.Duration
	// MaxLongPoll 长轮询等待上限（防止客户端无限挂起）。
	MaxLongPoll time.Duration
	// MicroBatch 攒批微批（§5.1，可关）。窗口为 0 表示关闭。
	MicroBatchWindow time.Duration
	// MaxTxRows 单事务行数上限（防长事务，§5.1）。
	MaxTxRows int
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *APIConfig) ApplyDefaults() { c.applyDefaults() }

func (c *APIConfig) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":7000"
	}
	if c.LongPollInterval == 0 {
		c.LongPollInterval = 200 * time.Millisecond
	}
	if c.MaxLongPoll == 0 {
		c.MaxLongPoll = 30 * time.Second
	}
	if c.MaxTxRows == 0 {
		c.MaxTxRows = 5000
	}
}

// InternalListen 内部 gRPC（ExecutorService）监听地址。
type InternalConfig struct {
	Listen string
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *InternalConfig) ApplyDefaults() { c.applyDefaults() }

func (c *InternalConfig) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":7001"
	}
}

// FactoryConfig TaskFactory 配置（§5.7）。
type FactoryConfig struct {
	// Entries 周期/定时任务定义。
	Entries []FactoryEntry
	// TickInterval 生成检查周期。
	TickInterval time.Duration
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *FactoryConfig) ApplyDefaults() { c.applyDefaults() }

func (c *FactoryConfig) applyDefaults() {
	if c.TickInterval == 0 {
		c.TickInterval = time.Second
	}
}

// FactoryEntry 单个周期任务定义：period 或 cron 二选一。
type FactoryEntry struct {
	Name        string // 工厂名（batch_id 前缀 factory:{name}，用于孤儿冻结判定）
	TaskType    string // 生成任务的 type
	Payload     []byte // 生成任务的 payload 模板
	Priority    string
	ExecMode    string
	Period      time.Duration // next = last_success + period
	CronSpec    string        // cron 表达式（robfig/cron 标准格式），优先于 Period
	MaxAttempts int32
	TimeoutMS   int64
	// Window 允许的迟到上限（错过窗口即跳过、不补跑；0 表示不限制，§5.7）。
	Window time.Duration
}

// OTelConfig OpenTelemetry 配置（§6.5）：全框架指标统一走 OTel，导出协议可替换。
type OTelConfig struct {
	// Enabled 关闭时装配 noop provider（默认 false，零开销）。
	Enabled bool
	// ServiceName / NodeID / Version / Env 进 Resource 标签。
	ServiceName string
	NodeID      string
	Version     string
	Env         string
	// Exporter: "grpc" | "http"（OTLP），endpoint 可配。
	Exporter       string
	Endpoint       string
	Insecure       bool
	ExportInterval time.Duration
	// Headers OTLP 导出附带头。
	Headers map[string]string
}

// ApplyDefaults 导出的默认值补全入口（装配层使用）。
func (c *OTelConfig) ApplyDefaults() { c.applyDefaults() }

func (c *OTelConfig) applyDefaults() {
	if c.ServiceName == "" {
		c.ServiceName = "stateflux"
	}
	if c.Exporter == "" {
		c.Exporter = "grpc"
	}
	if c.ExportInterval == 0 {
		c.ExportInterval = 60 * time.Second // 默认 60s 推送（§6.5）
	}
}

// LogConfig 简单日志开关（框架日志保持轻量，生产接业务方 slog）。
type LogConfig struct {
	Debug bool
}

// Config 汇总配置。各角色包只读取自己需要的子配置。
type Config struct {
	Postgres  PostgresConfig
	Redis     RedisConfig
	Cluster   ClusterConfig
	Internal  InternalConfig
	Scheduler SchedulerConfig
	Executor  ExecutorConfig
	Collector CollectorConfig
	Reconcile ReconcileConfig
	API       APIConfig
	Factory   FactoryConfig
	OTel      OTelConfig
	Log       LogConfig
}

// ApplyDefaults 补全全部默认值（§10 默认参数表）。
func (c *Config) ApplyDefaults() {
	c.Postgres.applyDefaults()
	c.Cluster.applyDefaults()
	c.Internal.applyDefaults()
	c.Scheduler.applyDefaults()
	c.Executor.applyDefaults()
	c.Collector.applyDefaults()
	c.Reconcile.applyDefaults()
	c.API.applyDefaults()
	c.Factory.applyDefaults()
	c.OTel.applyDefaults()
}
