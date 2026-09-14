package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是服务总配置（§8、§15.1#3）：flag/env 解析后填充，缺省值由 Default 提供。
type Config struct {
	PG        PG
	Redis     Redis
	Cluster   Cluster
	Node      Node
	Server    Server
	Runtime   Runtime
	Dispatch  Dispatch
	Coherence Coherence
	Obs       Obs
}

// PG 是 PostgreSQL 连接配置（§3.1：任务账本唯一权威存储）。
type PG struct {
	// DSN 数据源名称，如 postgres://user:pass@host:5432/db?sslmode=disable。
	DSN string
	// MaxOpenConns 最大打开连接数。
	MaxOpenConns int
	// MaxIdleConns 最大空闲连接数。
	MaxIdleConns int
	// ConnMaxLifetime 连接最长复用时间。
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime 空闲连接最长保留时间。
	ConnMaxIdleTime time.Duration
	// SearchPath 是连接建立后固定的 search_path（默认 public）：避免「用户名与 schema 同名」
	// 时 PostgreSQL 的 "$user" 隐式 schema 把表解析到错误位置（§3.1）。
	SearchPath string
}

// Redis 是 Redis 连接配置（§3.2：仅作为 best-effort 通信实现，正确性不依赖 Redis）。
type Redis struct {
	// Addrs 节点地址列表：单个地址走单机客户端，多个地址走 cluster 客户端。
	Addrs        []string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
}

// ClusterTransport 是集群视图客户端的传输形态（§15.1#3）：grpc / http / static。
type ClusterTransport string

const (
	// ClusterGRPC 经 gRPC 调用外部 ClusterService。
	ClusterGRPC ClusterTransport = "grpc"
	// ClusterHTTP 经 HTTP/JSON 调用外部 ClusterService（网关式部署）。
	ClusterHTTP ClusterTransport = "http"
	// ClusterStatic 单机模式：节点列表与调度节点直接来自配置，不访问外部系统。
	ClusterStatic ClusterTransport = "static"
)

// Cluster 是外置集群视图的连接信息（§15.1#3），归入 Stateflux.Config.Cluster。
type Cluster struct {
	// Transport 选择客户端实现：grpc（默认）/ http / static。
	Transport ClusterTransport
	// Endpoint 外部选举/成员服务地址。grpc 为 host:port；http 为 base URL。
	Endpoint string
	// Timeout 单次拉取超时。
	Timeout time.Duration
	// PollInterval 周期拉取间隔；0 表示只按需拉取。
	PollInterval time.Duration
	// StaticNodes 单机/静态模式下的节点清单（仅 Transport == static 时使用）。
	StaticNodes []Node
	// StaticSchedulerNodeID 静态模式下的调度节点 ID；空表示本节点。
	StaticSchedulerNodeID string
}

// Node 是本进程的节点身份（§2.1、§15.1#3）：注册进集群视图的能力声明。
type Node struct {
	// NodeID 节点唯一 ID；空则由集群视图或地址补齐（static 模式必填）。
	NodeID string
	// Address 对外可达的 gRPC 地址（host:port）。
	Address string
	// VPC 节点所属网络域（调度目标节点属性）。
	VPC string
	// Label 节点匹配标签（自由文本，框架不解释语义）。
	Label string
	// Roles 本节点承担的角色：biz / executor / scheduler / collector / reconcile。
	Roles []string
	// Capabilities 能力标签（worker 能力名集合，供调度侧匹配）。
	Capabilities []string
}

// Server 是服务监听配置。
type Server struct {
	// GRPCAddr gRPC 监听地址（ExecutorService / DispatchService / CoherenceService /
	// ForwardService / CapabilityService 共用）。
	GRPCAddr string
	// HTTPAddr 健康检查与诊断 HTTP 监听地址。
	HTTPAddr string
	// ShutdownTimeout 优雅退出等待上限。
	ShutdownTimeout time.Duration
}

// Runtime 是控制面运行时开关（§2.1、§15.1#11）。
type Runtime struct {
	// SchedulerSchedulers 调度运行时实例名列表：同一进程可承载多个触发器实例
	// （tick / notify / coherence / manual），空则按内置默认启用 tick。
	SchedulerTriggers []string
	// EnableCollector 是否启用结果归集运行时。
	EnableCollector bool
	// EnableReconcile 是否启用 R1–R4 对账运行时。
	EnableReconcile bool
	// ClaimBatch 单次认领批量（§10 默认 500）。
	ClaimBatch int
	// TickInterval 调度 tick（§10 默认 100ms）。
	TickInterval time.Duration
	// PromoteInterval 晋升 tick（§10 默认 200ms）。
	PromoteInterval time.Duration
	// ReconcileInterval R1 扫描间隔。
	ReconcileInterval time.Duration
	// DispatchGrace 通道无结果前不重试的最小窗口（§10 默认 90s）。
	DispatchGrace time.Duration
	// WorkerQueues 是执行运行时订阅的逻辑队列集合（异步任务 channel 名，§5.3）。
	WorkerQueues []string
	// FactoryInterval 是内置示例工厂的生成周期（§5.6）；<=0 表示不注册工厂。
	FactoryInterval time.Duration
}

// Delivery 是分发投递形态（§15.1#5）。
type Delivery string

const (
	// DeliveryRedisQueue 异步 redis queue 投递。
	DeliveryRedisQueue Delivery = "redis_queue"
	// DeliverySyncRPC 同步 rpc 投递。
	DeliverySyncRPC Delivery = "sync_rpc"
)

// Semantics 是 Dispatch 的投递语义（§15.1#5）。
type Semantics string

const (
	// AtLeastOnce 至少一次：允许重复，失败可重试。
	AtLeastOnce Semantics = "at_least_once"
	// AtMostOnce 至多一次：放弃重试，允许丢失。
	AtMostOnce Semantics = "at_most_once"
	// ExactlyOnce 尽力恰一次：入口去重 + 幂等账本，通道丢失仍由对账兜底。
	ExactlyOnce Semantics = "exactly_once"
)

// Dispatch 是分发器配置（§15.1#5）。
type Dispatch struct {
	// DefaultSemantics 未显式指定时的投递语义。
	DefaultSemantics Semantics
	// DefaultDelivery 未显式指定时的投递形态。
	DefaultDelivery Delivery
	// Forward 是否允许节点间转发。
	Forward bool
	// MaxHops 转发跳数上限（环路保护）。
	MaxHops int
	// Timeout 单次分发超时。
	Timeout time.Duration
	// DedupeWindow exactly_once 的入口去重窗口。
	DedupeWindow time.Duration
}

// Coherence 是共识信息同步配置（§15.1#4）。
type Coherence struct {
	// QueuePrefix 异步队列名前缀（队列↔节点映射的键空间）。
	QueuePrefix string
	// SyncInterval 执行节点周期拉取共识信息的间隔。
	SyncInterval time.Duration
	// RevisionTTL 共识 revision 在 redis 的保留时长。
	RevisionTTL time.Duration
}

// Obs 是观测装配参数（§6.4、§15.1#10）。
type Obs struct {
	// Enabled 是否启用 OTel 导出；false 时 instruments 为 no-op。
	Enabled bool
	// ServiceName 资源服务名。
	ServiceName string
	// OTLPEndpoint OTLP 导出地址（grpc）。
	OTLPEndpoint string
	// Insecure 是否使用明文 OTLP 连接。
	Insecure bool
}

// Default 返回全量默认配置。
func Default() Config {
	return Config{
		PG: PG{
			DSN:             "postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable",
			MaxOpenConns:    30,
			MaxIdleConns:    10,
			ConnMaxLifetime: 30 * time.Minute,
			ConnMaxIdleTime: 5 * time.Minute,
			SearchPath:      "public",
		},
		Redis: Redis{
			Addrs:        []string{"127.0.0.1:6379"},
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		},
		Cluster: Cluster{
			Transport:    ClusterStatic,
			Timeout:      3 * time.Second,
			PollInterval: 10 * time.Second,
			StaticNodes: []Node{{
				NodeID:       "local",
				Address:      "127.0.0.1:9090",
				Roles:        []string{"biz", "executor", "scheduler", "collector"},
				Capabilities: []string{},
			}},
		},
		Node: Node{
			NodeID:  "local",
			Address: "127.0.0.1:9090",
			Roles:   []string{"biz", "executor", "scheduler", "collector"},
		},
		Server: Server{
			GRPCAddr:        "127.0.0.1:9090",
			HTTPAddr:        "127.0.0.1:9091",
			ShutdownTimeout: 10 * time.Second,
		},
		Runtime: Runtime{
			SchedulerTriggers: []string{"tick"},
			EnableCollector:   true,
			EnableReconcile:   true,
			ClaimBatch:        500,
			TickInterval:      100 * time.Millisecond,
			PromoteInterval:   200 * time.Millisecond,
			ReconcileInterval: 10 * time.Second,
			DispatchGrace:     90 * time.Second,
			WorkerQueues:      []string{"default"},
			FactoryInterval:   30 * time.Second,
		},
		Dispatch: Dispatch{
			DefaultSemantics: AtLeastOnce,
			DefaultDelivery:  DeliveryRedisQueue,
			Forward:          true,
			MaxHops:          3,
			Timeout:          10 * time.Second,
			DedupeWindow:     10 * time.Minute,
		},
		Coherence: Coherence{
			QueuePrefix:  "stateflux:queue:",
			SyncInterval: 10 * time.Second,
			RevisionTTL:  5 * time.Minute,
		},
		Obs: Obs{
			Enabled:     false,
			ServiceName: "stateflux",
			Insecure:    true,
		},
	}
}

// FromEnv 在 Default 之上应用环境变量覆盖（STATEFLUX_ 前缀）；
// 解析失败的项静默保留默认值，由启动后的健康检查暴露问题。
func FromEnv() Config {
	cfg := Default()

	envString(func(key, val string) { cfg.PG.DSN = val }, "STATEFLUX_PG_DSN")
	envInt(func(key string, val int) { cfg.PG.MaxOpenConns = val }, "STATEFLUX_PG_MAX_OPEN_CONNS")
	envInt(func(key string, val int) { cfg.PG.MaxIdleConns = val }, "STATEFLUX_PG_MAX_IDLE_CONNS")
	envDuration(func(key string, val time.Duration) { cfg.PG.ConnMaxLifetime = val }, "STATEFLUX_PG_CONN_MAX_LIFETIME")
	envDuration(func(key string, val time.Duration) { cfg.PG.ConnMaxIdleTime = val }, "STATEFLUX_PG_CONN_MAX_IDLE_TIME")
	envString(func(key, val string) { cfg.PG.SearchPath = val }, "STATEFLUX_PG_SEARCH_PATH")

	envStrings(func(key string, val []string) { cfg.Redis.Addrs = val }, "STATEFLUX_REDIS_ADDRS")
	envString(func(key, val string) { cfg.Redis.Password = val }, "STATEFLUX_REDIS_PASSWORD")
	envInt(func(key string, val int) { cfg.Redis.DB = val }, "STATEFLUX_REDIS_DB")
	envDuration(func(key string, val time.Duration) { cfg.Redis.DialTimeout = val }, "STATEFLUX_REDIS_DIAL_TIMEOUT")
	envDuration(func(key string, val time.Duration) { cfg.Redis.ReadTimeout = val }, "STATEFLUX_REDIS_READ_TIMEOUT")
	envDuration(func(key string, val time.Duration) { cfg.Redis.WriteTimeout = val }, "STATEFLUX_REDIS_WRITE_TIMEOUT")
	envInt(func(key string, val int) { cfg.Redis.PoolSize = val }, "STATEFLUX_REDIS_POOL_SIZE")
	envInt(func(key string, val int) { cfg.Redis.MinIdleConns = val }, "STATEFLUX_REDIS_MIN_IDLE_CONNS")

	envString(func(key, val string) { cfg.Cluster.Transport = ClusterTransport(val) }, "STATEFLUX_CLUSTER_TRANSPORT")
	envString(func(key, val string) { cfg.Cluster.Endpoint = val }, "STATEFLUX_CLUSTER_ENDPOINT")
	envDuration(func(key string, val time.Duration) { cfg.Cluster.Timeout = val }, "STATEFLUX_CLUSTER_TIMEOUT")
	envDuration(func(key string, val time.Duration) { cfg.Cluster.PollInterval = val }, "STATEFLUX_CLUSTER_POLL_INTERVAL")
	envString(func(key, val string) { cfg.Cluster.StaticSchedulerNodeID = val }, "STATEFLUX_CLUSTER_STATIC_SCHEDULER_NODE_ID")

	envString(func(key, val string) { cfg.Node.NodeID = val }, "STATEFLUX_NODE_ID")
	envString(func(key, val string) { cfg.Node.Address = val }, "STATEFLUX_NODE_ADDRESS")
	envString(func(key, val string) { cfg.Node.VPC = val }, "STATEFLUX_NODE_VPC")
	envString(func(key, val string) { cfg.Node.Label = val }, "STATEFLUX_NODE_LABEL")
	envStrings(func(key string, val []string) { cfg.Node.Roles = val }, "STATEFLUX_NODE_ROLES")
	envStrings(func(key string, val []string) { cfg.Node.Capabilities = val }, "STATEFLUX_NODE_CAPABILITIES")

	envString(func(key, val string) { cfg.Server.GRPCAddr = val }, "STATEFLUX_SERVER_GRPC_ADDR")
	envString(func(key, val string) { cfg.Server.HTTPAddr = val }, "STATEFLUX_SERVER_HTTP_ADDR")

	envStrings(func(key string, val []string) { cfg.Runtime.SchedulerTriggers = val }, "STATEFLUX_RUNTIME_SCHEDULER_TRIGGERS")
	envBool(func(key string, val bool) { cfg.Runtime.EnableCollector = val }, "STATEFLUX_RUNTIME_ENABLE_COLLECTOR")
	envBool(func(key string, val bool) { cfg.Runtime.EnableReconcile = val }, "STATEFLUX_RUNTIME_ENABLE_RECONCILE")
	envInt(func(key string, val int) { cfg.Runtime.ClaimBatch = val }, "STATEFLUX_RUNTIME_CLAIM_BATCH")
	envDuration(func(key string, val time.Duration) { cfg.Runtime.TickInterval = val }, "STATEFLUX_RUNTIME_TICK_INTERVAL")
	envDuration(func(key string, val time.Duration) { cfg.Runtime.PromoteInterval = val }, "STATEFLUX_RUNTIME_PROMOTE_INTERVAL")
	envDuration(func(key string, val time.Duration) { cfg.Runtime.ReconcileInterval = val }, "STATEFLUX_RUNTIME_RECONCILE_INTERVAL")
	envDuration(func(key string, val time.Duration) { cfg.Runtime.DispatchGrace = val }, "STATEFLUX_RUNTIME_DISPATCH_GRACE")
	envStrings(func(key string, val []string) { cfg.Runtime.WorkerQueues = val }, "STATEFLUX_RUNTIME_WORKER_QUEUES")
	envDuration(func(key string, val time.Duration) { cfg.Runtime.FactoryInterval = val }, "STATEFLUX_RUNTIME_FACTORY_INTERVAL")

	envString(func(key, val string) { cfg.Dispatch.DefaultSemantics = Semantics(val) }, "STATEFLUX_DISPATCH_DEFAULT_SEMANTICS")
	envString(func(key, val string) { cfg.Dispatch.DefaultDelivery = Delivery(val) }, "STATEFLUX_DISPATCH_DEFAULT_DELIVERY")
	envBool(func(key string, val bool) { cfg.Dispatch.Forward = val }, "STATEFLUX_DISPATCH_FORWARD")
	envInt(func(key string, val int) { cfg.Dispatch.MaxHops = val }, "STATEFLUX_DISPATCH_MAX_HOPS")
	envDuration(func(key string, val time.Duration) { cfg.Dispatch.Timeout = val }, "STATEFLUX_DISPATCH_TIMEOUT")
	envDuration(func(key string, val time.Duration) { cfg.Dispatch.DedupeWindow = val }, "STATEFLUX_DISPATCH_DEDUPE_WINDOW")

	envString(func(key, val string) { cfg.Coherence.QueuePrefix = val }, "STATEFLUX_COHERENCE_QUEUE_PREFIX")
	envDuration(func(key string, val time.Duration) { cfg.Coherence.SyncInterval = val }, "STATEFLUX_COHERENCE_SYNC_INTERVAL")
	envDuration(func(key string, val time.Duration) { cfg.Coherence.RevisionTTL = val }, "STATEFLUX_COHERENCE_REVISION_TTL")

	envBool(func(key string, val bool) { cfg.Obs.Enabled = val }, "STATEFLUX_OBS_ENABLED")
	envString(func(key, val string) { cfg.Obs.ServiceName = val }, "STATEFLUX_OBS_SERVICE_NAME")
	envString(func(key, val string) { cfg.Obs.OTLPEndpoint = val }, "STATEFLUX_OBS_OTLP_ENDPOINT")
	envBool(func(key string, val bool) { cfg.Obs.Insecure = val }, "STATEFLUX_OBS_INSECURE")

	return cfg
}

func envString(set func(key, val string), key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		set(key, v)
	}
}

func envInt(set func(key string, val int), key string) {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			set(key, n)
		}
	}
}

func envBool(set func(key string, val bool), key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			set(key, b)
		}
	}
}

func envStrings(set func(key string, val []string), key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		items := strings.Split(v, ",")
		out := make([]string, 0, len(items))
		for _, item := range items {
			if s := strings.TrimSpace(item); s != "" {
				out = append(out, s)
			}
		}
		set(key, out)
	}
}

func envDuration(set func(key string, val time.Duration), key string) {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			set(key, d)
		}
	}
}
