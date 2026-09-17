package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config 是服务总配置（§8、§15.1#3）：flag/env 解析后填充，缺省值由 Default 提供。
type Config struct {
	PG        PG        `mapstructure:"pg"`
	Redis     Redis     `mapstructure:"redis"`
	Cluster   Cluster   `mapstructure:"cluster"`
	Server    Server    `mapstructure:"server"`
	Runtime   Runtime   `mapstructure:"runtime"`
	Dispatch  Dispatch  `mapstructure:"dispatch"`
	Coherence Coherence `mapstructure:"coherence"`
	Obs       Obs       `mapstructure:"obs"`
}

// PG 是 PostgreSQL 连接配置（§3.1：任务账本唯一权威存储）。
type PG struct {
	// DSN 数据源名称，如 postgres://user:pass@host:5432/db?sslmode=disable。
	DSN string `mapstructure:"dsn"`
	// MaxOpenConns 最大打开连接数。
	MaxOpenConns int `mapstructure:"max_open_conns"`
	// MaxIdleConns 最大空闲连接数。
	MaxIdleConns int `mapstructure:"max_idle_conns"`
	// ConnMaxLifetime 连接最长复用时间。
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	// ConnMaxIdleTime 空闲连接最长保留时间。
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
	// SearchPath 是连接建立后固定的 search_path（默认 public）：避免「用户名与 schema 同名」
	// 时 PostgreSQL 的 "$user" 隐式 schema 把表解析到错误位置（§3.1）。
	SearchPath string `mapstructure:"search_path"`
}

// Redis 是 Redis 连接配置（§3.2：仅作为 best-effort 通信实现，正确性不依赖 Redis）。
type Redis struct {
	// Addrs 节点地址列表：单个地址走单机客户端，多个地址走 cluster 客户端。
	Addrs        []string      `mapstructure:"addrs"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	PoolSize     int           `mapstructure:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns"`
}

// ClusterScheme 是集群视图 DSN 的模式（§15.1#3）：local / http / https / grpc。
type ClusterScheme string

const (
	// ClusterSchemeGRPC 经 gRPC 调用外部 ClusterService。
	ClusterSchemeGRPC ClusterScheme = "grpc"
	// ClusterSchemeHTTP 经 HTTP/JSON 调用外部 ClusterService（网关式部署，明文）。
	ClusterSchemeHTTP ClusterScheme = "http"
	// ClusterSchemeHTTPS 经 HTTPS/JSON 调用外部 ClusterService（网关式部署，TLS）。
	ClusterSchemeHTTPS ClusterScheme = "https"
	// ClusterSchemeLocal 单节点部署：节点清单与 leader 位置直接来自本进程配置，不访问外部系统。
	ClusterSchemeLocal ClusterScheme = "local"
)

// 集群 DSN 的默认参数（全部通过 URL query 覆盖）。
const (
	// DefaultClusterTimeout 单次拉取超时（query: timeout）。
	DefaultClusterTimeout = 10 * time.Second
	// DefaultClusterConnectTimeout 连接超时（query: connect_timeout）。
	DefaultClusterConnectTimeout = 30 * time.Second
	// DefaultClusterInterval 拉取间隔（query: interval）。
	DefaultClusterInterval = 3 * time.Second
)

// Cluster 是集群视图的连接配置（§15.1#3），归入 Stateflux.Config.Cluster，仅保留 DSN：
//
//	grpc://host:port?timeout=10s&connect_timeout=30s&interval=3s
//	http(s)://host[:port][/path]?timeout=10s&connect_timeout=30s&interval=3s
//	local://localhost?node_id=n1&address=127.0.0.1:9090
//
// 声明的 DSN 参数：
//   - timeout:         单次拉取超时，默认 10s；
//   - connect_timeout: 连接超时，默认 30s；
//   - interval:        拉取间隔，默认 3s（显式 0 表示只按需拉取）。
//
// 时长值支持 Go duration 语法（10s / 500ms / 0）或无单位秒数（如 timeout=5）。
// local 表示单节点部署，其余 query 参数（node_id / address 等）按模式各自解释。
type Cluster struct {
	// DSN 集群视图数据源名称；空等价于 local://localhost。
	DSN string `mapstructure:"dsn"`
}

// ClusterOptions 是 ParseClusterDSN 的解析结果：连接与调用参数的单一来源，
// 由 cluster 视图客户端（grpc / http(s) / local）与缓存刷新循环共同消费。
type ClusterOptions struct {
	// Scheme 解析出的模式（local / http / https / grpc）。
	Scheme ClusterScheme
	// Host 目标地址（host[:port]，来自 DSN authority；local 模式忽略）。
	Host string
	// Timeout 单次拉取超时（每次 Get 调用的预算）。
	Timeout time.Duration
	// ConnectTimeout 连接建立超时（grpc 连接参数 / http 拨号超时）。
	ConnectTimeout time.Duration
	// Interval 周期拉取间隔（缓存后台刷新；0 表示只按需拉取）。
	Interval time.Duration
	// Params 全部 query 参数（key 统一小写）。
	Params map[string]string
}

// Param 返回 query 参数值；不存在返回空串。
func (o ClusterOptions) Param(key string) string { return o.Params[strings.ToLower(key)] }

// ParseClusterDSN 把集群 DSN 解析为 ClusterOptions：模式来自 scheme，
// 连接与调用参数来自 URL query（timeout / connect_timeout / interval）。
func ParseClusterDSN(dsn string) (ClusterOptions, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		dsn = string(ClusterSchemeLocal) + "://localhost"
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return ClusterOptions{}, fmt.Errorf("config: parse cluster dsn %q: %w", dsn, err)
	}
	o := ClusterOptions{
		Scheme:         ClusterScheme(strings.ToLower(u.Scheme)),
		Timeout:        DefaultClusterTimeout,
		ConnectTimeout: DefaultClusterConnectTimeout,
		Interval:       DefaultClusterInterval,
		Params:         make(map[string]string),
	}
	for k, vs := range u.Query() {
		if len(vs) > 0 {
			o.Params[strings.ToLower(k)] = vs[0]
		}
	}
	for _, spec := range []struct {
		key string
		out *time.Duration
	}{
		{"timeout", &o.Timeout},
		{"connect_timeout", &o.ConnectTimeout},
		{"interval", &o.Interval},
	} {
		if err := durationParam(o.Params, spec.key, spec.out); err != nil {
			return ClusterOptions{}, err
		}
	}
	switch o.Scheme {
	case ClusterSchemeGRPC, ClusterSchemeHTTP, ClusterSchemeHTTPS:
		if u.Host == "" {
			return ClusterOptions{}, fmt.Errorf("config: cluster dsn %q: scheme %s requires host", dsn, o.Scheme)
		}
		o.Host = u.Host
	case ClusterSchemeLocal:
		// 单节点部署：不强制 host；节点身份为内置默认值（见 cluster.DefaultLocalNode）。
		o.Host = u.Host
	default:
		return ClusterOptions{}, fmt.Errorf("config: cluster dsn %q: unknown scheme %q (want local|http|https|grpc)", dsn, u.Scheme)
	}
	return o, nil
}

// durationParam 解析时长 query 参数：支持 Go duration 语法（10s / 500ms / 0）
// 与无单位秒数（如 timeout=5 视为 5s）。
func durationParam(params map[string]string, key string, out *time.Duration) error {
	v, ok := params[key]
	if !ok || v == "" {
		return nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		*out = d
		return nil
	}
	secs, err := strconv.ParseFloat(v, 64)
	if err != nil || secs < 0 {
		return fmt.Errorf("config: cluster dsn param %s=%q: invalid duration", key, v)
	}
	*out = time.Duration(secs * float64(time.Second))
	return nil
}

// Server 是服务监听配置。
type Server struct {
	// GRPCAddr gRPC 监听地址（ExecutorService / DispatchService / CoherenceService /
	// ForwardService / CapabilityService 共用）。
	GRPCAddr string `mapstructure:"grpc_addr"`
	// HTTPAddr 健康检查与诊断 HTTP 监听地址。
	HTTPAddr string `mapstructure:"http_addr"`
	// ShutdownTimeout 优雅退出等待上限。
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// Runtime 是控制面运行时开关（§2.1、§15.1#11）。
type Runtime struct {
	// SchedulerSchedulers 调度运行时实例名列表：同一进程可承载多个触发器实例
	// （tick / notify / coherence / manual），空则按内置默认启用 tick。
	SchedulerTriggers []string `mapstructure:"scheduler_triggers"`
	// EnableCollector 是否启用结果归集运行时。
	EnableCollector bool `mapstructure:"enable_collector"`
	// EnableReconcile 是否启用 R1–R4 对账运行时。
	EnableReconcile bool `mapstructure:"enable_reconcile"`
	// ClaimBatch 单次认领批量（§10 默认 500）。
	ClaimBatch int `mapstructure:"claim_batch"`
	// TickInterval 调度 tick（§10 默认 100ms）。
	TickInterval time.Duration `mapstructure:"tick_interval"`
	// PromoteInterval 晋升 tick（§10 默认 200ms）。
	PromoteInterval time.Duration `mapstructure:"promote_interval"`
	// ReconcileInterval R1 扫描间隔。
	ReconcileInterval time.Duration `mapstructure:"reconcile_interval"`
	// DispatchGrace 通道无结果前不重试的最小窗口（§10 默认 90s）。
	DispatchGrace time.Duration `mapstructure:"dispatch_grace"`
	// WorkerQueues 是执行运行时订阅的逻辑队列集合（异步任务 channel 名，§5.3）。
	WorkerQueues []string `mapstructure:"worker_queues"`
	// FactoryInterval 是内置示例工厂的生成周期（§5.6）；<=0 表示不注册工厂。
	FactoryInterval time.Duration `mapstructure:"factory_interval"`
	// WALDir 是结果 WAL 目录（§5.4）；空表示使用进程内 WAL。
	WALDir string `mapstructure:"wal_dir"`
	// WALMaxEntries 是 WAL 高水位条目数（§10 默认 10k）。
	WALMaxEntries int `mapstructure:"wal_max_entries"`
	// WALMaxBytes 是 WAL 高水位字节数（§10 默认 256MB）。
	WALMaxBytes int64 `mapstructure:"wal_max_bytes"`
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
	DefaultSemantics Semantics `mapstructure:"default_semantics"`
	// DefaultDelivery 未显式指定时的投递形态。
	DefaultDelivery Delivery `mapstructure:"default_delivery"`
	// Forward 是否允许节点间转发。
	Forward bool `mapstructure:"forward"`
	// MaxHops 转发跳数上限（环路保护）。
	MaxHops int `mapstructure:"max_hops"`
	// Timeout 单次分发超时。
	Timeout time.Duration `mapstructure:"timeout"`
	// DedupeWindow exactly_once 的入口去重窗口。
	DedupeWindow time.Duration `mapstructure:"dedupe_window"`
	// ResultChannel 是结果归集通道的逻辑名（§5.5）：默认 stream（worker→调度节点 gRPC
	// ResultStream），可替换为 redis-pubsub / redis-list 等。
	ResultChannel string `mapstructure:"result_channel"`
}

// Coherence 是共识信息同步配置（§15.1#4）。
type Coherence struct {
	// QueuePrefix 异步队列名前缀（队列↔节点映射的键空间）。
	QueuePrefix string `mapstructure:"queue_prefix"`
	// SyncInterval 执行节点周期拉取共识信息的间隔。
	SyncInterval time.Duration `mapstructure:"sync_interval"`
	// RevisionTTL 共识 revision 在 redis 的保留时长。
	RevisionTTL time.Duration `mapstructure:"revision_ttl"`
}

// Obs 是观测装配参数（§6.4、§15.1#10）。
type Obs struct {
	// Enabled 是否启用 OTel 导出；false 时 instruments 为 no-op。
	Enabled bool `mapstructure:"enabled"`
	// ServiceName 资源服务名。
	ServiceName string `mapstructure:"service_name"`
	// OTLPEndpoint OTLP 导出地址（grpc）。
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
	// Insecure 是否使用明文 OTLP 连接。
	Insecure bool `mapstructure:"insecure"`
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
		Cluster: Cluster{DSN: "local://localhost"},
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
			WALDir:            ".stateflux-wal",
			WALMaxEntries:     10000,
			WALMaxBytes:       256 << 20,
		},
		Dispatch: Dispatch{
			DefaultSemantics: AtLeastOnce,
			DefaultDelivery:  DeliveryRedisQueue,
			Forward:          true,
			MaxHops:          3,
			Timeout:          10 * time.Second,
			DedupeWindow:     10 * time.Minute,
			ResultChannel:    "stream",
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

// NewViper 返回已装配默认值、环境变量（STATEFLUX_ 前缀，. → _）与可选配置文件的
// viper 实例：Default → 配置文件 → 环境变量 →（由调用方绑定）命令行 flag，后者优先。
// configFile 为空时跳过文件读取，仅用默认值与环境变量。
func NewViper(configFile string) (*viper.Viper, error) {
	v := viper.New()
	setDefaults(v)
	v.SetEnvPrefix("STATEFLUX")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: read %s: %w", configFile, err)
		}
	}
	return v, nil
}

// BindFlags 把命令行 flag 绑定到 viper key：flag 显式给出时覆盖文件与环境变量。
// 绑定关系集中在此维护，避免各命令散落拼写。
func BindFlags(v *viper.Viper, fs *pflag.FlagSet) {
	bind := func(key, name string) { _ = v.BindPFlag(key, fs.Lookup(name)) }
	bind("pg.dsn", "pg-dsn")
	bind("redis.addrs", "redis-addrs")
	bind("cluster.dsn", "cluster-dsn")
	bind("server.grpc_addr", "grpc-addr")
	bind("server.http_addr", "http-addr")
}

// FromViper 把 viper 实例解码为 Config：AllSettings 已合并默认值、配置文件、
// 环境变量与绑定的命令行 flag（优先级从低到高）。
func FromViper(v *viper.Viper) (Config, error) {
	var cfg Config
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           &cfg,
		TagName:          "mapstructure",
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(",")),
	})
	if err != nil {
		return Default(), err
	}
	if err := dec.Decode(v.AllSettings()); err != nil {
		return Default(), fmt.Errorf("config: decode: %w", err)
	}
	return cfg, nil
}

// FromEnv 在 Default 之上应用环境变量覆盖（STATEFLUX_ 前缀）；
// 保留给纯环境变量部署形态与测试使用。
func FromEnv() Config {
	v, err := NewViper("")
	if err != nil {
		return Default()
	}
	cfg, err := FromViper(v)
	if err != nil {
		return Default()
	}
	return cfg
}

// setDefaults 把 Default() 展开成小写点分 key 逐项注册：每个 key 都有默认值后，
// viper 的 AutomaticEnv 才能在 Unmarshal 时按 key 命中环境变量。
func setDefaults(v *viper.Viper) {
	raw, err := toMap(Default())
	if err != nil {
		return
	}
	flatten("", raw, v)
}

// toMap 按 mapstructure tag 把 Config 编码为嵌套 map。
func toMap(cfg Config) (map[string]any, error) {
	var raw map[string]any
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:  &raw,
		TagName: "mapstructure",
	})
	if err != nil {
		return nil, err
	}
	if err := dec.Decode(cfg); err != nil {
		return nil, err
	}
	return raw, nil
}

// flatten 把嵌套 map 展开为点分 key 注册进 viper。
func flatten(prefix string, m map[string]any, v *viper.Viper) {
	for k, val := range m {
		key := strings.ToLower(k)
		if prefix != "" {
			key = prefix + "." + key
		}
		if nested, ok := val.(map[string]any); ok {
			flatten(key, nested, v)
			continue
		}
		v.SetDefault(key, val)
	}
}
