// Package config 加载 stateflux、datamigrate 与 mock-dm 的配置。
//
// 配置来源只有两个：配置文件（ini，完整示例见 config.conf.example）与命令行
// 参数。优先级从低到高：默认值 → 配置文件 → 命令行参数（显式给出才生效）。
// 不读取环境变量。
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	ini "github.com/go-viper/encoding/ini"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config 是配置总表，一段 ini 对应一个结构。
type Config struct {
	Postgres Postgres `mapstructure:"postgres"`
	Redis    Redis    `mapstructure:"redis"`
	DM       DM       `mapstructure:"dm"`
	Schedule Schedule `mapstructure:"schedule"`
}

// Postgres 是 PostgreSQL 连接配置（[postgres]），stateflux 与 datamigrate 共用。
type Postgres struct {
	// DSN 数据源名称，URL 形式：postgres://user:pass@host:5432/db?sslmode=disable。
	DSN string `mapstructure:"dsn"`
}

// Redis 是 Redis 连接配置（[redis]）：队列、账本与定时锁共用。
type Redis struct {
	// Addrs 节点地址，多个以英文逗号间隔；首个地址可带认证，格式
	// user:password@host。
	Addrs []string `mapstructure:"addrs"`
	// DB 库号。
	DB int `mapstructure:"db"`
}

// DM 是 dm 服务配置（[dm]）：dispatch 经它查询集群节点表，调度器启动时也经
// 它获取本节点 ID；mock-dm 以 Address 为监听地址、ServiceKey 为校验密钥，
// 在本地顶替 dm。
type DM struct {
	// Address dm 服务的 unix socket 监听地址。
	Address string `mapstructure:"address"`
	// ServiceKey dm 服务内部接口的服务密钥。
	ServiceKey string `mapstructure:"service_key"`
}

// Schedule 是调度器配置（[schedule]）。本节点标识不在配置里：stateflux 启动
// 时经 dm 服务的 ListClusterNodesStatus 获取（响应的 node_id）。
type Schedule struct {
	// ReconcileInterval 对账间隔。
	ReconcileInterval time.Duration `mapstructure:"reconcile_interval"`
}

// key 描述一个配置项。这张表是配置项的唯一登记处：ini 的键名、命令行参数
// 与默认值都由它派生——新增配置项只需在此加一行。
type key struct {
	name  string // 配置键，即 ini 的「段.键」
	flag  string // 命令行参数名
	def   any    // 默认值；nil 表示必填项，须由配置文件或命令行参数提供
	usage string
}

var keys = []key{
	{"postgres.dsn", "pg-dsn", nil, "PostgreSQL DSN (postgres://user:pass@host:5432/db?sslmode=disable)"},
	{"redis.addrs", "redis-addrs", []string{"127.0.0.1:6379"}, "Redis node addresses, comma-separated or repeated; the first may carry credentials as user:password@host"},
	{"redis.db", "redis-db", 0, "Redis database number"},
	{"dm.address", "dm-address", nil, "dm service unix socket path (dispatch queries the cluster node table and the local node id through it)"},
	{"dm.service_key", "dm-service-key", nil, "dm service internal API key"},
	{"schedule.reconcile_interval", "reconcile-interval", 30 * time.Second, "reconciliation interval"},
}

// section 返回配置键所属的段。
func (k key) section() string { return k.name[:strings.IndexByte(k.name, '.')] }

// RegisterFlags 把配置项注册为命令行参数；sections 限定注册哪些段（如
// "postgres"），为空时注册全部。--config 由命令自行注册。
func RegisterFlags(fs *pflag.FlagSet, sections ...string) {
	for _, k := range keys {
		if len(sections) > 0 && !slices.Contains(sections, k.section()) {
			continue
		}
		switch def := k.def.(type) {
		case nil:
			fs.String(k.flag, "", k.usage+" (required)")
		case string:
			fs.String(k.flag, def, k.usage)
		case []string:
			fs.StringSlice(k.flag, def, k.usage)
		case int:
			fs.Int(k.flag, def, k.usage)
		case time.Duration:
			fs.Duration(k.flag, def, k.usage)
		default:
			panic(fmt.Sprintf("config: unsupported default type %T for key %s", def, k.name))
		}
	}
}

// Load 装配配置并解码为 Config。configFile 为空时不读配置文件；fs 是经
// RegisterFlags 注册过的参数集（可为 nil），只有显式给出的参数才覆盖配置
// 文件与默认值。
func Load(configFile string, fs *pflag.FlagSet) (Config, error) {
	v := newViper()
	if configFile != "" {
		data, err := os.ReadFile(configFile)
		if err != nil {
			return Config{}, fmt.Errorf("config: read config file: %w", err)
		}
		v.SetConfigType("ini")
		if err := v.ReadConfig(bytes.NewReader(data)); err != nil {
			return Config{}, fmt.Errorf("config: parse %s: %w", configFile, err)
		}
		// 此时 v 里只有配置文件的键
		var unknown []string
		for _, k := range v.AllKeys() {
			if !slices.ContainsFunc(keys, func(item key) bool { return item.name == k }) {
				unknown = append(unknown, k)
			}
		}
		if len(unknown) > 0 {
			slices.Sort(unknown)
			return Config{}, fmt.Errorf("config: %s contains unknown keys: %s (see config.conf.example for the full list)",
				configFile, strings.Join(unknown, ", "))
		}
	}
	for _, k := range keys {
		def := k.def
		if def == nil {
			def = "" // 必填项：没有默认值，留空交由 Validate 报错
		}
		v.SetDefault(k.name, def)
		if fs != nil && fs.Lookup(k.flag) != nil {
			_ = v.BindPFlag(k.name, fs.Lookup(k.flag))
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: decode: %w", err)
	}
	return cfg, nil
}

// newViper 返回支持 ini 的 viper 实例：viper 自 v1.21 起把 ini 编解码拆到了
// 独立模块，需显式注册。这里不调用 AutomaticEnv——配置不来自环境变量。
func newViper() *viper.Viper {
	codecs := viper.NewCodecRegistry()
	_ = codecs.RegisterCodec("ini", ini.Codec{})
	return viper.NewWithOptions(viper.WithCodecRegistry(codecs))
}

// Validate 校验 PostgreSQL 连接配置；datamigrate 只用到这一段。
func (p Postgres) Validate() error {
	if p.DSN == "" {
		return errors.New("postgres.dsn must not be empty")
	}
	return nil
}

// Validate 校验 dm 服务配置；mock-dm 只用到这一段。
func (d DM) Validate() error {
	if d.Address == "" || d.ServiceKey == "" {
		return errors.New("dm.address and dm.service_key must not be empty")
	}
	return nil
}

// Validate 校验全量配置。服务启动前执行，让错误配置在启动即失败，而不是
// 运行期才暴露。
func (c Config) Validate() error {
	var errs []string
	if err := c.Postgres.Validate(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(c.Redis.Addrs) == 0 {
		errs = append(errs, "redis.addrs must not be empty (comma-separated address list)")
	}
	if err := c.DM.Validate(); err != nil {
		errs = append(errs, err.Error())
	}
	if c.Schedule.ReconcileInterval <= 0 {
		errs = append(errs, "schedule.reconcile_interval must be positive")
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}
