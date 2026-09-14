package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是服务总配置（§8）：flag/env 解析后填充，缺省值由 Default 提供。
type Config struct {
	PG    PG
	Redis Redis
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

// Default 返回全量默认配置。
func Default() Config {
	return Config{
		PG: PG{
			DSN:             "postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable",
			MaxOpenConns:    30,
			MaxIdleConns:    10,
			ConnMaxLifetime: 30 * time.Minute,
			ConnMaxIdleTime: 5 * time.Minute,
		},
		Redis: Redis{
			Addrs:        []string{"127.0.0.1:6379"},
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
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
	envStrings(func(key string, val []string) { cfg.Redis.Addrs = val }, "STATEFLUX_REDIS_ADDRS")
	envString(func(key, val string) { cfg.Redis.Password = val }, "STATEFLUX_REDIS_PASSWORD")
	envInt(func(key string, val int) { cfg.Redis.DB = val }, "STATEFLUX_REDIS_DB")
	envDuration(func(key string, val time.Duration) { cfg.Redis.DialTimeout = val }, "STATEFLUX_REDIS_DIAL_TIMEOUT")
	envDuration(func(key string, val time.Duration) { cfg.Redis.ReadTimeout = val }, "STATEFLUX_REDIS_READ_TIMEOUT")
	envDuration(func(key string, val time.Duration) { cfg.Redis.WriteTimeout = val }, "STATEFLUX_REDIS_WRITE_TIMEOUT")
	envInt(func(key string, val int) { cfg.Redis.PoolSize = val }, "STATEFLUX_REDIS_POOL_SIZE")
	envInt(func(key string, val int) { cfg.Redis.MinIdleConns = val }, "STATEFLUX_REDIS_MIN_IDLE_CONNS")
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

func envStrings(set func(key string, val []string), key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		addrs := strings.Split(v, ",")
		for i := range addrs {
			addrs[i] = strings.TrimSpace(addrs[i])
		}
		set(key, addrs)
	}
}

func envDuration(set func(key string, val time.Duration), key string) {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			set(key, d)
		}
	}
}
