package queue

import (
	"crypto/tls"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 3600
)

// Config holds all configuration for a machinery server. Construct it in code
// and pass it to NewServer together with a broker, a backend and a lock.
type Config struct {
	// AppName namespaces the periodic-task lock keys (DefaultQueue is the
	// redis queue, AppName identifies the deploying application). Defaults
	// to the binary name when empty.
	AppName string
	// DefaultQueue is the redis list tasks are published to when no custom
	// queue is set. Required.
	DefaultQueue    string
	ResultsExpireIn int
	Redis           *RedisConfig
	TLSConfig       *tls.Config
	// NoUnixSignals - when set disables signal handling in machinery
	NoUnixSignals bool
}

// RedisConfig holds redis-specific configuration for broker, backend and lock.
type RedisConfig struct {
	// NormalTasksPollPeriod specifies the period in milliseconds when polling redis for normal tasks
	// Default: 1000
	NormalTasksPollPeriod int

	// DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
	// Default: 500
	DelayedTasksPollPeriod int

	// DelayedTasksKey specifies the sorted-set key used to store delayed tasks
	// Default: delayed_tasks
	DelayedTasksKey string

	// MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
	MasterName string

	// ClusterEnabled specifies whether cluster mode is enabled, regardless the number of addresses.
	// This helps create ClusterClient for Redis servers that enabled cluster mode with 1 node, or using AWS configuration endpoint
	ClusterEnabled bool

	// SentinelPassword specifies the password to be used when connecting to a Redis server via Sentinel
	SentinelPassword string
}
