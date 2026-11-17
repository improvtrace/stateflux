package config

import (
	"strings"
	"testing"
	"time"

	"github.com/improvtrace/stateflux/internal/task"
)

// Default() 必须能通过自身校验——否则所有子命令无法用默认配置启动。
func TestValidateDefaultOK(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("Default().Validate(): %v", err)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"empty pg dsn", func(c *Config) { c.PG.DSN = "" }, "pg.dsn"},
		{"empty redis addrs", func(c *Config) { c.Redis.Addrs = nil }, "redis.addrs"},
		{"empty cluster dsn", func(c *Config) { c.Cluster.DSN = "" }, "cluster.dsn"},
		{"bad semantics", func(c *Config) { c.Dispatch.DefaultSemantics = task.Semantics("sometimes") }, "default_semantics"},
		{"bad delivery", func(c *Config) { c.Dispatch.DefaultDelivery = task.Delivery("carrier_pigeon") }, "default_delivery"},
		{"zero grace", func(c *Config) { c.Runtime.DispatchGrace = 0 }, "dispatch_grace"},
		{"zero tick", func(c *Config) { c.Runtime.TickInterval = 0 }, "tick_interval"},
		{"negative factory", func(c *Config) { c.Runtime.FactoryInterval = -time.Second }, "factory_interval"},
		{"unknown trigger", func(c *Config) { c.Runtime.SchedulerTriggers = []string{"tick", "cron"} }, "scheduler_triggers"},
		{"zero max hops", func(c *Config) { c.Dispatch.MaxHops = 0 }, "max_hops"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			tc.mut(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// FromViper 集成校验：环境变量注入的非法值必须在装配期被拒。
func TestFromViperRejectsInvalidEnv(t *testing.T) {
	t.Setenv("STATEFLUX_DISPATCH_DEFAULT_SEMANTICS", "whenever")
	v, err := NewViper("")
	if err != nil {
		t.Fatalf("NewViper: %v", err)
	}
	if _, err := FromViper(v); err == nil {
		t.Fatal("FromViper with invalid semantics: want error, got nil")
	}
}
