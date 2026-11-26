package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

// flags 构造注册了指定各段参数的参数集，args 是显式给出的命令行参数。
func flags(t *testing.T, sections []string, args ...string) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs, sections...)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("解析参数 %v: %v", args, err)
	}
	return fs
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stateflux.conf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const fileConfig = `[postgres]
dsn = postgres://file:file@db:5432/file

[redis]
addrs = 10.0.0.1:6379,10.0.0.2:6379
db = 5

[dm]
address = /run/dm.sock
service_key = file-key

[schedule]
reconcile_interval = 5s
`

var fileWant = Config{
	Postgres: Postgres{DSN: "postgres://file:file@db:5432/file"},
	Redis:    Redis{Addrs: []string{"10.0.0.1:6379", "10.0.0.2:6379"}, DB: 5},
	DM:       DM{Address: "/run/dm.sock", ServiceKey: "file-key"},
	Schedule: Schedule{ReconcileInterval: 5 * time.Second},
}

// TestExample 断言示例配置能被解析并通过校验，且各段取值符合书写。
func TestExample(t *testing.T) {
	cfg, err := Load("config.conf.example", nil)
	if err != nil {
		t.Fatalf("Load(config.conf.example): %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("示例配置应通过校验: %v", err)
	}
	want := Config{
		Postgres: Postgres{DSN: "postgres://postgres:postgres@127.0.0.1:5432/stateflux?sslmode=disable"},
		Redis:    Redis{Addrs: []string{"127.0.0.1:6379"}},
		DM:       DM{Address: "/tmp/pam/dm.sock", ServiceKey: "change-me"},
		Schedule: Schedule{ReconcileInterval: 30 * time.Second},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("示例配置解码不符:\n got: %+v\nwant: %+v", cfg, want)
	}
}

// TestLoadPrecedence 断言取值优先级：默认值 → 配置文件 → 显式给出的命令行参数。
func TestLoadPrecedence(t *testing.T) {
	file := writeFile(t, fileConfig)

	t.Run("只有默认值", func(t *testing.T) {
		cfg, err := Load("", nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		// 必填项没有默认值；其余取默认值
		if cfg.Postgres.DSN != "" || cfg.DM.Address != "" || cfg.DM.ServiceKey != "" {
			t.Fatalf("必填项不应有默认值: %+v", cfg)
		}
		if !reflect.DeepEqual(cfg.Redis, Redis{Addrs: []string{"127.0.0.1:6379"}}) || cfg.Schedule.ReconcileInterval != 30*time.Second {
			t.Fatalf("默认值不符: %+v", cfg)
		}
		if err := cfg.Validate(); err == nil {
			t.Fatal("缺少必填项时应校验失败")
		}
	})

	t.Run("配置文件", func(t *testing.T) {
		cfg, err := Load(file, nil)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !reflect.DeepEqual(cfg, fileWant) {
			t.Fatalf("配置文件解码不符:\n got: %+v\nwant: %+v", cfg, fileWant)
		}
	})

	t.Run("参数优先于配置文件", func(t *testing.T) {
		fs := flags(t, nil,
			"--pg-dsn", "postgres://flag:flag@db:5432/flag",
			"--redis-addrs", "10.9.9.9:6379", "--redis-addrs", "10.9.9.8:6379",
			"--redis-db", "7", "--reconcile-interval", "1m")
		cfg, err := Load(file, fs)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		want := fileWant
		want.Postgres.DSN = "postgres://flag:flag@db:5432/flag"
		want.Redis = Redis{Addrs: []string{"10.9.9.9:6379", "10.9.9.8:6379"}, DB: 7}
		want.Schedule.ReconcileInterval = time.Minute
		if !reflect.DeepEqual(cfg, want) {
			t.Fatalf("参数未正确覆盖:\n got: %+v\nwant: %+v", cfg, want)
		}
	})

	t.Run("未给出的参数不覆盖配置文件", func(t *testing.T) {
		cfg, err := Load(file, flags(t, nil))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !reflect.DeepEqual(cfg, fileWant) {
			t.Fatalf("参数默认值不应覆盖配置文件:\n got: %+v\nwant: %+v", cfg, fileWant)
		}
	})
}

// TestPostgresFlagsOnly 断言只注册部分段的参数时，其余段仍能从配置文件读取。
func TestPostgresFlagsOnly(t *testing.T) {
	fs := flags(t, []string{"postgres"}, "--pg-dsn", "postgres://flag@db/flag")
	cfg, err := Load(writeFile(t, fileConfig), fs)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Postgres.DSN != "postgres://flag@db/flag" {
		t.Fatalf("postgres.dsn: got %q", cfg.Postgres.DSN)
	}
	if err := cfg.Postgres.Validate(); err != nil {
		t.Fatalf("Postgres.Validate: %v", err)
	}
	if !reflect.DeepEqual(cfg.Redis, fileWant.Redis) {
		t.Fatalf("未注册参数的段应取配置文件: got %+v", cfg.Redis)
	}
}

// TestLoadUnknownKey 断言配置文件里的未知键（拼写错误、放错段）被拒绝，而不是
// 被静默忽略。
func TestLoadUnknownKey(t *testing.T) {
	cases := map[string]string{
		"shcedule.node_id": "[postgres]\ndsn = x\n[shcedule]\nnode_id = oops\n", // 段名拼错
		"schedule.node_id": "[postgres]\ndsn = x\n[schedule]\nnode_id = old\n",  // 已移除：节点 ID 改由 dm 提供
		"node_id":          "[postgres]\ndsn = x\nnode_id = no-section\n",       // 不在任何段内
		"redis.password":   "[redis]\npassword = not-supported\n",               // 不支持的键
	}
	for want, content := range cases {
		_, err := Load(writeFile(t, content), nil)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("应报未知配置项 %q:\n配置:\n%s\nerr: %v", want, content, err)
		}
	}
}

// TestLoadIgnoresEnv 断言配置不读取环境变量。
func TestLoadIgnoresEnv(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env:env@db:5432/env")
	t.Setenv("PG_DSN", "postgres://env:env@db:5432/env")
	t.Setenv("STATEFLUX_POSTGRES_DSN", "postgres://env:env@db:5432/env")
	t.Setenv("REDIS_ADDRS", "env:6379")
	t.Setenv("REDIS_DB", "9")

	cfg, err := Load("", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Postgres.DSN != "" || !reflect.DeepEqual(cfg.Redis, Redis{Addrs: []string{"127.0.0.1:6379"}}) {
		t.Fatalf("配置不应来自环境变量: %+v", cfg)
	}
}

func TestValidate(t *testing.T) {
	missing := Config{}
	err := missing.Validate()
	if err == nil {
		t.Fatal("空配置应校验失败")
	}
	for _, want := range []string{"postgres.dsn", "redis.addrs", "dm.address", "schedule.reconcile_interval"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("校验信息应包含 %q: %v", want, err)
		}
	}
	if err := fileWant.Validate(); err != nil {
		t.Fatalf("完整配置应通过校验: %v", err)
	}
	if err := (Config{}).Postgres.Validate(); err == nil {
		t.Fatal("Postgres.Validate 对空 DSN 应报错")
	}
}

// TestLoadMissingFile 断言配置文件不存在时报错。
func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.conf"), nil); err == nil {
		t.Fatal("配置文件不存在时应报错")
	}
}
