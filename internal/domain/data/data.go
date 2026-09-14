package data

import (
	"context"
	"fmt"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/cenkalti/backoff/v5"
	"github.com/go-faster/errors"
	"github.com/redis/go-redis/v9"

	// pq 驱动：注册 database/sql 的 "postgres" driver（§3.1）。
	_ "github.com/lib/pq"

	"github.com/improvtrace/stateflux/internal/config"
	"github.com/improvtrace/stateflux/internal/domain/data/ent"
	"github.com/improvtrace/stateflux/internal/domain/repository"
)

// CloseTimeout 是 Data 关闭时的默认等待上限，避免退出被慢连接挂住。
const CloseTimeout = 5 * time.Second

// maxWatchRetries 是 Redis 乐观事务（Watch）失败后的最大重试次数。
const maxWatchRetries = 5

type tranCtxKey struct{}

func NewTransaction(data *Data) repository.Transaction {
	return data
}

// Data 是数据源基建的组合根：PG（ent client，任务账本唯一权威存储，§3.1）与
// Redis（best-effort 通信实现，§3.2）的客户端装配。双栈都在本包使用，不设显式
// pg/redis 子包。访问经 Ent()/Redis() 方法，字段不对外暴露。
type Data struct {
	// db 是 PG 的 ent client：全部仓储实现经它读写（§3.1）。
	db *ent.Client
	// redis 是 Redis 客户端（单机或 cluster，由配置的地址数决定，§3.2/§8）。
	redis redis.UniversalClient
}

// DB 返回 PG 的 ent client：仓储构造器（NewTaskPendings 等）的依赖来源。
func (d *Data) DB() *ent.Client { return d.db }

// Redis 返回 Redis 客户端。
func (d *Data) Redis() redis.UniversalClient { return d.redis }

// Open 按 config 初始化 PG 与 Redis 客户端。不做周期性健康检查——连接质量由
// 驱动连接池与使用期错误暴露。
func Open(ctx context.Context, cfg config.Config) (*Data, func(), error) {
	// lib/pq 注册的 driver 名为 "postgres"；连接池参数经 Driver.DB() 设置。
	drv, err := entsql.Open(dialect.Postgres, cfg.PG.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("data: open pg: %w", err)
	}
	if sqldb := drv.DB(); sqldb != nil {
		sqldb.SetMaxOpenConns(cfg.PG.MaxOpenConns)
		sqldb.SetMaxIdleConns(cfg.PG.MaxIdleConns)
		sqldb.SetConnMaxLifetime(cfg.PG.ConnMaxLifetime)
		sqldb.SetConnMaxIdleTime(cfg.PG.ConnMaxIdleTime)
	}

	rdb, err := openRedis(cfg.Redis)
	if err != nil {
		_ = drv.Close()
		return nil, nil, fmt.Errorf("data: open redis: %w", err)
	}

	return &Data{
			db:    ent.NewClient(ent.Driver(drv)),
			redis: rdb,
		}, func() {
			_ = drv.Close()
			_ = rdb.Close()
		}, nil
}

// openRedis 按地址数选择单机或 cluster 客户端：一个地址走单机，多个走 cluster。
func openRedis(cfg config.Redis) (redis.UniversalClient, error) {
	switch len(cfg.Addrs) {
	case 0:
		return nil, fmt.Errorf("data: redis addrs is empty")
	case 1:
		return redis.NewClient(&redis.Options{
			Addr:         cfg.Addrs[0],
			Password:     cfg.Password,
			DB:           cfg.DB,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
		}), nil
	default:
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addrs,
			Password:     cfg.Password,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
		}), nil
	}
}

// WithTx 返回一个以事务 client 为底层的数据视图：跨表挪行（晋升/认领/终态）必须
// 同事务，事务内的仓储实例经它构造（§5.2/§5.5）。
func (d *Data) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := d.db.Tx(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		if v := recover(); v != nil {
			tx.Rollback()
			panic(v)
		}
	}()
	ctx = context.WithValue(ctx, tranCtxKey{}, tx)
	if err := fn(ctx); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = errors.Wrapf(err, "rolling back transaction: %v", rerr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "committing transaction: %v", err)
	}
	return nil
}

// retirableWatch 以指数退避重试 Redis 乐观事务：仅在 TxFailedErr（并发冲突）时重试，
// 其他错误立即终止并返回。退避参数：500ms 起、5s 封顶（cenkalti/backoff/v5）。
func (d *Data) retirableWatch(ctx context.Context, fn func(tx *redis.Tx) error, keys ...string) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 500 * time.Millisecond
	b.MaxInterval = 5 * time.Second

	op := func() (struct{}, error) {
		err := d.redis.Watch(ctx, fn, keys...)
		if err != nil && err != redis.TxFailedErr {
			return struct{}{}, backoff.Permanent(err) // 非竞争错误不重试
		}
		return struct{}{}, err
	}
	_, err := backoff.Retry(ctx, op,
		backoff.WithBackOff(b),
		backoff.WithMaxTries(maxWatchRetries))
	return err
}
