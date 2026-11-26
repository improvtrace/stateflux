package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// NewGoRedisClient builds a redis.UniversalClient (single node, Sentinel failover
// or cluster, depending on config) for injection into NewRedisBrokerWithClient,
// NewRedisBackendWithClient or NewRedisLockWithClient.
// It parses username/password from the first address if present (format: user:password@host).
// Returns nil when addrs is empty; callers must treat a nil client as invalid input.
func NewGoRedisClient(cnf *Config, addrs []string, db int) redis.UniversalClient {
	if len(addrs) == 0 {
		return nil
	}
	// work on a copy so the caller's slice is never mutated
	addrs = append([]string(nil), addrs...)
	var password string
	var username string
	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		options := strings.SplitN(strings.Join(parts[:len(parts)-1], "@"), ":", 2)
		if len(options) >= 2 {
			username = options[0]
			password = options[1]
		} else {
			password = options[0]
		}
		addrs[0] = parts[len(parts)-1]
	}

	ropt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Username: username,
		Password: password,
	}
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
		if cnf.Redis.SentinelPassword != "" {
			ropt.SentinelPassword = cnf.Redis.SentinelPassword
		}
	}
	if cnf.TLSConfig != nil {
		ropt.TLSConfig = cnf.TLSConfig
	}

	if cnf.Redis != nil && cnf.Redis.ClusterEnabled {
		return redis.NewClusterClient(ropt.Cluster())
	}
	return redis.NewUniversalClient(ropt)
}

type Lock interface {
	//Acquire the lock, retrying up to the configured number of times.
	//key: the name of the lock,
	//ttl: how long the lock is held; passing a non-positive ttl is an error
	LockWithRetries(ctx context.Context, key string, ttl time.Duration) error

	//Acquire the lock with a single attempt.
	//key: the name of the lock,
	//ttl: how long the lock is held; passing a non-positive ttl is an error
	Lock(ctx context.Context, key string, ttl time.Duration) error
}

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

// defaultLockRetryInterval is the sleep between lock acquisition attempts.
const defaultLockRetryInterval = 500 * time.Millisecond

// RedisLock is a distributed lock backed by redsync (Redlock) — the same
// implementation the backend uses for its TriggerChord mutex. The lock is held
// for the given TTL and auto-expires afterwards, so a crashed holder never
// deadlocks the schedule.
type RedisLock struct {
	rsync    *redsync.Redsync
	retries  int
	interval time.Duration
}

// NewRedisLockWithClient creates a RedisLock with an externally owned
// redis.UniversalClient. The caller owns the client lifecycle.
func NewRedisLockWithClient(client redis.UniversalClient, retries int) RedisLock {
	if retries < 1 {
		retries = 1
	}
	l := RedisLock{
		retries:  retries,
		interval: defaultLockRetryInterval,
	}
	if client != nil {
		l.rsync = redsync.New(redsyncgoredis.NewPool(client))
	}
	return l
}

func (r RedisLock) LockWithRetries(ctx context.Context, key string, ttl time.Duration) error {
	var lastErr error
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(ctx, key, ttl)
		if err == nil {
			// 成功拿到锁，返回
			return nil
		}
		lastErr = err

		// 最后一次尝试后不再空等一个 interval
		if i < r.retries {
			time.Sleep(r.interval)
		}
	}
	// keep ErrRedisLockFailed as the sentinel while preserving the last cause
	return fmt.Errorf("%w: %w", ErrRedisLockFailed, lastErr)
}

func (r RedisLock) Lock(ctx context.Context, key string, ttl time.Duration) error {
	if r.rsync == nil {
		// a nil client would panic inside go-redis; fail with a domain error
		// instead (mirrors the nil-client guard the hand-rolled lock had)
		return fmt.Errorf("%w: redis client is nil", ErrRedisLockFailed)
	}
	if ttl <= 0 {
		// a non-positive ttl cannot express "hold until ..."; require a real
		// duration so the key always carries an expiry
		return fmt.Errorf("%w: lock ttl must be positive", ErrRedisLockFailed)
	}

	// single attempt here; retrying is LockWithRetries' job
	m := r.rsync.NewMutex(key,
		redsync.WithExpiry(ttl),
		redsync.WithTries(1),
	)
	if err := m.Lock(); err != nil {
		// redsync.ErrFailed means someone else holds the lock: report the
		// domain sentinel instead of leaking the implementation error
		if errors.Is(err, redsync.ErrFailed) {
			return ErrRedisLockFailed
		}
		return err
	}

	// intentionally not unlocking: the lock must survive for the whole TTL so
	// the same schedule slot cannot fire twice; it auto-expires afterwards
	return nil
}
