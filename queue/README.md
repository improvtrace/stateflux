# queue

Machinery is an asynchronous task queue / job queue for Go, based on distributed
message passing.

This is a trimmed-down fork of [RichardKnop/machinery](https://github.com/RichardKnop/machinery):

- **Redis only** — broker, result backend and distributed lock are implemented
  on top of [go-redis v9](https://github.com/redis/go-redis) (single instance,
  Sentinel failover and cluster modes supported).
- **Single flat package** — all code lives in one `package queue`, import path
  `github.com/improvtrace/stateflux/queue`.
- No AMQP / SQS / GCPPubSub / DynamoDB / Mongo / Memcache implementations, no
  v1 code, no build or test tooling.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/improvtrace/stateflux/queue"
)

func main() {
	cnf := &queue.Config{
		DefaultQueue:  "machinery_tasks",
		ResultsExpireIn: 3600,
		Redis:         &machinery.RedisConfig{NormalTasksPollPeriod: 1000},
	}

	// Build one redis client and inject it into broker, backend and lock.
	// The caller owns the client lifecycle.
	client := queue.NewGoRedisClient(cnf, []string{"localhost:6379"}, 0)

	broker := queue.NewRedisBrokerWithClient(cnf, client)
	backend := queue.NewRedisBackendWithClient(cnf, client)
	lock := queue.NewRedisLockWithClient(client, 3)

	server, err := queue.NewServer(cnf, broker, backend, lock)
	if err != nil {
		panic(err)
	}
	server.RegisterTask("add", func(a, b int) (int, error) {
		return a + b, nil
	})

	// Send a task
	asyncResult, err := server.SendTask(
		&queue.Signature{Name: "add", Args: []queue.Arg{{
			Type: "int", Value: 1,
		}, {
			Type: "int", Value: 2,
		}}},
	)
	if err != nil {
		panic(err)
	}

	// Wait for the result; the context cancels the wait
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := asyncResult.Get(ctx, time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println(results[0].Int()) // 3

	// Run a worker (blocking)
	worker := server.NewWorker("worker_1", 10)
	if err := worker.Launch(); err != nil {
		panic(err)
	}
}
```

The broker, backend and lock are always constructed with an externally owned
`redis.UniversalClient` (build one with `NewGoRedisClient`, or bring your own —
for example a client that already shares a connection pool with your app).
The library never closes the injected client.

## Configuration

`Config` is constructed in code and passed to `NewServer` together with a
broker, a backend and a lock (all three must be non-nil; `DefaultQueue` is
required).

Key fields:

| Field | Description |
|---|---|
| `AppName` | Namespaces periodic-task lock keys (defaults to the binary name) |
| `DefaultQueue` | Required queue name (e.g. `machinery_tasks`) |
| `ResultsExpireIn` | Seconds to keep task states / group metas (default 3600) |
| `Redis.NormalTasksPollPeriod` | Poll period in ms for normal tasks (default 1000) |
| `Redis.DelayedTasksPollPeriod` | Poll period in ms for delayed tasks (default 500) |
| `Redis.DelayedTasksKey` | Sorted-set key used for delayed tasks (`delayed_tasks`) |
| `Redis.MasterName` / `Redis.SentinelPassword` | Sentinel mode |
| `Redis.ClusterEnabled` | Use a cluster client |
| `TLSConfig` | TLS settings applied to the redis client |
| `NoUnixSignals` | Disable the built-in SIGINT/SIGTERM handling |

## API overview

| Identifier | Purpose |
|---|---|
| `Server` / `NewServer` | Register tasks, send workflows, create workers; `Shutdown` stops the scheduler and quits launched workers |
| `Worker` | `Launch`, `LaunchAsync`, `Quit`, custom handlers (`SetErrorHandler`, `SetPreTaskHandler`, ...) |
| `NewRedisBrokerWithClient` | Redis broker (externally owned client) |
| `NewRedisBackendWithClient` | Redis result backend (externally owned client); all operations take a `context.Context` |
| `NewRedisLockWithClient` | Distributed lock used by periodic tasks (redsync/Redlock, TTL based, externally owned client) |
| `Signature`, `Arg`, `Chain`, `Group`, `Chord` | Task / workflow definitions |
| `AsyncResult`, `ChainAsyncResult`, `ChordAsyncResult` | Result handles (`Get(ctx, ...)`, `GetWithTimeout(ctx, ...)`, `Touch(ctx)`; failures surface as `TaskFailedError`) |
| `NewGoRedisClient` | Build a `redis.UniversalClient` from `Config` (single / Sentinel / cluster) for injection |
| `RegisterPeriodicTask/Chain/Group/Chord` | Cron-scheduled workflows (cron spec, guarded by the lock; the scheduler starts lazily and panics in jobs are recovered) |

## Project layout

Everything lives in one flat `package queue`:

| File | Contents |
|---|---|
| `machinery.go` | `Server` and `Worker` — task registration, publishing, consumption, lifecycle |
| `broker.go` | `Broker` / `TaskProcessor` interfaces, `BaseBroker`, errors, `RedisBroker` |
| `backend.go` | `Backend` interface, `RedisBackend`, result handles |
| `redis.go` | `NewGoRedisClient` (single / Sentinel / cluster), `Lock` interface and `RedisLock` |
| `config.go` | `Config` and `RedisConfig` |
| `tasks.go` | `Signature`, `Task`, task states, chains / groups / chords, reflection helpers |
| `utils.go` | Retry closure and Fibonacci backoff |
| `tracing.go` | OpenTracing integration |

Logging uses the standard library's `log/slog`: the library emits records at
`Debug`, `Info`, `Warn` and `Error` levels through `slog.Default()`. Configure
logging in your application with `slog.SetDefault` (for example with a JSON
handler) before launching workers.

## License

MIT — see [LICENSE](LICENSE).
