// Package examples 以场景化方式测试与验证调度框架：场景不经过任何 API，直接以
// 调度框架的接缝驱动（repo 落表、事件注入、handler 执行记录），验证任务生命
// 周期、两阶段发布、定时触发、失败收敛与启动恢复等调度语义本身。
//
// 组织方式：依赖初始化与场景共享句柄在本文件（Setup/Reset 与 H），场景 case
// 在 scenarios/ 各子包，清单与顺序执行在 main_test.go。依赖仅 PostgreSQL 与
// Redis，本地以 Docker 容器部署（见 maintain.sh），环境变量见 README.md；
// 依赖不可达时场景整体 Skip。
//
// 测试入口在 main_test.go（build tag: integration），运行方式：
//
//	go test -tags=integration -v ./examples/
package examples

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	pq "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	schedule "github.com/improvtrace/stateflux"
	dmpb "github.com/improvtrace/stateflux/api/dm"
	"github.com/improvtrace/stateflux/dispatch"
	"github.com/improvtrace/stateflux/migrations"
	"github.com/improvtrace/stateflux/queue"
	"github.com/improvtrace/stateflux/states"
	"github.com/improvtrace/stateflux/tasks"
)

// 组件运行参数：节点标识供调度器与手工认领共用；间隔取短值加速场景，
// 对账 grace 取短值使回拨 updated_at 的行能立即被对账收敛（对账超时判定
// 有 30s 预算下限，见 AgeRows）。
const (
	NodeName       = "examples-node"
	ReconcileEvery = 200 * time.Millisecond // 对账间隔
	ReconcileGrace = 50 * time.Millisecond  // 对账超时判定的额外缓冲

	// WaitDefault/WaitRetry 是场景等待窗口：常规收敛 20s；含框架执行内重试
	// 的场景放宽到 40s（重试退避为斐波那契秒级：约 1s 退避 + 1s 延迟队列轮询）。
	WaitDefault = 20 * time.Second
	WaitRetry   = 40 * time.Second
)

// DMServiceKey 是进程内 dm 桩的服务密钥：桩校验请求携带的 service_key，
// 借此验证 dispatch 的服务密钥拦截器（缺密钥的请求会被真实 dm 拒绝）。
const DMServiceKey = "examples-service-key"

// seedNode 是场景手工落表的雪花节点（各场景使用独立节点号，避免 ID 冲突）。
var seedNode, _ = snowflake.NewNode(3)

// Scenario 与场景同形：main_test.go 手工维护清单，顺序执行。
type Scenario struct {
	Name    string
	Disable bool
	Test    func(h *H, t *testing.T)
}

// H 是场景共享的环境句柄：数据库连接、任务表 repo、Redis 账本与调度器装配
// 入口。DMAddress 指向进程内 dm 桩的 unix socket，供 dispatch 走真实节点表
// 拉取路径。
type H struct {
	Ctx        context.Context
	DB         *sql.DB // 裸 SQL（清场/统计/回拨时间）与 repo 共用的连接池
	RedisAddrs []string
	RedisDB    int
	DMAddress  string
	Repo       states.TaskPersistentRepo
	Ledger     *states.Ledger

	rdb redis.UniversalClient
}

// Setup 连接依赖组件并做一次性引导：跑真实迁移（migrations/ 的 Atlas 版本化
// 迁移，测试库不存在时由它建库）建表。依赖不可达时跳过全部场景（不视为失败）。
func Setup(t *testing.T) *H {
	t.Helper()
	ctx := context.Background()
	pgAddr := envDefault("SCHEDULE_STABLE_EXAMPLES_PG_ADDR", "127.0.0.1:5432")
	pgUser := envDefault("SCHEDULE_STABLE_EXAMPLES_PG_USER", "postgres")
	pgPass := envDefault("SCHEDULE_STABLE_EXAMPLES_PG_PASSWORD", "postgres")
	dbName := envDefault("SCHEDULE_STABLE_EXAMPLES_PG_DB", "stateflux_examples")
	redisAddr := envDefault("SCHEDULE_STABLE_EXAMPLES_REDIS", "127.0.0.1:6379")
	redisDB := envInt("SCHEDULE_STABLE_EXAMPLES_REDIS_DB", 15)

	if !pgReachable(ctx, pgAddr, pgUser, pgPass) || !redisReachable(ctx, redisAddr) {
		t.Skipf("依赖组件不可达（PG %s / Redis %s），请先执行 ./maintain.sh install",
			pgAddr, redisAddr)
	}

	// 真实迁移链，迁移文件本身也是被验证对象；测试库不存在时由 Migrate 建库
	dsn := pgDSN(pgAddr, pgUser, pgPass, dbName)
	if _, err := migrations.Migrate(ctx, dsn); err != nil {
		t.Fatalf("执行迁移失败: %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("打开测试库失败: %v", err)
	}
	client := states.NewClient(db)

	rdb := queue.NewGoRedisClient(&queue.Config{}, []string{redisAddr}, redisDB)
	if err := rdb.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Fatalf("连接 Redis db %d 失败: %v", redisDB, err)
	}

	dmStop, dmAddr := startFakeDM(t)
	h := &H{
		Ctx:        ctx,
		DB:         db,
		RedisAddrs: []string{redisAddr},
		RedisDB:    redisDB,
		DMAddress:  dmAddr,
		Repo:       states.NewTaskPersistentRepo(client),
		Ledger:     states.NewLedger(rdb, ""),
		rdb:        rdb,
	}
	t.Cleanup(func() {
		dmStop()
		_ = rdb.Close()
		_ = client.Close()
	})
	return h
}

// dmServer 是进程内 dm 服务桩：unix socket 上提供 ListClusterNodesStatus，
// 返回固定单节点在线表（节点 ID 即 NodeName），驱动 dispatch 的真实节点表
// 拉取与缓存路径。与真实 dm 一致，要求请求携带正确的服务密钥。
type dmServer struct {
	dmpb.UnimplementedServiceServiceServer
}

// ListClusterNodesStatus 实现 dmpb.ServiceServiceServer。
func (dmServer) ListClusterNodesStatus(ctx context.Context, _ *emptypb.Empty) (*dmpb.ListClusterNodesStatusResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("service_key")) == 0 || md.Get("service_key")[0] != DMServiceKey {
		return nil, fmt.Errorf("service_key validation failed")
	}
	return &dmpb.ListClusterNodesStatusResponse{
		NodeId: NodeName,
		Nodes: []*dmpb.ListClusterNodesStatusResponse_Node{{
			Id:     NodeName,
			Ip:     "localhost",
			Role:   "LEADER",
			Online: true,
		}},
	}, nil
}

// startFakeDM 在临时目录的 unix socket 上启动 dm 服务桩，返回停止函数与
// 监听地址（socket 路径，作 DMAddress）。
func startFakeDM(t *testing.T) (func(), string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "scheddm-*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	sock := fmt.Sprintf("%s/dm.sock", dir)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("监听 dm unix socket 失败: %v", err)
	}
	srv := grpc.NewServer()
	dmpb.RegisterServiceServiceServer(srv, dmServer{})
	go func() { _ = srv.Serve(lis) }()
	return func() {
		srv.Stop()
		_ = os.RemoveAll(dir)
	}, sock
}

// Reset 清场：清空任务表与测试 Redis 库，保证 case 互不污染。
func (h *H) Reset(t *testing.T) {
	t.Helper()
	if _, err := h.DB.ExecContext(h.Ctx, `TRUNCATE task_persistents`); err != nil {
		t.Fatalf("清空 task_persistents 失败: %v", err)
	}
	if err := h.rdb.FlushDB(h.Ctx).Err(); err != nil {
		t.Fatalf("清空 Redis 失败: %v", err)
	}
}

// Runtime 是已启动的调度器组件集，Stop 停机（幂等，场景可 defer 调用）。
type Runtime struct {
	rt   *schedule.Runtime
	stop func()
}

// Stop 停机并等待各循环退出。
func (r *Runtime) Stop() { r.stop() }

// StartRuntime 按入口 Run 的顺序装配并启动调度器（装配顺序对照
// wire_gen.go，生命周期对照 entry.go Run）：注册 handler → 首轮对账（消化
// 崩溃残留并重派未完成行）→ worker → listener/cron/reconciler。registry 由
// 调用方指定，间隔与 grace 用 examples 短值，供场景细粒度验证。禁用任务
// （TaskSpec.Enabled=false）与入口同语义：不注册 handler、不启动 worker，
// cron 条目由 Registry.Timed 过滤。
func (h *H) StartRuntime(t *testing.T, specs []*tasks.TaskSpec) *Runtime {
	t.Helper()
	reg, err := tasks.NewRegistry(specs...)
	if err != nil {
		t.Fatalf("构造注册表失败: %v", err)
	}
	qcnf := &queue.Config{}
	if len(specs) > 0 {
		qcnf = &queue.Config{DefaultQueue: dispatch.QueueName(specs[0].Resource, NodeName)}
	}
	rdb := queue.NewGoRedisClient(qcnf, h.RedisAddrs, h.RedisDB)
	server, err := queue.NewServer(qcnf,
		queue.NewRedisBrokerWithClient(qcnf, rdb),
		queue.NewRedisBackendWithClient(qcnf, rdb),
		queue.NewRedisLockWithClient(rdb, 3),
	)
	if err != nil {
		t.Fatalf("构造执行框架失败: %v", err)
	}
	listener := schedule.NewListener(0)
	disp := dispatch.New(server, h.DMAddress, DMServiceKey)
	pub := schedule.NewPublisher(h.Repo, disp, reg, h.Ledger, NodeName)
	cronScheduler, err := schedule.NewCronScheduler(reg, pub, disp, listener, rdb, NodeName)
	if err != nil {
		t.Fatalf("构造定时调度器失败: %v", err)
	}
	reconciler := states.NewReconciler(h.Repo, server.GetBackend(), h.Ledger, pub, ReconcileEvery, ReconcileGrace)
	rt := &schedule.Runtime{
		Repo:          h.Repo,
		Registry:      reg,
		Server:        server,
		Listener:      listener,
		CronScheduler: cronScheduler,
		Reconciler:    reconciler,
	}

	// 与 entry.go handlers 一致的成功内联回写包装（禁用任务不注册）
	handlers := make(map[string]any, len(specs))
	for _, ts := range specs {
		if !ts.Enabled {
			continue
		}
		raw := ts.Handler.(func(string, int64) error)
		handlers[ts.HandlerName()] = func(payload string, taskID int64) error {
			if err := raw(payload, taskID); err != nil {
				return err
			}
			if taskID != 0 {
				wctx := context.WithoutCancel(h.Ctx)
				if err := h.Repo.MarkTerminal(wctx, taskID, states.TaskOutcomeSucceeded, ""); err != nil {
					slog.Error("examples: failed to mark task succeeded", "task_id", taskID, "error", err)
				}
			}
			return nil
		}
	}
	if err := server.RegisterTasks(handlers); err != nil {
		t.Fatalf("注册 handler 失败: %v", err)
	}

	// 首轮对账先于一切发布（消化崩溃残留并重派未完成行）
	rt.Reconciler.ReconcileOnce(h.Ctx)

	runCtx, cancel := context.WithCancel(h.Ctx)
	var wg sync.WaitGroup
	run := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = fn(runCtx)
		}()
	}

	// 每个资源在本节点的分节点队列上各一个消费者，排空 worker 错误通道
	errCh := make(chan error, len(specs))
	go func() {
		for {
			select {
			case <-runCtx.Done():
				return
			case err := <-errCh:
				if err != nil {
					slog.Error("examples: worker exited with error", "error", err)
				}
			}
		}
	}()
	for _, spec := range specs {
		if !spec.Enabled {
			continue // 禁用任务不启动消费者（TaskSpec.Enabled）
		}
		concurrency := spec.Concurrency
		if concurrency <= 0 {
			concurrency = tasks.DefaultConcurrency
		}
		worker := server.NewCustomQueueWorker(fmt.Sprintf("%s@%s", spec.Resource, NodeName), concurrency, dispatch.QueueName(spec.Resource, NodeName))
		worker.LaunchAsync(errCh)
	}

	listener.Register(cronScheduler)
	run(listener.Run)
	run(cronScheduler.Run)
	run(reconciler.Run)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			<-server.Shutdown().Done()
			_ = rdb.Close()
		})
	}
	t.Cleanup(stop)
	return &Runtime{rt: rt, stop: stop}
}

// StartEntry 以真实入口 schedule.Run 启动调度器（注册表为 wire 装配的默认
// 任务：持久化转发消费侧 + sayhello 演示任务），用于冒烟验证 entry 生命周期
// 与停机语义。节点 ID 由入口经 dm 桩的 ListClusterNodesStatus 获取
// （node_id = NodeName）。返回退出错误通道（ctx 取消时收到 ctx.Err()）与
// 停止函数。
func (h *H) StartEntry(t *testing.T) (<-chan error, context.CancelFunc) {
	t.Helper()
	cfg := schedule.Config{
		RedisAddrs:        h.RedisAddrs,
		RedisDB:           h.RedisDB,
		DMAddress:         h.DMAddress, // 进程内 dm 桩：单节点在线表（节点 ID 即 NodeName）
		DMServiceKey:      DMServiceKey,
		Repo:              h.Repo,
		ReconcileInterval: ReconcileEvery,
	}
	ctx, cancel := context.WithCancel(h.Ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- schedule.Run(ctx, cfg) }()
	return errCh, cancel
}

// SeedRow 手工落一行 pending 任务（模拟业务投递），返回任务 ID。
func (h *H) SeedRow(t *testing.T, resource string, payload json.RawMessage, key string) int64 {
	t.Helper()
	row := (&tasks.TaskSpec{Resource: resource}).NewRow(payload)
	row.Id = seedNode.Generate().Int64()
	row.IdempotencyKey = key
	created, err := h.Repo.Create(h.Ctx, row)
	if err != nil {
		t.Fatalf("落任务行失败: %v", err)
	}
	return created.Id
}

// GetRow 按 ID 取行（断言失败即 Fatal）。
func (h *H) GetRow(t *testing.T, id int64) *states.TaskPersistent {
	t.Helper()
	row, err := h.Repo.Get(h.Ctx, id)
	if err != nil {
		t.Fatalf("查询任务 %d 失败: %v", id, err)
	}
	return row
}

// CountRows 统计某资源的总行数。
func (h *H) CountRows(t *testing.T, resource string) int {
	t.Helper()
	return h.countRowsWhere(t, `resource = $1`, resource)
}

// CountTerminal 统计某资源已到达指定终态的行数。
func (h *H) CountTerminal(t *testing.T, resource string, outcome states.TaskOutcome) int {
	t.Helper()
	return h.countRowsWhere(t, `resource = $1 AND phase = $2 AND outcome = $3`,
		resource, int(states.TaskPhaseTerminal), int(outcome))
}

// CountTerminalByIds 统计指定任务集合中已到达指定终态的行数（排除额外生成行
// 的干扰）。
func (h *H) CountTerminalByIds(t *testing.T, ids []int64, outcome states.TaskOutcome) int {
	t.Helper()
	return h.countRowsWhere(t, `phase = $1 AND outcome = $2 AND id = ANY($3)`,
		int(states.TaskPhaseTerminal), int(outcome), pq.Array(ids))
}

// countRowsWhere 按 SQL 条件统计行数（H 内部辅助）。
func (h *H) countRowsWhere(t *testing.T, cond string, args ...any) int {
	t.Helper()
	query := fmt.Sprintf(`SELECT count(*) FROM task_persistents WHERE %s`, cond)
	var n int
	if err := h.DB.QueryRowContext(h.Ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("统计任务行失败: %v", err)
	}
	return n
}

// AgeRows 把在途行（dispatching/dispatched）的 updated_at 回拨 120s，越过
// 对账超时判定（NewRow 默认预算 60s + grace；同步触发对账时必须一次到位），
// 驱动对账场景无需真实等待。
func (h *H) AgeRows(t *testing.T) {
	t.Helper()
	_, err := h.DB.ExecContext(h.Ctx,
		`UPDATE task_persistents SET updated_at = now() - interval '120 seconds' WHERE phase IN ($1, $2)`,
		int(states.TaskPhaseDispatching), int(states.TaskPhaseDispatched))
	if err != nil {
		t.Fatalf("回拨 updated_at 失败: %v", err)
	}
}

// FlushRedis 清空测试 Redis 库，模拟队列与执行状态全丢（Redis 无持久化）。
func (h *H) FlushRedis(t *testing.T) {
	t.Helper()
	if err := h.rdb.FlushDB(h.Ctx).Err(); err != nil {
		t.Fatalf("清空 Redis 失败: %v", err)
	}
}

// NewQueueBackend 构造独立的 Redis backend：对账场景在启动 runtime 前收敛
// 在途行时使用（场景结束时随测试清理连接）。
func (h *H) NewQueueBackend(t *testing.T) queue.Backend {
	t.Helper()
	qcnf := &queue.Config{}
	rdb := queue.NewGoRedisClient(qcnf, h.RedisAddrs, h.RedisDB)
	t.Cleanup(func() { _ = rdb.Close() })
	return queue.NewRedisBackendWithClient(qcnf, rdb)
}

// WaitFor 以 200ms 步进轮询 cond 直到成立，超时 Fatal。
func WaitFor(t *testing.T, desc string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("等待超时（%v）: %s", timeout, desc)
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// pgDSN 拼 PostgreSQL 的 URL 形式 DSN（账号密码经 URL 转义）。
func pgDSN(addr, user, pass, dbName string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host, port = addr, "5432"
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     net.JoinHostPort(host, port),
		Path:     dbName,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// pgReachable 探活 PostgreSQL（连维护库 postgres）。
func pgReachable(ctx context.Context, addr, user, pass string) bool {
	db, err := sql.Open("postgres", pgDSN(addr, user, pass, "postgres"))
	if err != nil {
		return false
	}
	defer db.Close()
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return db.PingContext(pctx) == nil
}

// redisReachable 探活 Redis。
func redisReachable(ctx context.Context, addr string) bool {
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return client.Ping(pctx).Err() == nil
}
