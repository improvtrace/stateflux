# stateflux

基于 **PostgreSQL（唯一权威）+ 可替换 EventBus 通道** 的分布式任务调度服务。任务可靠性不依赖
Redis、RPC 或任何消息通道的持久性：PG 的四阶段账本、attempt fence 和对账负责收敛；业务 handler
仍必须幂等。

设计为 **v4.0**（在 v3.21 基础上按 §15「落地补充确认」落地）：详见
[设计索引](./.docs/design/README.md) 与 [§15 落地补充确认](./.docs/design/08-confirmed-delivery.md)。

## 核心设计

- **四阶段账本**：`pending → schedulable → processing → completed`；结果不可变，PG 是唯一事实来源。
- **分发语义**：`Dispatch` 契约支持 `at_least_once` / `at_most_once` / `exactly_once`，支持
  异步 redis queue 与同步 rpc 两种投递，并支持节点间转发（§15.1#5/#6）。
- **执行节点能力**：`api/stateflux/worker` 定义用户自定义能力 RPC（主机验密、文件上传等），
  实现放 `internal/biz`，`internal/worker` 只负责组织/注册/管理（§15.1#7/#8）。
- **共识信息**：`api/stateflux/coherence` 同步「异步 redis 队列 ↔ 执行节点」映射，由调度节点
  分配并 RPC 通知执行节点（§15.1#4）。
- **集群视图**：`api/cluster` 提供 `node_id` / `vpc` / `label`，`internal/cluster` 封装
  grpc/http/static 客户端（§15.1#3）。
- **运行时**：`internal/runtime/scheduler` 可承载多个实例，各自绑定不同触发方式
  （tick / notify / coherence / manual）（§15.1#11）。
- **编解码**：异步任务分发/消费采用 machinery 风格的「签名 + 消息体」codec
  （`internal/task/codec`，§15.1#13）。
- **装配**：启动依赖 **google/wire** 注入（`internal/server/wire.go` + `wire_gen.go`，§15.1#1）。

## 布局

```text
api/
├── cluster/v1/            # 外置集群视图（node_id / vpc / label / roles / capabilities）
├── dispatch/v1/           # 对外任务分发（语义 / 投递 / 转发）
└── stateflux/
    ├── task/v1/           # ExecutorService + TaskMessage/ResultEvent 信封
    ├── coherence/v1/      # 共识信息同步（队列↔节点映射）
    ├── forward/v1/        # 节点间 RPC 转发
    └── worker/v1/         # 执行节点能力 RPC
internal/
├── server/                # wire 装配 + 服务生命周期
├── biz/                   # api 契约服务端实现（按 api 域拆分子包）
│   ├── task/              #   api/stateflux/task/v1：Execute/归集 + 工厂入队
│   ├── worker/            #   api/stateflux/worker/v1：能力 RPC + 验密/上传/队列视图
│   ├── coherence/         #   api/stateflux/coherence/v1：共识快照与调度↔执行同步
│   └── dispatch/          #   api/dispatch/v1：对外分发与节点间转发
├── cluster/               # ClusterView：grpc / http / static
├── forward/               # 节点间 RPC 转发（环路保护）
├── worker/                # 能力注册/管理 + 执行运行时（WAL）
├── task/                  # Task/Registry + codec + factory + dispatch
├── runtime/               # scheduler（多触发器实例）/ collector / reconcile
├── eventbus/              # 逻辑 topic；channel/ 的 rpc 与 redis 实现
├── domain/                # schema / repository / data / cacheview / migration
├── config/                # 配置与默认值
└── obs/                   # OTel 指标/链路装配
```

## 运行时开关（`STATEFLUX_*` 环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `STATEFLUX_CLUSTER_TRANSPORT` | `static` | `static` / `grpc` / `http` |
| `STATEFLUX_CLUSTER_ENDPOINT` | – | 外置集群视图地址（grpc/http） |
| `STATEFLUX_RUNTIME_SCHEDULER_TRIGGERS` | `tick` | 触发方式：`tick,notify,coherence,manual`（多实例） |
| `STATEFLUX_DISPATCH_RESULT_CHANNEL` | `stream` | 结果归集通道，可换 `redis-pubsub`/`redis-list` |
| `STATEFLUX_SERVER_SHUTDOWN_TIMEOUT` | `10s` | 优雅退出总预算（停接入 → 逆序停组件 → 强制停 gRPC） |
| `STATEFLUX_RUNTIME_WAL_DIR` | `.stateflux-wal` | 结果 WAL 目录；空为进程内 WAL |
| `STATEFLUX_PG_SEARCH_PATH` | `public` | 固定 search_path（避免用户名与 schema 同名） |

控制面（Scheduler/Collector/Reconciler/Factory/Coherence 同步）仅在外部选举指向本实例时运行；
执行侧（worker、coherence 拉取）常驻所有节点（§2.1）。

## 优雅退出

进程收到首个 `SIGINT`/`SIGTERM` 后按固定顺序退出，总预算由 `STATEFLUX_SERVER_SHUTDOWN_TIMEOUT`
约束（默认 10s）：

1. `/readyz` 立即转 `503`，通知负载均衡/服务发现摘除流量（`/healthz` 保持存活探针语义）；
2. gRPC `GracefulStop` + HTTP `Shutdown`：停止接入新请求并排空在途 RPC/HTTP；
3. 逆序停止全部后台组件（worker/调度/归集/对账/共识同步/工厂），最后关闭 eventbus 的
   结果流等底层通道（使 gRPC `GracefulStop` 能立即排空）；worker 会等待在途执行写完 WAL
   后再关闭本地结果日志；
4. 预算内未排空则强制 `grpc.Stop`；随后逐步限时释放装配资源（DB 连接池、PG 监听、集群
   视图等，每步上限 5s，超时告警并继续）并刷出 OTel 数据。

再次收到信号即强制退出（退出码 2），避免关机无限挂起；启动阶段任一组件启动失败会回滚已启动
组件并关闭服务面，不留半启动状态。

## 开发

```bash
make api        # 生成 api/ 下 proto 的 Go 代码（需要 protoc）
make generate   # 生成 ent 代码（go tool entgo.io/ent/cmd/ent）
make wire       # 生成 wire 注入代码（go tool github.com/google/wire/cmd/wire）
make build      # 构建 cmd/stateflux
make vet        # go vet ./...
```

本地中间件：`hack/dev.sh up`。生产 Redis 仅作为通道部署；其 AOF、复制、ACK 或 Stream PEL 不能成为
stateflux 正确性假设。
