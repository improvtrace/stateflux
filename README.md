# stateflux

基于 **PostgreSQL（唯一权威）+ 可替换 EventBus 通道** 的分布式任务调度服务。任务可靠性不依赖
Redis、RPC 或任何消息通道的持久性：PG 的四阶段账本、attempt fence 与对账负责收敛，Redis 只作为
异步通道与尽力而为的执行视图；业务 handler 仍必须幂等。

单二进制同时提供服务面（gRPC + HTTP）与执行面；设计文档见
[设计索引](./.docs/design/README.md) 与 [§15 落地补充确认](./.docs/design/08-confirmed-delivery.md)。

## 核心设计

- **四阶段账本**：`pending → schedulable → processing → completed` 七表落库（含幂等账本、在途
  payload、终态结果）；挪行均在单事务内完成，结果不可变，PG 是唯一事实来源。
- **attempt fence**：`attempts` 只在 `Claim`（`FOR UPDATE SKIP LOCKED`）递增；`Complete` 拒绝
  任何 attempt 不一致的过期结果，重复投递天然安全。
- **分发语义**：`at_least_once` / `at_most_once` / `exactly_once`（exactly_once 依赖 Redis 去重
  窗口，Redis 不可用时报错而非静默重复）；投递支持异步 redis queue 与同步 rpc，目标不为本节点时
  经 forward 服务转发（环路保护：visited_nodes + ttl）。
- **集群视图（DSN 配置）**：`local://`（单机内置）、`http(s)://`、`grpc://` 三种 DSN 接入外部
  集群视图；`NodeInfo` 只含 `node_id/address/vpc/labels/role/online/is_leader`，
  stateflux 侧权限（`operation/execute/schedule/collect`）由 role 推导：primary 全量，
  standby 仅 `operation+execute`。
- **选举门控**：控制面组件（Scheduler/Collector/Reconciler/Factory/Coherence 同步）仅当外部视图
  指向本节点（`SchedulerNodeID == self`）且具备所需权限时运行，轮询跟随（默认 2s）；执行面
  （worker 运行时、coherence 拉取）常驻所有节点。
- **EventBus 懒创建通道**：逻辑 topic（队列名即任务行 channel 列，如 `default`；结果归集为
  `result`）经 channel 工厂按需创建并缓存；每次调用可用 `WithChannel` 指定通道，未指定时按
  「显式 option → 节点寻址（coherence 队列路由）→ 默认通道」解析。预注册 `default`（redis-list）、
  `sync`（rpc-unary）、`stream`（rpc-stream）及各 kind 别名。
- **共识信息（coherence）**：调度节点分配「队列 ↔ 执行节点」路由并 gRPC `Notify` 推送、执行节点
  `GetCoherence` 拉取；worker 据此动态重算订阅队列（周期同 `coherence.sync_interval`，默认 10s）。
- **结果可靠性**：worker 先写本地 WAL 再发布 `ResultEvent`，未 ACK 结果周期性重放（1s）；
  WAL 积压超阈值（1 万条 / 256MB 高水位）暂停新订阅。WAL 是易失的衍生数据，丢失由 R1 重派收敛。
- **调度触发**：Scheduler 支持多实例，各绑定 `tick` / `notify`（PG LISTEN/NOTIFY 唤醒，仅是
  优化，tick 兜底）/ `coherence`（revision 变更）/ `manual` 触发器。
- **编解码与工厂**：异步分发采用 machinery 风格「签名 + 消息体」帧（magic `SFXM`）；
  Factory 按周期生成任务并记录 batch_id，孤儿批次由对账扫描。
- **回调**：任务终态可携带 `OnSuccess` / `OnError` 子任务（幂等派生，深度上限 8）。
- **装配**：依赖注入用 google/wire（`cmd/stateflux/wire.go` + `wire_gen.go`）；数据模型用 ent
  （`internal/domain/schema`，生成码不入库）。

## 布局

```text
api/
├── cluster/v1/                 # 外置集群视图：ClusterService.GetClusterInfo + NodeInfo(role)
└── stateflux/
    ├── task/v1/                # ExecutorService：Execute / ResultStream / Collect + TaskMessage/ResultEvent 信封
    ├── dispatch/v1/            # 对外分发：Dispatch（语义 / 投递 / 目标 / 转发）
    ├── coherence/v1/           # 共识信息：GetCoherence / Notify（队列路由 + revision）
    ├── forward/v1/             # 节点间 RPC 转发（方法 + payload + ttl）
    └── worker/v1/              # 执行节点能力：ListCapabilities / Invoke / VerifyPassword / UploadFile
pkg/
├── transport/                  # gRPC 连接池（Dialer）：rpc 通道、共识推送/转发共用
└── idgen/                      # 雪花任务 ID（bwmarrin/snowflake 封装：固定 epoch + 节点位哈希策略）
internal/
├── server/                     # App 装配与生命周期、健康检查、选举门控
├── cluster/                    # ClusterView：local / http(s) / grpc 客户端 + 缓存与权限推导
├── eventbus/                   # EventBus（懒创建、WithChannel）+ channel/{rpc,redis,mem}
├── task/                       # Task/Registry + 投递语义枚举 + codec + factory + dispatch
├── worker/                     # handler/capability 注册 + 执行运行时（WAL，先写后发）
├── biz/                        # api 契约服务端实现（按域拆分）
│   ├── task/                   #   ExecutorService + 结果发布 + 工厂入队
│   ├── worker/                 #   能力 RPC（host.verify_password / file.upload）+ 队列视图
│   ├── coherence/              #   共识快照 + 调度侧推送 / 执行侧拉取
│   ├── dispatch/               #   对外分发与转发入口
│   └── forward/                #   节点间转发（环路保护）
├── runtime/                    # scheduler（多触发器实例）/ collector / reconcile（R1–R4）
├── domain/                     # ent schema / repository（自有仓储实体）/ data（PG+Redis，ent↔实体转换）/ cacheview / migration
├── config/                     # 配置与默认值（viper）
└── obs/                        # OTel 指标/链路装配（默认关闭）
cmd/                            # main（cobra）+ serve 子命令 + wire 装配
build/                          # Dockerfile + docker-compose（PG/Redis）
hack/                           # dev.sh（中间件起停、迁移）
```

## 快速开始

要求：Go 1.27；构建前需生成代码——**ent 客户端与 proto 生成码均不入库**，克隆后先执行：

```bash
make generate   # ent 生成码（go tool ent）
make api        # proto 生成码（需要 protoc ≥ 25 及 protoc-gen-go / protoc-gen-go-grpc）
```

本地中间件与迁移：

```bash
hack/dev.sh up        # docker compose 启动 PostgreSQL 16(5432) + Redis 7(6379)，账号/库名均为 stateflux
hack/dev.sh migrate   # 按序应用 internal/domain/migration/*.sql（不会自动迁移，升级时需手动执行）
```

构建并启动单机节点（默认 DSN 即指向上述本地 PG/Redis）：

```bash
make build
./bin/stateflux serve --cluster-dsn local:// --grpc-addr 127.0.0.1:9090 --http-addr 127.0.0.1:9091
```

接入外部集群视图（多节点部署，由外部系统实现 `cluster.v1.ClusterService`）：

```bash
./bin/stateflux serve --cluster-dsn 'grpc://10.0.0.5:9090?timeout=10s&connect_timeout=30s&interval=3s'
./bin/stateflux serve --cluster-dsn 'http://10.0.0.5:8080?interval=3s'
```

DSN 参数（URL query）：`timeout`（单次调用预算，默认 10s）、`connect_timeout`（默认 30s）、
`interval`（视图缓存刷新周期，默认 3s，`0` 表示仅按需拉取）；`local://` 另支持
`node_id` / `address` 覆盖内置节点。

## 配置

装载优先级：**默认值 → 配置文件（`serve --config <file>`，yaml/toml/json 等）→ `STATEFLUX_*`
环境变量 → 命令行 flag**。命令行 flag 仅 `--config`、`--pg-dsn`、`--redis-addrs`、`--cluster-dsn`、
`--grpc-addr`、`--http-addr`；其余经环境变量或配置文件调整（`STATEFLUX_` 前缀，路径 `.` → `_`，
如 `runtime.wal_dir` → `STATEFLUX_RUNTIME_WAL_DIR`；列表用逗号分隔）。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `STATEFLUX_PG_DSN` | `postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable` | PG 连接串 |
| `STATEFLUX_PG_SEARCH_PATH` | `public` | 固定 search_path（避免用户名与 schema 同名冲突） |
| `STATEFLUX_PG_MAX_OPEN_CONNS` / `_MAX_IDLE_CONNS` | `30` / `10` | 连接池 |
| `STATEFLUX_REDIS_ADDRS` | `127.0.0.1:6379` | 逗号分隔；多个地址按 cluster 客户端连接 |
| `STATEFLUX_CLUSTER_DSN` | `local://localhost` | 集群视图 DSN，见上文 |
| `STATEFLUX_SERVER_GRPC_ADDR` / `_HTTP_ADDR` | `127.0.0.1:9090` / `127.0.0.1:9091` | 监听地址 |
| `STATEFLUX_SERVER_SHUTDOWN_TIMEOUT` | `10s` | 优雅退出总预算 |
| `STATEFLUX_RUNTIME_SCHEDULER_TRIGGERS` | `tick` | 逗号分隔：`tick,notify,coherence,manual`，多个值 = 多个 Scheduler 实例 |
| `STATEFLUX_RUNTIME_TICK_INTERVAL` | `100ms` | tick 触发周期 |
| `STATEFLUX_RUNTIME_PROMOTE_INTERVAL` | `200ms` | pending→schedulable 挪行周期 |
| `STATEFLUX_RUNTIME_CLAIM_BATCH` | `500` | 单轮认领批量 |
| `STATEFLUX_RUNTIME_RECONCILE_INTERVAL` | `10s` | 对账（R1 等）周期 |
| `STATEFLUX_RUNTIME_DISPATCH_GRACE` | `90s` | processing 超过此时长视为失联（R1 重派）；同时作为执行超时 |
| `STATEFLUX_RUNTIME_WORKER_QUEUES` | `default` | 执行节点初始订阅队列（coherence 路由可用后被覆盖） |
| `STATEFLUX_RUNTIME_FACTORY_INTERVAL` | `30s` | Factory 生成周期；`<=0` 不注册 Factory |
| `STATEFLUX_RUNTIME_WAL_DIR` | `.stateflux-wal` | 结果 WAL 目录（`{dir}/results.wal`）；置空为进程内 WAL |
| `STATEFLUX_DISPATCH_DEFAULT_SEMANTICS` | `at_least_once` | 亦可为 `at_most_once` / `exactly_once` |
| `STATEFLUX_DISPATCH_DEFAULT_DELIVERY` | `redis_queue` | 亦可为 `sync_rpc` |
| `STATEFLUX_DISPATCH_RESULT_CHANNEL` | `stream` | 结果归集通道：`stream`（gRPC ResultStream）或 `redis-pubsub`/`redis-list` 等 |
| `STATEFLUX_DISPATCH_MAX_HOPS` | `3` | 转发最大跳数 |
| `STATEFLUX_DISPATCH_DEDUPE_WINDOW` | `10m` | exactly_once 去重窗口 |
| `STATEFLUX_COHERENCE_SYNC_INTERVAL` | `10s` | 共识同步/队列重算周期 |
| `STATEFLUX_COHERENCE_REVISION_TTL` | `5m` | revision 缓存 TTL |
| `STATEFLUX_OBS_ENABLED` | `false` | OTel 导出开关 |
| `STATEFLUX_OBS_OTLP_ENDPOINT` | – | OTLP gRPC 导出地址 |
| `STATEFLUX_OBS_SERVICE_NAME` | `stateflux` | 资源服务名 |

`runtime.enable_collector` / `enable_reconcile` 目前仅是配置占位：装配始终创建对应组件，
实际运行由选举门控决定。

## 优雅退出

进程收到首个 `SIGINT`/`SIGTERM` 后按固定顺序退出，总预算由 `STATEFLUX_SERVER_SHUTDOWN_TIMEOUT`
约束（默认 10s）：

1. `/readyz` 立即转 `503`，通知负载均衡/服务发现摘除流量（`/healthz` 保持存活探针语义）；
2. gRPC `GracefulStop` + HTTP `Shutdown` 并行执行：停止接入新请求并排空在途 RPC/HTTP；
3. 逆序停止全部后台组件（worker/调度/归集/对账/共识同步/工厂），最后关闭 eventbus 的底层通道；
   worker 等待在途执行写完 WAL 后再关闭本地结果日志；
4. 预算内未排空则强制 `grpc.Stop`；随后逐步限时释放装配资源（DB 连接池、PG 监听、集群视图等，
   每步上限 5s，超时告警并继续），最后刷出 OTel 数据。

再次收到信号即强制退出（退出码 2）；启动阶段任一组件失败会回滚已启动组件并关闭服务面，
不留半启动状态。

## 开发

```bash
make api        # 生成 api/ 下 proto 的 Go 代码（protoc）
make generate   # 生成 ent 代码（features: sql/upsert, sql/lock, sql/execquery）
make wire       # 生成 wire 注入代码
make build      # 构建 bin/stateflux（入口 ./cmd）
make vet        # go vet ./...
make fmt        # gofmt
go test ./...   # 全量测试；internal/domain/data 下 *_it_test.go 为集成测试，需要本地 PG/Redis
```

生产 Redis 仅作为通道部署：其 AOF、复制、ACK 或 Stream PEL 均不构成 stateflux 的正确性假设。
