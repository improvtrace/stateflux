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
├── cluster/                    # ClusterView：local / http(s) / grpc 客户端 + 缓存与权限推导 + Query/Describe（info 子命令）
│   └── mockserver/             #   外置集群服务本地单机模拟（cluster 子命令实现）：gRPC+HTTP 双面、主机派生固定节点 ID
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
cmd/                            # main（cobra）+ serve/cluster/info 子命令 + wire 装配
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
hack/dev.sh migrate   # dist/bin/stateflux migrate：版本化迁移（幂等，见「版本与迁移」）
```

构建并启动单机节点（默认 DSN 即指向上述本地 PG/Redis）：

```bash
make build
dist/bin/stateflux serve --cluster-dsn local:// --grpc-addr 127.0.0.1:9090 --http-addr 127.0.0.1:9091
```

接入外部集群视图（多节点部署，由外部系统实现 `cluster.v1.ClusterService`）：

```bash
dist/bin/stateflux serve --cluster-dsn 'grpc://10.0.0.5:9090?timeout=10s&connect_timeout=30s&interval=3s'
dist/bin/stateflux serve --cluster-dsn 'http://10.0.0.5:8080?interval=3s'
```

DSN 参数（URL query）：`timeout`（单次调用预算，默认 10s）、`connect_timeout`（默认 30s）、
`interval`（视图缓存刷新周期，默认 3s，`0` 表示仅按需拉取）；`local://` 另支持
`node_id` / `address` 覆盖内置节点。

### 本地模拟外部集群（cluster 子命令）

没有真实的外部选举/成员系统时，用 `stateflux cluster` 起一个本地单机 mock：同时监听
gRPC（`127.0.0.1:9190`）与 HTTP/JSON（`127.0.0.1:9191`），serve 侧以任一协议 DSN
接入即可（实现全部在 `internal/cluster/mockserver`，§15.1#3）。**单机定位**：仅供
serve 单机部署测试，不内置成员同步/共识——多实例各自独立、视图不互通。

```bash
# 默认单节点视图：主机信息派生的固定节点 ID（见下），primary/leader，
# 地址指向 serve 默认 gRPC 监听 127.0.0.1:9090
dist/bin/stateflux cluster

# serve 接入 mock（二选一）
dist/bin/stateflux serve --cluster-dsn grpc://127.0.0.1:9190
dist/bin/stateflux serve --cluster-dsn http://127.0.0.1:9191
```

**默认节点 ID 由主机信息派生（固定）**：`node-<hostname>-<hash8>`——hostname 归一
为 `[a-z0-9-]`，叠加 machine-id（不可读时退回首个非环回网卡 MAC）做 FNV 哈希短
后缀。同一主机跨重启恒定，serve 单机联调因此保持稳定的节点身份（选举归属、任务
归属都携带节点 ID）；不同主机天然不撞名。显式 `--node` / `--self` 声明优先于派生值。

多节点视图与身份：`--node` 可重复声明节点（`id=<id>[,address=<host:port>][,vpc=<vpc>]
[,labels=<a;b>][,roles=<primary;standby>][,online=<bool>][,leader=<bool>]`）；`--self`
是上报给调用方的「本节点身份」（`ClusterInfoResponse.node_id`，serve 以它作为自身
身份参与选举归属判断），缺省取启动时的 leader。mock 无法区分调用方，多个 serve
共用同一 mock 会共享身份，适合单节点联调。

```bash
dist/bin/stateflux cluster --self node-1 \
    --node id=node-1,address=127.0.0.1:9090,roles=primary,leader=true \
    --node id=node-2,address=127.0.0.1:9092,roles=standby

# 故障切换演练：运行期切换 leader，serve 在下一个视图刷新周期（默认 3s）接管
curl -X POST http://127.0.0.1:9191/admin/leader -d '{"node_id":"node-2"}'
```

HTTP 面另提供 `/healthz` / `/readyz`；gRPC 面注册了 server reflection，可直接用
grpcurl 排障。SIGINT/SIGTERM 优雅退出。

除 flag 外，`cluster` 子命令同样支持 `--config` 配置文件与 `STATEFLUX_*` 环境变量
（`cluster.mock_server` 段，优先级同 serve：默认值 → 文件 → 环境变量 → flag）：

```yaml
cluster:
  mock_server:
    grpc_addr: 127.0.0.1:9190
    http_addr: 127.0.0.1:9191
    self: node-1
    nodes:
      - "id=node-1,address=127.0.0.1:9090,roles=primary,leader=true"
      - "id=node-2,address=127.0.0.1:9092,roles=standby"
```

### 集群视图查询（info 子命令）

`stateflux info` 只读拉取一份集群视图并打印**全部信息**，不启动任何监听：输出首行
是本二进制的构建信息（`build: version=... commit=... builddate=... goversion=...`，
编译注入，与 `--version` 同源），随后是上报身份、调度节点、每个
节点的地址 / vpc / 标签 / 角色 / 推导权限 / 在线与 leader 状态。
目标经 `--cluster-dsn` 指定，语义与 serve 完全一致（`local://` 静态视图、
`grpc://host:port`、`http://host[:port]`，`timeout` / `connect_timeout` 走 URL
query）；缺省取配置的 `cluster.dsn`（默认 `local://localhost`），同样支持
`--config` 文件与环境变量。查询走的正是 serve 侧的消费路径客户端
（`internal/cluster.Query`）：

```bash
# 默认：local 静态单节点视图
stateflux info

# 查询 mock 集群服务
stateflux info --cluster-dsn grpc://127.0.0.1:9190
stateflux info --cluster-dsn 'http://127.0.0.1:9191?timeout=2s'
```


## 配置

装载优先级：**默认值 → 配置文件（`--config <file>`，yaml/toml/json 等）→ `STATEFLUX_*`
环境变量 → 命令行 flag**（serve / cluster / info 子命令同规则；cluster 读
`cluster.mock_server` 段、info 读 `cluster.dsn`，见上文）。serve 的命令行 flag 仅 `--config`、
`--pg-dsn`、`--redis-addrs`、`--cluster-dsn`、`--grpc-addr`、`--http-addr`；其余经环境变量或
配置文件调整（`STATEFLUX_` 前缀，路径 `.` → `_`，
如 `runtime.wal_dir` → `STATEFLUX_RUNTIME_WAL_DIR`；列表用逗号分隔）。

仓库自带一份全节点共用的模板配置 [internal/config/config.yaml](./internal/config/config.yaml)
（也是 `make install` 安装包内的 `config.yaml`）：所有节点下发**同一份**，`cluster.dsn` 指向
**本机**外置集群服务（`grpc://127.0.0.1:9190`，每台机器用同一文件运行 `stateflux cluster`，
节点身份由主机信息派生、天然不撞名）；`pg` / `redis` 指向**共享**实例，部署时按环境改好后
统一下发，节点级临时差异用环境变量 / flag 覆盖。

### 构建产物与安装包

三个构建目标统一输出到 `dist/` 下各子目录，产物文件名均含 version。version 的唯一
来源是 [build/version](./build/version) 版本历史文件（每行 `${alias}.${major}.${minor}`
如 `chronos.0.1`，末行为当前版本），经 `go:embed` 嵌入二进制（根包 `embed.go`）——
因此**产物名里的 version 与二进制 `--version` / `stateflux info` 报告的 version 始终
一致**，且不受构建环境（git / CI）影响；commit / builddate 仍为编译注入：

| 目标 | 产物 | 说明 |
| --- | --- | --- |
| `make build` | `dist/bin/stateflux` | 本地开发二进制（同样带构建信息） |
| `make install` | `dist/package/stateflux-<version>-<os>-<arch>.tar.gz` | 离线安装包（可交叉编译） |
| `make docker` | `dist/image/stateflux_<version>_linux_<arch>.tar.gz` | Docker 镜像（tag `stateflux:<version>` 及 `latest`，`docker save` 导出） |

安装包部署（解压后 `sudo ./install.sh` 安装到 `/usr/local/stateflux`）：

```bash
make install
mkdir -p /tmp/stateflux && tar -xzf dist/package/stateflux-*.tar.gz -C /tmp/stateflux
cd /tmp/stateflux && sudo ./install.sh   # 安装到 /usr/local/stateflux
```

**包内布局与安装后目录保持一致**（除三个脚本外即 `/usr/local/stateflux` 的内容）：
`install.sh`（安装）、`upgrade.sh`（升级）、`downgrade.sh`（降级，在旧版本包内执行）、
`bin/stateflux`（静态二进制，`CGO_ENABLED=0`）、`conf/config.yaml`（全节点共用配置）、
`VERSION`（构建信息）。
`install.sh` 幂等（重复执行视为升级），安装布局：

```text
/usr/local/stateflux/
├── bin/stateflux      # 二进制（符号链接到 /usr/local/bin/stateflux）
├── conf/config.yaml   # 配置文件
├── dumps/             # 升级/降级前的 pg_dump 备份（${timestamp}_${version}.dump）
└── VERSION            # 构建信息
```

`conf/config.yaml` 视为节点本地状态：已存在时**不覆盖**（包内新版差异旁路保存为
`conf/config.yaml.new`，人工确认合并）；旧版平铺布局（`$PREFIX/stateflux`、
`$PREFIX/config.yaml`）自动迁移；安装位置可经 `PREFIX=... ./install.sh` 覆盖。
每个节点安装后按序初始化与启动：

```bash
stateflux migrate --config /usr/local/stateflux/conf/config.yaml  # 0) 初始化 PG（幂等）
stateflux cluster --config /usr/local/stateflux/conf/config.yaml  # 1) 本机外置集群服务
stateflux serve  --config /usr/local/stateflux/conf/config.yaml   # 2) 节点服务
```

Docker：`make docker` 走与 `make install` 同一条打包+安装路径（`build/docker.sh` →
`build/Dockerfile` → `build/package.sh` → `install.sh`），镜像内即安装包部署后的
`/usr/local/stateflux` 布局，默认 `CMD` 为带上述配置启动 serve（监听地址经环境变量
放开为 `0.0.0.0`）；版本经 `--build-arg` 透传，镜像内二进制构建信息与镜像 tag 一致。

### 版本与迁移

版本历史 [build/version](./build/version)（嵌入二进制）每行一个版本
`${alias}.${major}.${minor}`（如 `chronos.0.1`），行序即升级顺序；发新版本 = 追加
一行 + 新增迁移文件。迁移文件位于 `internal/domain/migration/`，命名为
`${version}_${date}`（如 `chronos.0.1_20260929`），同一名下 `.sql` 与 `.go` 可共存：
SQL 为主（整文件单事务执行，支持 `$$` 函数体），Go 迁移用于 SQL 不便表达的部分、
与同名 SQL 同事务在其后执行；全部 SQL 嵌入二进制，`stateflux migrate` 执行时自包含。
当前 `chronos.0.1` 的初始化迁移由原 000001~000003 三个文件**合并为单个**
`chronos.0.1_20260929.sql`（七表账本 + 挪行函数/索引 + 唤醒触发器）。

PG 中维护 `version` 表：**每次升级（含首次安装）应用一个版本即插入一条
`{id, version, date}`**，已记录的版本跳过，重复执行幂等。

```bash
stateflux migrate                                # 初始化 / 升级到嵌入历史的最新版本
stateflux migrate --target chronos.0.1           # 升级到指定版本
stateflux migrate --target chronos.0.1 --rebuild # 重建 schema 至指定版本（降级流程内部使用）
stateflux migrate --generate chronos.0.2         # 开发期：同时生成该版本的 .sql + .go 迁移脚手架
```

升级用安装包内的 `upgrade.sh`，降级解压**旧版本安装包**执行其中的 `downgrade.sh`
（用包内文件完整回退；两者都需 `pg_dump` / `pg_restore`）：

```bash
sudo ./upgrade.sh       # 升级（新包内）：dump 当前库 → 替换 bin/conf → stateflux migrate
sudo ./downgrade.sh     # 降级（旧包内）：dump 当前库 → 替换回包内 bin/conf → rebuild → 回载该版本 dump
```

- 升级前自动 `pg_dump` 当前库到 `/usr/local/stateflux/dumps/${timestamp}_${version}.dump`，
  再替换二进制与配置（旧配置带时间戳备份为 `conf/config.yaml.bak-<ts>`），最后执行
  `stateflux migrate`（version 表记录新版本）；迁移失败时提示用该 dump 恢复。
- 降级同样先 dump 当前库，然后用**旧版本包内**的 `bin/stateflux`、`conf/config.yaml`、
  `VERSION` 替换已装文件（旧配置同样时间戳备份），`stateflux migrate --target <包版本>
  --rebuild` 重建该版本 schema（不写 version 记录），最后以 `pg_restore --data-only`
  加载该版本最近一份 dump（业务数据与 version 记录一并回填）——二进制、配置、数据库
  全部回退。前提是 dumps/ 下留存过该版本的备份（每次升级 / 降级都会自动留）。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `STATEFLUX_PG_DSN` | `postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable` | PG 连接串 |
| `STATEFLUX_PG_SEARCH_PATH` | `public` | 固定 search_path（避免用户名与 schema 同名冲突） |
| `STATEFLUX_PG_MAX_OPEN_CONNS` / `_MAX_IDLE_CONNS` | `30` / `10` | 连接池 |
| `STATEFLUX_REDIS_ADDRS` | `127.0.0.1:6379` | 逗号分隔；多个地址按 cluster 客户端连接 |
| `STATEFLUX_CLUSTER_DSN` | `local://localhost` | 集群视图 DSN，见上文 |
| `STATEFLUX_CLUSTER_MOCK_SERVER_GRPC_ADDR` / `_HTTP_ADDR` | `127.0.0.1:9190` / `127.0.0.1:9191` | `cluster` 子命令监听地址 |
| `STATEFLUX_CLUSTER_MOCK_SERVER_SELF` | 空（取启动时 leader） | `cluster` 子命令上报的调用方身份 |
| `STATEFLUX_CLUSTER_MOCK_SERVER_NODES` | 空（主机派生 ID 的默认单节点） | `cluster` 子命令节点规格串，逗号分隔（规格内用分号分隔列表） |
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
make build      # 构建 dist/bin/stateflux（入口 ./cmd）
make install    # 构建 dist/package/ 下 .tar.gz 安装包（见「构建产物与安装包」）
make docker     # 构建 Docker 镜像并导出到 dist/image/（tag stateflux:<version>）
make vet        # go vet ./...
make fmt        # gofmt
go test ./...   # 全量测试；internal/domain/data 下 *_it_test.go 为集成测试，需要本地 PG/Redis
```

生产 Redis 仅作为通道部署：其 AOF、复制、ACK 或 Stream PEL 均不构成 stateflux 的正确性假设。
