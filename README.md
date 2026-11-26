# stateflux

基于定时触发的任务调度框架，由 pam 的 `backend/internal/schedule-stable` 搬运而来
（Go 包名沿用 `schedule`，import 路径 `github.com/improvtrace/stateflux`）。
设计与决策记录见 [docs/DESIGN.md](docs/DESIGN.md)。

cron 到点 → listener 分发事件 → CronScheduler 取 factory 批次 → publisher 落表
发布 → dispatch 按集群节点表路由（queue:<resource>/<node>）→ queue 执行 →
对账收敛与重派。

其他节点提交持久化任务只需经 Dispatcher：带 `AttributePersistent` 标记的
Signature 会被包装成 `persistent_forward` 仅投递到 leader，由 leader 侧
`tasks/persistents` 先落 task_persistents 再重新发布（DB 写入收敛在 leader）。

## 目录布局

```
stateflux/                       # 根包 package schedule：entry（Run + wire 装配集）+ scheduler（Listener + CronScheduler + Publisher）
├── api/dm/                      # dm 服务集群节点查询接口的精简 proto 与生成码（dispatch 节点表来源）
├── cmd/
│   ├── stateflux/               # 服务入口（cobra）：装配配置、校验数据库就位、运行调度器
│   ├── datamigrate/             # 数据库迁移的独立工具（cobra）
│   └── mock-dm/                 # dm 服务的本地桩（cobra）：按节点数自动生成集群节点表
├── config/                      # 配置：config.go + config.conf.example（ini）
├── dispatch/                    # 发布器：节点匹配 + 分节点路由 + 持久化转发
├── docs/                        # 设计与决策记录（DESIGN.md）与业务任务表示例 schema
├── examples/                    # 场景化集成测试（见 examples/README.md）
├── migrations/                  # 数据库迁移的全部逻辑
│   ├── *.sql, atlas.sum         #   版本化迁移（由 ent schema 经 Atlas 生成，不手写）
│   ├── migrate.go               #   加载（Dir）、查询待应用项（Pending）、执行（Migrate，目标库不存在时先建库）
│   ├── format/                  #   迁移 SQL 的美化格式器
│   └── gen/main.go              #   由 ent schema 生成增量迁移的工具
├── metrics/                     # 指标契约：名称/类型/标签/语义/采集点/查询与告警（见 metrics/README.md）
├── queue/                       # machinery fork：异步执行框架（Redis only）
├── states/                      # 调度事实源 task_persistents 的数据访问
│   ├── task_persistent.go       #   领域类型、repo 接口与 ent 实现
│   ├── client.go                #   连接构造（Open 含启动期就绪检查 / NewClient）
│   ├── ledger.go                #   发布侧的加速索引
│   ├── reconcile.go             #   对账：在途超时收敛 + pending 重派
│   └── ent/                     #   ent schema（表结构的唯一来源）与生成码
└── tasks/                       # 任务定义核心包（TaskSpec/Registry/Factory）；业务任务按目录新增
    └── persistents/             #   持久化转发消费侧：persistent_forward 落表后重发（不生产任务）
```

## 运行

```sh
go build -o bin/ ./cmd/datamigrate ./cmd/mock-dm ./cmd/stateflux

bin/mock-dm --config stateflux.conf --nodes 3   # 本地没有 dm 时：起 dm 桩，先于 stateflux
bin/datamigrate --config stateflux.conf   # 先迁移数据库（库不存在时自动建库；重复执行幂等）
bin/stateflux --config stateflux.conf     # 再启动服务
```

本地没有真实 dm 服务时，用 `mock-dm` 顶替：它复用同一份配置的 `[dm]` 段（监听
地址与服务密钥），按 `--nodes` 自动生成集群节点表（`node-1` … `node-N`，首节点
LEADER、其余 FOLLOWER，全部在线）。`stateflux` 启动时经 `ListClusterNodesStatus`
获取本节点 ID（响应的 `node_id`，不再由配置指定）：单实例直接用默认的 `node-1`；
本地模拟多节点时，为每个实例各起一个 `mock-dm`（不同 socket，`--node-id node-K`
指定该实例的身份），各实例的 `[dm] address` 指向自己的桩。

配置只来自配置文件（ini）与命令行参数，不读取环境变量；显式给出的参数优先于
配置文件。完整配置项及说明见 [config/config.conf.example](config/config.conf.example)：

```ini
[postgres]
dsn = postgres://postgres:postgres@127.0.0.1:5432/stateflux?sslmode=disable

[redis]
addrs = 127.0.0.1:6379
db = 0

[dm]
address = /tmp/pam/dm.sock
service_key = ...

[schedule]
reconcile_interval = 30s
```

| 命令 | 使用的配置段 | 说明 |
|---|---|---|
| `stateflux` | 全部 | 运行调度器，阻塞直到收到 SIGINT/SIGTERM；调度表不存在或 dm 不可达时启动即失败 |
| `datamigrate` | `[postgres]` | 应用待执行的迁移（目标库不存在时先建库）；`--dry-run` 只列出待应用项，不改动数据库 |
| `mock-dm` | `[dm]` | dm 服务的本地桩：在 `[dm] address` 上监听，按 `--nodes` 自动生成集群节点表、`--node-id` 指定响应里的本节点 ID；`--nodes 0`、本节点不在表内等非法值启动即失败 |

`stateflux` 的日志默认以 INFO 文本格式输出到 stderr；`--log-level debug` 打开
逐行发布、消费与对账细节，`--log-format json` 输出结构化日志供采集。日志内容
统一用英文（代码注释与文档保持中文）。指标契约（命名、标签、语义、采集点、
PromQL 与告警基线）见 [metrics/README.md](metrics/README.md)。

DSN 所指的数据库不存在时，`datamigrate` 先用 DSN 里的同一账号经维护库 `postgres`
建库（账号须有 `CREATEDB` 权限）；`--dry-run` 不改动数据库，此时只报错。
首次迁移要求数据库是空库（只允许存在账本表 `atlas_schema_revisions`）。

## 表结构变更

表结构的唯一来源是 `states/ent/schema`，迁移文件不手写，由 ent 内置的 Atlas
生成：

```sh
go generate ./states/ent   # 修改 schema 后重新生成 ent 代码
go run migrations/gen/main.go -name add_xxx \
  -dev-url 'postgres://postgres:postgres@127.0.0.1:5432/stateflux_dev?sslmode=disable'
```

`-dev-url` 指向一个空库，仅用于计算 diff。生成的迁移如实反映 schema 变更——
删列、删索引也会生成 DROP 语句，提交前请评审。

## 作为库使用

```go
dsn := "postgres://user:pass@host:5432/stateflux?sslmode=disable"

applied, err := migrations.Migrate(ctx, dsn) // 迁移：部署步骤，库不存在时先建库，重复执行幂等

client, err := states.Open(ctx, dsn)          // 校验连接可达且调度表已就位
defer client.Close()

err = schedule.Run(ctx, schedule.Config{
	RedisAddrs:   []string{"127.0.0.1:6379"},
	DMAddress:    "/tmp/pam/dm.sock", // dm 服务的 unix socket：节点表与本节点 ID 来源
	DMServiceKey: "...",             // dm 内部接口的服务密钥
	Repo:         states.NewTaskPersistentRepo(client),
})
```

## 测试

```sh
go test ./...                                   # 单元测试：配置装配、节点路由、迁移 SQL 格式器、建库 DSN 推导、dm 桩
cd examples && ./maintain.sh install && cd ..   # 以 Docker 部署 PostgreSQL 与 Redis
make integration                                # 集成测试：migrations 建库迁移 + examples 场景
```
