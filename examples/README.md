# stateflux examples

场景化测试与验证 harness：验证 stateflux 调度框架的调度语义本身
（任务生命周期、两阶段发布、定时触发、失败收敛、启动恢复）。

场景不经过任何 API——场景直接以调度框架的接缝驱动
（repo 落表、事件注入、handler 执行记录）。因此不依赖 apiserver/真实 dm/
ClickHouse，只需要 PostgreSQL 与 Redis；dispatch 的集群节点表走真实拉取
路径——harness 在进程内以 unix socket 起一个 dm 服务桩（单节点在线表，
节点 ID 即 NodeName）。

## 目录布局

```
maintain.sh              # 依赖维护：以 Docker 容器安装部署/卸载 Redis 与 PostgreSQL
testing.go               # 依赖初始化与重置（Setup/Reset）+ 场景共享句柄 H
main_test.go             # 测试入口：场景清单，顺序执行所有 case
scenarios/                 # 场景 case，一目录一场景、相互独立（可整体复制/删除）
  smoke/                   #   冒烟：schedule.Run 入口生命周期（转发任务驱动）
  timer/                   #   定时触发（cron 直发 / cron 落表并发布）
  failure/                 #   失败重试与终态收敛
  startup_recovery/        #   启动恢复：未完成行重派
  forward/                 #   持久化转发：生产方 Dispatcher → leader 落表重发
```

case 相互独立：测试任务与 spec 定义随 case 内聚（`task.go`），case 之间
无横向依赖，只依赖根目录的 `examples.H` 句柄。

新增 case：在 `scenarios/<case>/` 建子包，任务定义放 `task.go`、场景放
`scenario.go`（实现 `func Test(h *examples.H, t *testing.T)`），并登记到
`main_test.go` 的清单。

## 依赖部署（Docker）

```sh
cd examples
./maintain.sh install      # 拉镜像并启动容器，就绪后输出连接地址
./maintain.sh uninstall    # 停止并删除容器（镜像保留）
```

容器端口绑定在 127.0.0.1，与场景默认环境变量一致；镜像与端口可用环境
变量覆盖（见下表）。端口已被其他服务占用时（如此前用 apt 安装的原生
redis-server/postgresql），脚本直接复用该服务、不重复起容器，uninstall
也不会动它；复用的 PostgreSQL 密码需与 `SCHEDULE_STABLE_EXAMPLES_PG_PASSWORD`
一致，否则测试会整体 Skip 并给出提示。

## 运行

```sh
go test -tags=integration -v ./examples/
go test -tags=integration -v -run 'Test/定时触发' ./examples/   # 单场景
```

首次运行由 `migrations.Migrate` 创建测试库并应用真实的 `migrations/`（Atlas
版本化迁移）。依赖不可达时场景整体 Skip。

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `SCHEDULE_STABLE_EXAMPLES_PG_ADDR` | `127.0.0.1:5432` | PostgreSQL 地址 |
| `SCHEDULE_STABLE_EXAMPLES_PG_USER` | `postgres` | 用户名 |
| `SCHEDULE_STABLE_EXAMPLES_PG_PASSWORD` | `postgres` | 密码 |
| `SCHEDULE_STABLE_EXAMPLES_PG_DB` | `stateflux_examples` | 测试库名 |
| `SCHEDULE_STABLE_EXAMPLES_REDIS` | `127.0.0.1:6379` | Redis 地址 |
| `SCHEDULE_STABLE_EXAMPLES_REDIS_DB` | `15` | Redis 库号（case 间 FLUSHDB 隔离） |
| `SCHEDULE_STABLE_EXAMPLES_REDIS_IMAGE` | `redis:7-alpine` | maintain.sh 用的 Redis 镜像 |
| `SCHEDULE_STABLE_EXAMPLES_PG_IMAGE` | `postgres:14-alpine` | maintain.sh 用的 PostgreSQL 镜像 |
| `SCHEDULE_STABLE_EXAMPLES_REDIS_PORT` | `6379` | maintain.sh 容器端口 |
| `SCHEDULE_STABLE_EXAMPLES_PG_PORT` | `5432` | maintain.sh 容器端口 |

## 场景清单

| case（scenarios/） | 验证点 |
|---|---|
| smoke | `schedule.Run` 真实入口（以持久化转发行驱动）：落表 → 启动恢复重派 → 两阶段发布 → 分节点路由 → 转发任务执行 → 成功内联回写 → 停机返回 ctx.Err() |
| timer | 非 Durable 定时直发不过表（taskID=0、固定 Payload）；Durable 定时经 factory 落表并发布，重复 tick 幂等去重；禁用任务（Enabled=false）不注册 cron，场景窗口内零执行 |
| failure | Retries 执行内重试耗尽仍失败 → 保持 dispatched → 对账按 backend FAILURE 收敛 failed（ErrorSummary/FinishedAt） |
| startup_recovery | 服务重启后（Redis 全丢）首轮对账：pending/dispatching/dispatched 未完成行全部重派并恰好执行一次 |

测试任务均为各 case 内聚定义的可观测 echo 变体（记录执行、按需注入失败、
按需实现 Factory）；框架本身不内置业务任务。

## 已知边界

- 五段 cron 表达式最小粒度为 1 分钟，定时场景用 @every 描述符表达亚分钟节奏，
  走同一 CronScheduler 代码路径，不测真实五段 cron 时刻。
- 定时防重锁（D11，多节点 SETNX）在单节点 harness 下无法验证防重效果。
- callback 派生（D5）框架未实装，不在场景范围。
- 对账超时判定有 30s 预算下限，场景通过回拨 updated_at（AgeRows）驱动，
  不真实等待。
