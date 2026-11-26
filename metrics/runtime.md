# 指标：进程与依赖

覆盖进程标识、启动时间与 PostgreSQL/Redis 依赖健康。指标名与标签常量定义见
[catalog.go](catalog.go)。

## 1. 指标定义

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_up` | gauge / 1 | — | P0 | 进程存活。进程启动后置 1，正常停机前置 0（或依赖抓取失败判定）。它是最后兜底指标：为 0 时其余指标都不可信。 |
| `stateflux_start_time_seconds` | gauge / seconds | — | P1 | 进程启动的 Unix 时间戳（秒）。用 `time() - stateflux_start_time_seconds` 观察运行时长与意外重启。 |
| `stateflux_runtime_info` | gauge / 1 | `node` | P1 | 进程静态身份，值恒为 1。`node` 即启动时经 dm 的 `ListClusterNodesStatus` 获取的本节点 ID（响应 `node_id`），用于把抓取目标与集群节点对应起来，也用于与其它指标做 `group_left` 关联。 |
| `stateflux_postgres_up` | gauge / 1 | — | P0 | PostgreSQL 健康探针。建议复用 `states.Open` 的就绪检查或定时 `SELECT 1`；成功 1、失败 0。它反映 `task_persistents` 事实源是否可用。 |
| `stateflux_redis_up` | gauge / 1 | — | P0 | Redis 健康探针（`PING`）。成功 1、失败 0。Redis 同时承载队列、定时锁与账本，为 0 时触发、发布、执行与对账都会失效。 |

### 1.1 采集说明

- `stateflux_up`、`stateflux_start_time_seconds`、`stateflux_runtime_info` 在
  进程启动时设置一次；`runtime_info` 不要按请求/事件重复设置。
- `postgres_up` / `redis_up` 建议每 10–30s 探活一次，探活超时 3s；不要在每次
  业务操作时更新，否则会把延迟算进探活。
- 探活失败只置 0，不 panic、不退出：连接恢复后自动回到 1，交给告警与日志
  处理。
- 探活不得输出密码、DSN 或服务密钥；日志只输出组件名与错误类型。

## 2. 抓取配置

每个 stateflux 进程一个 target（不要经网关聚合）：

```yaml
scrape_configs:
  - job_name: stateflux
    scrape_interval: 15s
    static_configs:
      - targets:
          - node-1:9101
          - node-2:9101
        labels:
          cluster: task-system
```

如果节点 ID 只存在于 `stateflux_runtime_info`，用 recording rule 建立关联：

```promql
# 每个抓取目标对应的节点 ID
stateflux:runtime_info:node =
  max by (instance) (stateflux_runtime_info) * 0
  + on(instance) group_left(node) (stateflux_runtime_info)
```

更简单的做法是在抓取配置里直接用 `relabel_configs` 从部署元数据写入 `node`
标签，并保留 `runtime_info` 作为自描述兜底。

## 3. 查询

```promql
# 进程运行时长
time() - stateflux_start_time_seconds

# 依赖健康总览
stateflux_postgres_up
stateflux_redis_up

# 节点身份（每个 target 一行）
stateflux_runtime_info

# 抓取目标是否存活（Prometheus 内置）
up{job="stateflux"}
```

## 4. 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 进程不可达 | `up{job="stateflux"} == 0` | 抓取失败或进程退出 |
| 进程重启 | `resets(stateflux_start_time_seconds[1h]) > 0` | 1 小时内重启过，结合日志排查 |
| PostgreSQL 不可用 | `stateflux_postgres_up == 0` | 事实源不可用，发布与对账会失败 |
| Redis 不可用 | `stateflux_redis_up == 0` | 队列/锁/账本全部失效 |
| 依赖持续不可用 | `stateflux_postgres_up == 0 or stateflux_redis_up == 0` 持续 5m | 短抖动可恢复，持续失败需介入 |

## 5. 与日志的对应关系

启动与停机日志（英文，Info 级）按以下顺序输出，可与指标时间线对照：

```
stateflux: starting                  → 配置加载完成，即将连接数据库
stateflux: database ready            → PostgreSQL 就绪
schedule: assembling runtime         → wire 装配开始
schedule: runtime assembled          → 注册表与依赖就绪
schedule: initial reconciliation ... → 首轮对账开始/完成
schedule: worker launched            → 每个资源的 worker 启动
schedule: scheduler running          → 全部循环启动
schedule: shutdown initiated         → 收到取消信号
schedule: shutdown complete          → 循环与 worker 全部停止
```

若 `stateflux_up` 为 1 但日志停在某一阶段，说明启动流程卡在该阶段（通常是
数据库连接、dm 查询或 Redis 连接），用对应依赖指标与错误日志定位。
