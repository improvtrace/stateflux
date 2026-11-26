# stateflux 指标契约

本目录定义 stateflux 的指标契约：名称、类型、单位、标签、语义、采集点、查询
与告警建议。`catalog.go` 是机器可读的唯一清单（埋点代码引用其中的名称与标签
常量），本目录的 Markdown 是评审与运维使用的完整说明。

> 当前状态：**只有定义，尚未埋点与导出**。落地时按 P0 → P1 → P2 分批接入；
> 指标名与标签一经发布即为兼容契约，不得随意重命名。语义变化应新增指标，
> 旧指标标记废弃。

## 1. 设计原则

指标围绕调度的完整生命周期组织，每一组指标回答一个明确的运维问题：

```
cron tick ──▶ listener ──▶ factory ──▶ 落表(pending) ──▶ 认领(dispatching)
    │                                                        │
    └── 单节点锁/防重                                          ▼
                                                          发布(dispatched)
                                                               │
                                                               ▼
                                                    queue:<resource>/<node>
                                                               │
                                                               ▼
                                                        worker 执行 handler
                                                               │
                                                               ▼
                                                   终态回写 / 对账收敛 / 重派
```

| 问题 | 指标组 | 文档 |
|---|---|---|
| 定时触发是否生效？是否重复触发？ | cron / listener | [scheduler.md](scheduler.md) |
| 批次是否落表、认领、发布？卡在哪一步？ | publisher | [scheduler.md](scheduler.md) |
| 任务是否路由到正确节点？节点表是否新鲜？ | dispatcher / dm | [dispatch.md](dispatch.md) |
| 任务是否执行、耗时多少、失败多少？ | handler / worker / queue | [execution.md](execution.md) |
| 在途任务是否积压、超时、收敛？ | reconciler / repo / ledger | [state.md](state.md) |
| 进程与依赖是否健康？ | runtime | [runtime.md](runtime.md) |

三条约束贯穿全部指标：

1. **低基数**：标签只放有限枚举或有限集合（节点、资源、队列、阶段、结果），
   禁止 `task_id`、UUID、payload、错误原文等无界值。
2. **可归因**：每个错误指标都带 `stage` / `reason` / `operation`，能直接定位到
   代码分支；每个计数指标都能对应到唯一的采集点。
3. **可聚合**：同一指标在不同节点、不同资源上可直接求和或分位数聚合，
   不依赖日志拼接。

## 2. 命名规范

| 规则 | 示例 |
|---|---|
| 统一前缀 `stateflux_` | `stateflux_cron_ticks_total` |
| Counter 以 `_total` 结尾 | `stateflux_publish_tasks_total` |
| 时间统一用秒，Histogram 以 `_seconds` 结尾 | `stateflux_task_execution_seconds` |
| 行数/节点数等计数用复数名词，不加 `_total`（非 Counter） | `stateflux_dispatch_candidate_nodes`、`stateflux_reconcile_inflight_rows` |
| 名称表达「对象 + 动作/状态」，不写实现细节 | `stateflux_publish_rows_requeued_total` |
| 不使用单位换算（毫秒、微秒） | 全部用秒 |

## 3. 标签规范

标签常量在 `catalog.go` 的 `Label*` 中定义。

| 标签 | 含义 | 取值域 | 基数控制 |
|---|---|---|---|
| `node` | 集群节点 ID | `node-1` … | 集群规模 |
| `resource` | 任务资源名 | `tasks.TaskSpec.Resource` | 任务类型数 |
| `queue` | 分节点队列名 | `queue:<resource>/<node>` | 资源数 × 节点数 |
| `handler` | 事件处理器名 | `cron` | 处理器数 |
| `trigger` | 触发来源 | `on_demand` / `periodic` / `cron` / `recovery` | 固定枚举 |
| `result` | 结果 | `ok` / `error` / `succeeded` / `failed` / `retried` / `created` / `duplicate` | 固定枚举（按指标） |
| `reason` | 失败/重派原因 | `no_candidates` / `node_table` / `send` / `signature` / `dispatch` / `missing_state` / `publish_failure` | 固定枚举（按指标） |
| `stage` | 处理阶段 | `factory` / `dispatch` / `lookup` / `create` / `claim` / `mark_dispatched` / `ledger` / `inflight` / `backend_state` / `requeue` / `mark_terminal` | 固定枚举（按指标） |
| `outcome` | 任务终态 | `succeeded` / `failed` / `dead` / `requeued` | 固定枚举 |
| `operation` | 数据库/账本/broker 操作 | `create` / `get` / `claim` / `mark_dispatched` / `mark_terminal` / `requeue` / `inflight` / `record` / `lookup` / `delete` / `publish` / `consume` | 固定枚举（按指标） |
| `method` | dm RPC 方法 | `list_cluster_nodes_status` | 固定枚举 |
| `state` | 节点在线状态 | `online` / `offline` | 固定枚举 |
| `event` | 调度事件类型 | `cron` | 固定枚举 |
| `kind` | 任务种类 | `durable` / `stateless` | 固定枚举 |

禁止使用的标签：`task_id`、`uuid`、`idempotency_key`、`payload`、`error`、
`error_message`、`dsn`、`service_key`、任意用户输入。

## 4. 类型与直方图桶

### 4.1 类型选择

- **Counter**：累计次数，只增不减；进程重启归零，查询用 `rate` / `increase`。
- **Gauge**：瞬时值，可升可降；用于积压、缓存年龄、在线节点数、worker 数。
- **Histogram**：耗时与批量大小；用于 P50/P95/P99 与分阶段耗时对比。

### 4.2 建议桶

耗时（秒）：

```
0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60
```

批次行数（`stateflux_publish_batch_rows`）：

```
1, 5, 10, 25, 50, 100
```

候选节点数（`stateflux_dispatch_candidate_nodes`）：

```
0, 1, 2, 3, 5, 8, 13, 21
```

说明：`_seconds` 直方图的桶可按实际 SLA 调整，但调整桶边界属于兼容变更
（分位数会变化），需要记录在变更说明中。

## 5. 指标全景

全部 63 个指标按组件分组，详细语义、采集点与查询见各组件文档。

| 组件 | 指标数 | P0 | 文档 |
|---|---:|---:|---|
| 定时触发（cron / listener） | 11 | 5 | [scheduler.md](scheduler.md#1-定时触发) |
| 发布通道（publisher） | 8 | 5 | [scheduler.md](scheduler.md#2-发布通道) |
| 发布器（dispatcher / dm / 节点表） | 12 | 3 | [dispatch.md](dispatch.md) |
| 对账（reconciler） | 7 | 4 | [state.md](state.md#2-对账) |
| 执行（handler / worker / queue） | 13 | 6 | [execution.md](execution.md) |
| 状态存储（repo / ledger） | 7 | 3 | [state.md](state.md) |
| 进程与依赖（runtime） | 5 | 3 | [runtime.md](runtime.md) |

## 6. 暴露与采集

- **端点（建议）**：`GET /metrics`（Prometheus 文本格式）；每个 stateflux 进程
  一个 target，不要通过网关聚合后再抓取。当前仓库尚未提供该端点，接入埋点时
  一并实现。
- **身份**：抓取目标上的 `stateflux_runtime_info{node="node-1"}` 提供节点身份；
  需要按节点聚合时用该标签与业务指标做 `group_left` 关联，而不是给所有指标都
  加 `node`。
- **OpenTelemetry**：若后续改用 OTel SDK，指标名映射为
  `stateflux.cron.ticks`（去掉 `_total`，点号分层），导出到 Prometheus 时再
  还原为下划线命名；语义与标签保持不变。
- **采样**：队列积压类 Gauge（`stateflux_queue_pending_messages`、
  `stateflux_queue_delayed_messages`）在抓取时或固定周期采样，不要求每次
  入队/出队都更新；其余 Counter/Histogram 在事件发生时同步更新。
- **进程重启**：Counter 归零是预期行为，告警必须用 `rate`/`increase`，
  不要用原始值判断。

## 7. 告警基线

以下为建议的第一批告警（P0）。阈值需按实际流量与 SLA 调整。

| 告警 | 表达式（示例） | 含义 |
|---|---|---|
| 调度器无 tick | `sum(rate(stateflux_cron_ticks_total[5m])) == 0` | cron 未触发或进程未运行 |
| 定时锁错误 | `rate(stateflux_cron_lock_errors_total[5m]) > 0` | Redis 异常，可能多节点重复触发 |
| 事件丢弃 | `rate(stateflux_cron_events_dropped_total[5m]) > 0` | listener 缓冲不足，触发被丢弃 |
| 发布失败 | `rate(stateflux_publish_errors_total[5m]) > 0` | 落表/认领/路由/确认失败 |
| 路由无节点 | `rate(stateflux_dispatch_errors_total{reason="no_candidates"}[5m]) > 0` | 节点约束无人满足，任务无法下发 |
| 执行失败率 | `rate(stateflux_task_executions_total{result="failed"}[5m]) / rate(stateflux_task_executions_total[5m]) > 0.05` | 业务 handler 失败率过高 |
| 终态回写失败 | `rate(stateflux_task_writeback_errors_total[5m]) > 0` | 成功任务无法落终态，会被对账重派 |
| 在途超时 | `rate(stateflux_reconcile_rows_timed_out_total[5m]) > 0` | 任务卡住或执行预算过小 |
| 对账错误 | `rate(stateflux_reconcile_errors_total[5m]) > 0` | 数据库或 Redis 异常 |
| 数据库错误 | `rate(stateflux_db_operations_total{result="error"}[5m]) > 0` | 调度事实源不可用 |
| Redis 不可用 | `stateflux_redis_up == 0` | 队列/锁/账本全部失效 |
| PostgreSQL 不可用 | `stateflux_postgres_up == 0` | 事实源不可用 |

## 8. 维护规则

新增或修改指标时：

1. 先在 `catalog.go` 登记名称常量与 `Metric` 定义；
2. 在同目录对应组件文档补充语义、采集点、查询与告警；
3. 埋点代码只引用 `metrics.*` 常量，不写裸字符串；
4. `go test ./metrics/` 会校验命名前缀、类型后缀、标签白名单、优先级与
   核心链路覆盖；
5. 删除或重命名指标属于破坏性变更，必须先确认没有看板/告警依赖。
