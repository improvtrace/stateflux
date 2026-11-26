# 指标：发布器、节点表与 dm

覆盖 `dispatch/`：`Dispatcher.Dispatch` 的路由与投递、`nodeTable` 的缓存刷新、
`dmCluster` 的 unix socket RPC。指标名与标签常量定义见
[catalog.go](catalog.go)。

## 1. 执行流程与采集点

```
Dispatcher.Dispatch(sig)
  ├─ ① DispatchAttemptsTotal{resource}
  ├─ RoutingKey 为空 → attachRoutingKey
  │    ├─ nodeTable.get(ctx)
  │    │    ├─ 缓存未过期（TTL 10s） ② DispatchNodeTableAgeSeconds
  │    │    └─ 缓存过期 → dm 查询
  │    │         ├─ ③ DispatchNodeTableRefreshesTotal{result}
  │    │         ├─ ④ DispatchNodeTableRefreshSeconds
  │    │         ├─ ⑤ DMRequestsTotal / DMRequestSeconds
  │    │         ├─ ⑥ DMConnectErrorsTotal（懒连接失败）
  │    │         └─ ⑦ DispatchNodeTableNodes{state=online|offline}
  │    ├─ matchNodes(vpcs, roles, labels)
  │    │    └─ ⑧ DispatchCandidateNodes{resource}
  │    ├─ 无候选 → ⑨ DispatchErrorsTotal{reason="no_candidates"}
  │    └─ pickNode → queue:<resource>/<node>
  ├─ SendTaskWithContext
  │    ├─ 失败 → ⑩ DispatchErrorsTotal{reason="send"} / {reason="node_table"}
  │    └─ 成功 → ⑪ DispatchRoutedTotal{resource,node}
  └─ ⑫ DispatchSeconds{resource,result}
```

## 2. 指标定义

### 2.1 发布与路由

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_dispatch_attempts_total` | counter / 1 | `resource` | P0 | 交给 Dispatcher 的 Signature 数。`resource` 取 `AttributeResource`，缺省取 `Signature.Name`。在 `Dispatch` 入口计数，包含最终失败的投递。 |
| `stateflux_dispatch_seconds` | histogram / seconds | `resource`, `result` | P0 | 单条 Signature 的路由 + 投递耗时。`result=ok/error`。P95 突增通常来自 dm 查询或 Redis 发布阻塞。 |
| `stateflux_dispatch_errors_total` | counter / 1 | `resource`, `reason` | P0 | 投递失败次数。`reason=no_candidates`（无在线节点满足 vpc/role/label 约束）、`node_table`（dm 查询失败）、`send`（Redis 发布失败）。 |
| `stateflux_dispatch_routed_total` | counter / 1 | `resource`, `node` | P1 | 成功投递到 `queue:<resource>/<node>` 的次数。用于观察分节点负载是否均衡，以及某个节点是否被约束排除。 |
| `stateflux_dispatch_candidate_nodes` | histogram / nodes | `resource` | P2 | 每次路由决策的候选节点数（在线且满足约束）。长期为 1 说明没有调度弹性；为 0 会触发 `no_candidates` 错误。 |

### 2.2 节点表缓存

`nodeTable` 按固定 TTL（10s）缓存 dm 返回的节点表；刷新失败时保留旧表并返回
错误，本次发布中止，下一轮再试。

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_dispatch_node_table_refreshes_total` | counter / 1 | `result` | P1 | 缓存过期触发的刷新次数。`result=ok/error`。`error` 表示 dm 不可达或返回异常；此时发布侧会拒绝本次路由。 |
| `stateflux_dispatch_node_table_refresh_seconds` | histogram / seconds | — | P2 | 一次节点表刷新的耗时（含懒连接与 RPC）。 |
| `stateflux_dispatch_node_table_nodes` | gauge / nodes | `state` | P1 | 最近一次成功刷新得到的节点数，`state=online/offline`。用于发现节点批量下线或 dm 返回空表。 |
| `stateflux_dispatch_node_table_age_seconds` | gauge / seconds | — | P2 | 最近一次路由决策时节点表缓存的年龄。正常应 < TTL（10s）；持续超过 TTL 说明刷新持续失败，发布会被拒绝（旧表保留在内存但不再用于路由）。 |

### 2.3 dm 客户端

| 指标 | 类型/单位 | 标签 | 优先级 | 语义与采集点 |
|---|---|---|---|---|
| `stateflux_dm_requests_total` | counter / 1 | `method`, `result` | P1 | 发往 dm 的 RPC 次数。当前只有 `method=list_cluster_nodes_status`；`result=ok/error`。 |
| `stateflux_dm_request_seconds` | histogram / seconds | `method` | P1 | dm RPC 耗时（含首次懒连接）。连接超时为 3s，P99 接近 3s 说明 dm 侧异常。 |
| `stateflux_dm_connect_errors_total` | counter / 1 | — | P1 | 建立 dm unix socket 连接失败次数。非 0 时所有路由都会失败，需检查 socket 路径、dm 进程与权限。 |

## 3. 查询

```promql
# 路由成功率
sum(rate(stateflux_dispatch_routed_total[5m]))
  /
sum(rate(stateflux_dispatch_attempts_total[5m]))

# 失败原因分布
sum by (reason) (rate(stateflux_dispatch_errors_total[5m]))

# 分节点投递量（观察倾斜）
sum by (node) (rate(stateflux_dispatch_routed_total[5m]))

# 候选节点数 P50/P95
histogram_quantile(0.5,
  sum by (le, resource) (rate(stateflux_dispatch_candidate_nodes_bucket[5m])))

# 节点表在线数
stateflux_dispatch_node_table_nodes{state="online"}

# 节点表缓存年龄
stateflux_dispatch_node_table_age_seconds

# dm 请求 P95
histogram_quantile(0.95,
  sum by (le, method) (rate(stateflux_dm_request_seconds_bucket[5m])))
```

## 4. 告警

| 告警 | 表达式 | 说明 |
|---|---|---|
| 无可用节点 | `rate(stateflux_dispatch_errors_total{reason="no_candidates"}[5m]) > 0` | vpc/role/label 约束无人满足，任务无法下发 |
| 节点表查询失败 | `rate(stateflux_dispatch_node_table_refreshes_total{result="error"}[5m]) > 0` | dm 不可达或返回异常 |
| dm 连接失败 | `rate(stateflux_dm_connect_errors_total[5m]) > 0` | socket 路径/权限/dm 进程问题 |
| 节点表陈旧 | `stateflux_dispatch_node_table_age_seconds > 30` | 刷新持续失败，路由可能基于过期节点表 |
| 节点全部离线 | `stateflux_dispatch_node_table_nodes{state="online"} == 0` | 集群无在线节点，所有任务都会失败 |
| 投递倾斜 | `max by (node) (rate(stateflux_dispatch_routed_total[5m])) / sum(rate(stateflux_dispatch_routed_total[5m])) > 0.8` | 单节点承担 80% 以上流量，检查 hash_bucket/claimed_node/约束 |
| 投递延迟 | `histogram_quantile(0.95, sum by (le) (rate(stateflux_dispatch_seconds_bucket[5m]))) > 1` | Redis 或 dm 变慢 |

## 5. 标签与基数说明

- `node` 只出现在 `dispatch_routed_total`，基数等于集群节点数；其它指标复用
  `runtime_info{node=...}` 做关联。
- `resource` 基数等于已注册任务类型数；`reason` 与 `method` 是固定枚举。
- 不要把 `vpc`、`label`、`target_node` 作为指标标签：它们的组合基数不可控；
  需要排查时看 Debug 日志（日志里输出约束与候选节点数）。
