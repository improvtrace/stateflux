# 业务任务表示例：task_biz_examples

> 本文档固化一份**业务任务表**的示例 schema 定义（源自 schedule-stable 早期
> 调度事实源表 `task_processings` 的全量字段版，后者的精简版演进为框架自持的
> `task_persistents`，见 `migrations/`，由 `states/ent/schema` 经 Atlas 生成）。
> 业务侧需要自己的持久化任务表（进度计数、批次归属、用户任务管理 API 等，
> 见 DESIGN.md §5.2 分层澄清）时，参照本定义建表，不动框架表。

## 定位

stateflux 的调度事实源是 `task_persistents`（框架自持，只记录调度阶段与
终态）。业务表的职责是**业务视图**：谁创建的任务、执行到哪一步、成功了多少，
供前端展示与运营查询。业务表与框架表的关联方式：

- 业务表自行维护主键；投递调度时以 `idempotency_key` 携带业务标识，消费端按
  其幂等（两阶段发布是至少一次投递，重复投递必现）；
- 调度侧状态以 `task_persistents.phase/outcome` 为准，业务侧不复制阶段字段，
  需要展示时按 id 查询框架表。

## DDL

```sql
CREATE TABLE IF NOT EXISTS task_biz_examples (
    id BIGINT NOT NULL,
    resource CHARACTER VARYING(50) NOT NULL,
    operator CHARACTER VARYING(50) NOT NULL DEFAULT '',
    priority SMALLINT NOT NULL DEFAULT 50,
    phase SMALLINT NOT NULL DEFAULT 0,
    outcome SMALLINT NOT NULL DEFAULT 0,
    error_summary CHARACTER VARYING(500) NOT NULL DEFAULT '',
    timeout_ms BIGINT NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_node CHARACTER VARYING(150) NOT NULL DEFAULT '',
    idempotency_key CHARACTER VARYING(150) NOT NULL DEFAULT '',
    payload jsonb,
    callback jsonb,
    parent_task_id BIGINT NOT NULL DEFAULT 0,
    vpc CHARACTER VARYING(50) NOT NULL DEFAULT '',
    node CHARACTER VARYING(150) NOT NULL DEFAULT '',
    label CHARACTER VARYING(150) NOT NULL DEFAULT '',
    hash_bucket SMALLINT NOT NULL DEFAULT 0,
    biz_race_labels jsonb,
    biz_race_entry CHARACTER VARYING(150) NOT NULL DEFAULT '',
    biz_group CHARACTER VARYING(50) NOT NULL DEFAULT '',
    biz_batch_id CHARACTER VARYING(50) NOT NULL DEFAULT '',
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

-- 幂等键部分唯一：空键（业务未提供）不参与去重
CREATE UNIQUE INDEX IF NOT EXISTS uk_task_biz_examples_idempotency_key
    ON task_biz_examples (idempotency_key) WHERE idempotency_key <> '';

-- 类型 + 阶段扫描：按类型统计各阶段存量
CREATE INDEX IF NOT EXISTS idx_task_biz_examples_resource_phase
    ON task_biz_examples (resource, phase);

-- 对账扫描：阶段 + 更新时间
CREATE INDEX IF NOT EXISTS idx_task_biz_examples_phase_updated
    ON task_biz_examples (phase, updated_at);
```

## 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | 任务 ID，雪花，客户端生成、非自增（写入方无需回读自增值即可组装派生关系） |
| resource | VARCHAR(50) | 任务资源名：决定 handler 与路由（路由 header `AttributeResource`），对应 `tasks.TaskSpec.Resource` |
| operator | VARCHAR(50) | 任务子类型（resource 的细化），与 resource 共同决定 handler |
| priority | SMALLINT | 调度优先级 0-100，默认 50，越大越优先 |
| phase | SMALLINT | 调度阶段，见下「阶段状态机」 |
| outcome | SMALLINT | 终态类别，仅 phase=terminal 时有效 |
| error_summary | VARCHAR(500) | 终态错误摘要（成功为空） |
| timeout_ms | BIGINT | 单次执行预算（毫秒），对账超时阈值 |
| max_attempts | INTEGER | 最大重试次数，重试由异步任务框架实施 |
| attempts | INTEGER | 发布轮数，每次认领 +1；执行内重试不计入 |
| claimed_node | VARCHAR(150) | 本次认领节点（诊断用） |
| idempotency_key | VARCHAR(150) | 业务幂等键，部分唯一索引去重 |
| payload | jsonb | 业务参数 |
| callback | jsonb | OnSuccess/OnError 派生规格（预留未实装） |
| parent_task_id | BIGINT | 派生来源任务 ID，0 = 非派生 |
| vpc | VARCHAR(50) | 目标节点属性：网络域，空 = 不限 |
| node | VARCHAR(150) | 目标节点属性：偏好节点 ID，空 = 任意 |
| label | VARCHAR(150) | 目标节点属性：节点匹配标签 |
| hash_bucket | SMALLINT | 目标节点属性：哈希桶 0-255，0 = 不限 |
| biz_race_labels | jsonb | 业务并发约束标签集合（机制待定） |
| biz_race_entry | VARCHAR(150) | 业务并发约束对象标识（机制待定） |
| biz_group | VARCHAR(50) | 业务分组键，框架不解释 |
| biz_batch_id | VARCHAR(50) | 工厂批次 ID，孤儿检测用 |
| finished_at | TIMESTAMPTZ | 终态时间，非终态为 NULL |
| created_at | TIMESTAMPTZ | 创建时间（不可变） |
| updated_at | TIMESTAMPTZ | 最后更新时间，对账扫描依据 |

## 阶段状态机

```
phase:  0 pending ──认领──▶ 1 dispatching ──发布成功──▶ 2 dispatched ──终态──▶ 3 terminal
            ▲                    │                            │
            └── requeue（发布失败/对账重派，attempts<max）◀─────┘
```

- 认领与发布确认分离（两阶段发布）：**发布成功才置 dispatched**，发布失败或
  中途崩溃的行停留在 dispatching，由对账回收。
- 由此产生**至少一次投递**：发布成功但未确认即崩溃的行会被重新发布，消费端
  必须按 `idempotency_key` 幂等。
- `outcome`：0 unset（零值，不是合法终态）/ 1 succeeded / 2 failed /
  3 dead（attempts ≥ max_attempts 时由对账写入）。

## 索引说明

| 索引 | 用途 |
|---|---|
| `uk_..._idempotency_key`（部分唯一） | 幂等去重；空键不参与（`WHERE idempotency_key <> ''`）。ent 表达不了部分索引，须在迁移 SQL 中维护 |
| `idx_..._resource_phase` | 按资源扫描待认领行（`phase=pending AND resource IN (...)`） |
| `idx_..._phase_updated` | 对账扫描在途行（`phase IN (dispatching, dispatched) AND updated_at < ...`） |

## 演进记录

该表原为 schedule-stable 的调度事实源 `task_processings`。v0 落地时调度框架
改为「dispatch 前落表 + 立即发布」的精简模型，调度侧不再需要 priority 水位
认领、attempts 轮数记账与 biz 并发预留列，框架表收敛为 `task_persistents`；
本定义保留全部字段，作为业务侧建表的参照样例。
