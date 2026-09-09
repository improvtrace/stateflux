# Vault / OpenBao 凭据（密码）轮转机制调研报告

> 版本：v1（2026-09-08）
> 调研目的：为 stateflux 的轮转类周期任务（密码/凭据轮转）设计提供参照——重点回答两个问题：
> ① 密码轮转在 Vault/OpenBao 里如何建模（状态、执行、一致性）；② 轮转的调度器如何设计与运作（触发、错过、失败、HA）。
> 事实来源：OpenBao `main` 分支源码（MPL-2.0）、HashiCorp Vault 官方文档与 `main` 分支源码、Vault Enterprise 文档。链接见 §9。

## 1. 结论先行

1. **Vault/OpenBao 把"密码轮转"拆成两种互补范式**：动态秘密（dynamic secrets）用"短寿命 + 到期撤销"让大部分凭据根本不需要轮转；静态角色（static roles）轮转负责必须长期存在的既有账号密码。前者是"按需获取新凭据"，后者才是真正的"改密码"。
2. **轮转调度器是一个进程内的优先级队列 + 短周期 ticker，不是分布式 cron**。每条待轮转凭据是一条队列项 `{key=角色名, priority=下次轮转时间的 Unix 秒}`；一个 5 秒的 ticker 不断弹出到期项执行，弹到第一个未到期项即停。调度分辨率是秒级近似（文档明确 TTL 读数因此是近似的）。
3. **调度器单实例化靠"角色"而不是靠分布式锁**：只有 active（primary）节点启动轮转队列，standby/DR 副本完全不建队列；active 切换后新主从持久化存储重建队列继续跑。这与 stateflux"外部选举指定唯一调度节点"是同一原则。
4. **时间状态只有一份权威：`last_vault_rotation`（上次成功轮转时间）**。下次轮转时间永远是现算的（period 模式 = `last + period`；cron 模式 = `schedule.Next(now)`），不作为独立的待执行事实持久化，避免了"存的时间点漂移"这一类 bug。
5. **错过就放弃，不补跑**：cron 调度配 `rotation_window`（默认无界，最小 1 小时），过了窗口本次轮转直接作废、跳到下一次计划时间。轮转的语义是"周期性意图"而非"一次性任务"，漏一次不该引发堆积式追跑。
6. **半失败靠 WAL（write-ahead log）恢复**：轮转是"改远端系统密码 + 写本地存储"两步，中间崩溃会留下"新密码已设进数据库但 Vault 不知道"的危险态。实现顺序是：生成新密码 → 写 WAL → 改数据库 → 更新角色存储 → 删 WAL；重启时逐条对账 WAL 与角色记录的 `last_vault_rotation` 时间戳，决定重放（用 WAL 里的密码重试）或丢弃。
7. **失败重试是"退避 + 队内重排"，不是死信队列**：OSS 版失败后把队列项的 priority 加 10 秒再入队；Enterprise 1.19+ 的统一轮转框架提供策略化重试（初始延迟 + 指数退避 + 随机抖动 + 上限封顶），超过重试上限把条目标记为 **orphaned**（孤儿）：停止自动轮转、要求人工介入。
8. **OpenBao 与 Vault 的差距在功能面而不在架构**：OpenBao（fork 自 Vault 1.14，MPL 许可）的静态角色轮转目前只支持 `rotation_period`（周期），不支持 cron 的 `rotation_schedule`/`rotation_window`；连接级根凭据只有手动 `rotate-root`，没有计划轮转。调度内核（队列、ticker、WAL、单 active）两者同源同构。

## 2. 背景与调研对象

- **HashiCorp Vault**：密钥/凭据管理系统。核心抽象是 secrets engine（秘密引擎插件）+ lease（租约）。许可证为 BUSL-1.1（源码可见，非开源许可）。
- **OpenBao**：2023 年从 Vault MPL 分支社区化延续的项目，API 与内核同源；本文源码分析以 OpenBao `main`（`internal/builtin/logical/database/`）为主，Vault `main`（`builtin/logical/database/`）为对照，两者该模块高度一致。
- 本文所有"Enterprise/Vault 企业版"标注的功能指 Vault 付费版；OpenBao 社区版不含。

| 范式 | 解决的问题 | 是否"改密码" | 版本 |
| --- | --- | --- | --- |
| 动态秘密 + 租约 | 按需短命凭据（DB 账号、云 AK/SK） | 否，到期自动撤销 | OSS |
| 静态角色轮转 | 既有账号的密码托管与定期轮换 | 是 | OSS（cron 调度部分 OSS 1.16+） |
| 连接级根凭据轮转 | 引擎自身持有的管理员密码 | 是 | 手动=OSS；计划轮转=Enterprise 1.18+ |
| 自动轮转框架（rotation policies） | 统一各引擎轮转的调度/重试/孤儿治理 | 是 | Enterprise 1.19+ |

## 3. 范式一：动态秘密与租约——用"短寿命"替代"轮转"

动态秘密在每次读取时**生成全新凭据**（如 `database/creds/my-role` 返回一个临时 DB 用户），并附一个租约：

- **TTL**：凭据只在租约期内有效，到期由 Vault 的内部撤销系统自动删除远端凭据（如删除 DB 用户、吊销云密钥）。
- **续租（renew）**：续租时长是"从当前时间起的增量"而非累加，且服务端可裁剪（increment 是建议值）；`max_ttl` 封顶，到期不可续。
- **撤销（revoke）**：支持按租约 ID 精确撤销，也支持按**前缀树**批量撤销（如 `vault lease revoke -prefix aws/`），泄漏时能整棵吊销。
- **运行位置**：租约到期检测由过期管理器（expiration manager）在 active 节点执行（内部为内存数据库 + 按到期时间组织的优先队列 + WAL），与静态轮转共用"单 active 调度"原则。

**设计思想**：如果凭据天然只能活 1 小时且每次使用都换新，"轮转"就退化成"重新获取"——不存在一个需要保守的长期秘密，也就没有"改密码窗口期"的同步问题。这是 Vault 对密码轮转的第一性回答：**能用生命周期解决的，不用调度解决**。

## 4. 范式二：静态角色轮转（static roles）——真正的密码轮转

### 4.1 模型

静态角色是 Vault 角色与既有数据库用户的 **1:1 映射**：Vault 托管该用户密码（首次导入即强制轮转一次，使 Vault 成为唯一持有者），之后按配置周期或计划自动改密。应用侧从 `database/static-creds/<role>` 读当前密码（返回 `username/password/last_vault_rotation/rotation_period 或 rotation_schedule/ttl`），或经 Agent 模板 / Vault Secrets Operator 消费。

关键配置（字段名按 API 文档）：

| 字段 | 语义 | 约束 |
| --- | --- | --- |
| `rotation_period` | 周期轮转间隔（秒），默认 24h | 最小 5s（OpenBao 实现 `defaultQueueTickSeconds`） |
| `rotation_schedule` | cron 表达式（5 字段） | 与 `rotation_period` **互斥**；Vault 1.20.5+ 按 UTC 解释 |
| `rotation_window` | 计划触点后的允许窗口 | 仅与 `rotation_schedule` 搭配，最小 1h；默认无界 |
| `disable_automated_rotation` | 关闭自动轮转 | 置 true 同时重置 TTL（period 模式） |
| `skip_import_rotation` | 导入时不立即轮转一次 | Enterprise |
| `credential_type` / `credential_config` | 凭据类型：`password` / `rsa_private_key` / `client_certificate`，可挂 `password_policy` | 密码默认策略：20 位，含大小写、数字、连字符各 ≥1 |

### 4.2 语义要点

- **错过窗口不补跑**：cron 触点过了 `rotation_window` 后，本次轮转作废，等下一个计划点（`IsInsideRotationWindow` 判定，见 §6.3）。
- **手动轮转不重置计划**：手动触发一次轮转后，`next_vault_rotation` 仍按原计划推进（Enterprise 文档明确）。
- **不要把真 root 用户配成静态角色**：Vault 轮转时不区分普通凭据和根凭据，会把引擎自己用来管理用户的那个账号密码也改掉。根凭据走范式三。
- **旧密码无宽限期**：轮转成功旧密码立即失效；下游通过轮询/推送 `static-creds` 感知。这是静态轮转固有的同步风险，Vault 的缓解手段是让应用尽量改用动态秘密，而不是在轮转器里做双活密码。

## 5. 范式三：连接级根凭据轮转

数据库引擎连接配置里保存着引擎自己的管理员凭据（用于创建/撤销动态用户）。轮转它有两条路：

- **手动（OSS）**：`POST database/rotate-root/:name`。执行后旧密码永久不可读——Vault 用随机密码改掉远端账号并只存于内部。文档强烈建议为 Vault 建专用账号而非复用真 root。OpenBao 同名接口 `bao write -force database/rotate-root/...`，支持 PostgreSQL/MySQL/Cassandra/InfluxDB/Valkey 等；不支持凭据对（如 MongoDB Atlas 密钥对）的插件不可用。
- **计划轮转（Enterprise 1.18 引入，1.19 扩展插件面）**：连接配置上加 `rotation_period` / `rotation_schedule`（最小 10s）/ `rotation_window` / `disable_automated_rotation` / `password_policy` / `root_rotation_statements`（自定义改密 SQL），复用静态角色同一套调度内核。

## 6. 调度实现剖析（源码级）

以下以 OpenBao `internal/builtin/logical/database/rotation.go`、`path_roles.go` 与 Vault `builtin/logical/database/`（rotation.go、schedule/schedule.go、path_roles.go）为准。

### 6.1 调度器形态：优先级队列 + 5 秒 ticker

```go
// OpenBao rotation.go
const defaultQueueTickSeconds = 5
const queueTickIntervalKey   = "rotation_queue_tick_interval" // 可配置 tick 间隔

func (b *databaseBackend) runTicker(ctx context.Context, queueTickInterval time.Duration, s logical.Storage) {
    tick := time.NewTicker(queueTickInterval)
    for {
        select {
        case <-tick.C:            b.rotateCredentials(ctx, s)   // 每 tick 批量处理到期项
        case <-ctx.Done():        return
        }
    }
}
```

- 队列项：`queue.Item{Key: roleName, Priority: role.StaticAccount.NextRotationTime().Unix()}`，最小堆按 Priority 出队。
- 每个 tick **连续弹出到期项直到遇到第一个未到期项即停**（`rotateCredentials` 循环 `rotateCredential`，后者对 `time.Now() < item.Priority` 直接把项塞回去并返回 false）——一个天然批处理，且避免全队列扫描。
- 队列是**纯内存**的，不持久化；每次启动从存储重建（见 6.5）。这意味着"调度计划"不是事实，"角色配置 + last_vault_rotation"才是事实。

### 6.2 单实例化：只在 active 节点建队列

```go
// OpenBao rotation.go: initQueue
if (conf.System.LocalMount() || !replicationState.HasState(consts.ReplicationPerformanceSecondary)) &&
    !replicationState.HasState(consts.ReplicationDRSecondary) &&
    !replicationState.HasState(consts.ReplicationPerformanceStandby) {
    // 建 WAL 恢复 → populateQueue → go runTicker
}
```

- standby / DR / performance secondary 上**根本不创建队列和 ticker**，不存在"多个调度器抢任务"的问题，因此也不需要分布式锁。
- Enterprise 自动轮转框架文档明确同样原则："Automated rotations execute exclusively on the active node of a cluster"；领导切换后新 active "automatically restores the rotation state from storage and resumes processing rotations where the previous node left off"。
- 复制集群下：shared mount 由主集群 active 轮转；local mount 各集群独立轮转。

### 6.3 时间计算：周期与 cron 两种算法，错过即放弃

```go
// Vault path_roles.go: staticAccount
func (s *staticAccount) NextRotationTimeFromInput(input time.Time) time.Time {
    if s.UsesRotationPeriod()  { return input.Add(s.RotationPeriod) }
    return s.Schedule.Next(input)                     // robfig/cron 五字段
}
func (s *staticAccount) IsInsideRotationWindow(t time.Time) bool {
    if s.UsesRotationSchedule() && s.RotationWindow != 0 {
        return t.Before(s.NextVaultRotation.Add(s.RotationWindow))
    }
    return true
}
func (s *staticAccount) ShouldRotate(priority int64, t time.Time) bool {
    return priority <= t.Unix() && s.IsInsideRotationWindow(t)
}
```

- **period 模式**：下次时间 = `last_vault_rotation + rotation_period`，纯相对计算，改配置即重算，无漂移。
- **schedule 模式**：下次时间 = `cron.Next(基准时间)`；到点时若已出窗（`now > next + window`），Vault 版的处理是**不执行、把 priority 更新为下一个计划点**（`NextRotationTimeFromInput(now)`），即错过即放弃、永不追跑。
- 无 window 时窗口默认为两个相邻计划点之间的全部时间（IsInsideRotationWindow 恒 true）。
- cron 解析用 `robfig/cron/v3`，`minRotationWindowSeconds = 3600`（窗口最小 1 小时，防止把窗口配得比 tick 粒度还小导致不可达）。

### 6.4 执行流程与 WAL：两步操作的最小恢复日志

一次轮转（`setStaticAccount`）的精确顺序：

1. 校验角色/连接/凭据类型支持；拿该角色的互斥锁（`locksutil.LockForKey(b.roleLocks, roleName)`，分片锁降低竞争）。
2. 生成新凭据（按 `password_policy` 或 RSA 密钥对生成器）。
3. **写 WAL**（`framework.PutWAL(ctx, s, "staticRotationKey", walEntry)`），内容含新密码、角色名、用户名、`LastVaultRotation`。
4. 调插件 `UpdateUser` 改远端系统密码（超时 `staticUpdateUserTimeout = 10s`）。
5. 更新角色的 `LastVaultRotation = now` 并写回存储。
6. **删 WAL**。任何一步失败，WAL 留在原地。

启动恢复（`populateQueue` + `loadStaticWALs`）时逐条对账：

- WAL 的 `LastVaultRotation` **晚于**角色存储里的值 → 说明上次轮转停在中间（密码可能已改但未记账），把该角色以 `Priority = now` 立即入队，重放时**复用 WAL 里的旧新密码**继续（`credentialIsSet()` 校验 WAL 中凭据非空，否则丢弃重建），成功后统一记账删 WAL。
- WAL 的 `LastVaultRotation` **早于**角色存储值 → 陈旧残留，直接删。
- 角色已不存在 / WAL 时间戳为零 → 删。
- 同一角色出现多条 WAL → 保留 `walCreatedAt` 最新一条，删旧的。

这是一个教科书式的"外部副作用 + 本地记账"两阶段方案：**先记日志、再动外部、最后记账**，重启后用时间戳比较判定向前重放还是向后回滚，而不需要分布式事务。

### 6.5 失败重试：队内退避，而非死信

- OSS 版（OpenBao/Vault 相同）：`setStaticAccount` 出错后 `item.Priority = time.Now().Add(10 * time.Second)` 再入队，保留 WAL ID——固定 10 秒退避的简单重试。
- Enterprise 自动轮转框架（1.19+）升级为**策略化重试**：初始延迟 + 指数翻倍 + 随机抖动（jitter）+ 上限封顶；封顶后按固定间隔重试，直到成功或超过重试上限。
- **孤儿（orphaned）**：超过策略上限的条目被标记 orphaned——停止自动轮转、需要人工修复后重新注册（日志："orphaning item, please re-register after resolving the error"）。即：轮转失败到极端情况宁可冻结，也不无限刷错误。

### 6.6 对外可见的状态

读 `static-creds` 返回：`last_vault_rotation`（上次成功轮转）、`next_vault_rotation`（Enterprise/计划轮转）、`ttl`（距下次轮转的近似秒数；实现注释明确"近似"——因为到期检查每 5 秒一轮、轮转本身耗时，TTL 可短暂为负、负值归零，提示调用者"密码正在被轮转"）。

## 7. 调度原则提炼

1. **单调度者由拓扑决定，不靠竞争产生**：active 节点独占调度，副本不建调度器；故障切换后由新 active 从持久层重建。调度无需分布式锁。
2. **权威状态最小化**：只持久化"配置 + last_vault_rotation + WAL"，下次执行时间一律现算。计划是派生视图，丢了就能重建。
3. **周期任务用优先级队列 + 心跳扫描近似触发**：5 秒 ticker + 最小堆，实现 30 行以内，秒级精度对密码轮转绰绰有余；不引入外部调度器依赖。
4. **错过窗口的语义是"跳过"而非"追跑"**：轮转是周期性意图，追跑会造成风暴且无收益；但必须有显式窗口参数把"允许的迟到"交给用户配置。
5. **跨系统写操作必须先写日志再动外部**：两步操作的最小 WAL + 时间戳对账，把"改了远端没记账"的危险态收敛为可重放。
6. **重试在队内退避，极端失败冻结为孤儿并显式告警**，避免重试风暴与静默失败两个极端。
7. **优先用生命周期消灭调度**：能短命轮换的凭据不做轮转调度；调度只留给必须长期存在的凭据。

## 8. 对 stateflux 的对照与启示

stateflux（PG 唯一权威 + Redis 队列 + 外部选举唯一调度节点）与 Vault 轮转器的架构同构度高，可直接借用其原则：

| Vault 轮转器 | stateflux 对应 | 可借鉴点 |
| --- | --- | --- |
| active 节点独占调度，副本不建队列 | 外部选举指定唯一 Scheduler | 一致；Vault 证明"副本完全不启动调度器"比"多调度器 + 分片锁"更简单可靠 |
| 角色配置 + last_vault_rotation 是唯一事实，next 现算 | `tasks` 表是唯一权威 | 周期轮转任务的"下次执行时间"建议由 `last_success + period` 现算，避免持久化绝对时间点的漂移与补偿逻辑 |
| 内存优先级队列 + 5s ticker | PG 认领扫描（自适应认领） | 轮转类任务量级小，可维持统一 PG 扫描；若需大量周期任务，可加内存堆做调度索引、PG 只做权威 |
| 错过窗口跳到下一计划点 | 轮转任务的错失语义 | `tasks` 表若支持 `cron`/`period` 类任务，需显式定义"错过即跳过"并提供可选窗口字段，而非默认补跑 |
| WAL 两阶段（写日志 → 改远端 → 记账） | 轮转任务的 payload 阶段标记 | 轮转执行器应把"新密码已生成/已写入目标系统/已持久化"作为可恢复的阶段状态存入任务记录，崩溃后按阶段重放或作废，而不是盲目重试整个任务 |
| 失败退避重试 → 孤儿 + 人工介入 | 重试策略 + 死信 | 对轮转类任务增加最大重试上限，超限冻结任务并暴露状态，而非无限重试 |
| `ttl` 读数是近似值、可短暂为负 | 结果查询语义 | 对外承诺轮转时间时预留 tick 粒度误差 |

需要 stateflux 自行补齐的：Vault 的静态轮转没有"多活密码/宽限期"，靠下游轮询新值；若 stateflux 的轮转任务需要平滑切换（新旧密码并行有效），这是轮转器之上的业务层设计，框架只需保证任务的可重放与幂等。

## 9. 参考资料

- OpenBao 源码（main）：
  - `internal/builtin/logical/database/rotation.go` — 轮转队列、ticker、WAL 全实现
  - `internal/builtin/logical/database/path_roles.go` — `RotationPeriod`/`NextRotationTime` 定义（仅 period，无 cron）
  - `internal/builtin/logical/database/path_config_connection.go` — 根凭据 `rotate-root`、`root_rotation_statements`
- Vault 文档：
  - Database secrets engine 概念与静态角色：https://developer.hashicorp.com/vault/docs/secrets/databases
  - Database API（字段与默认值）：https://developer.hashicorp.com/vault/docs/api-docs/secret/databases （现 developer.hashicorp.com/vault/api-docs/secret/databases）
  - Lease 概念：https://developer.hashicorp.com/vault/docs/concepts/lease
  - Enterprise 自动轮转框架（active-node 独占、指数退避、孤儿）：https://developer.hashicorp.com/vault/docs/enterprise/automated-credential-rotation/overview
- Vault 源码（main，BUSL-1.1）：
  - `builtin/logical/database/rotation.go` — schedule/window 判定、错过出窗跳过
  - `builtin/logical/database/schedule/schedule.go` — robfig/cron 解析、`minRotationWindowSeconds = 3600`
  - `builtin/logical/database/path_roles.go` — `staticAccount`：`NextRotationTimeFromInput`/`IsInsideRotationWindow`/`SetNextVaultRotation`
- 背景文章：HashiCorp 博客 "Vault Enterprise 1.19 … automated root rotation"（1.18 引入数据库根凭据自动轮转、1.19 扩展）。
