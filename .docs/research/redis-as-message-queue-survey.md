# Redis 作为消息队列的四种方案调研：List / Pub/Sub / ZSet / Stream

> 调研日期：2026-09-11。Redis 版本基线：8.x（8.0 GA 2025-05，8.2 GA 2025-08；对 5.0~6.2 引入的特性逐条标注版本）。
> 语境：为 stateflux「异步任务投递通道」（设计 G5：Redis LIST + BRPOP）与 v2 演进（tech-research §7 P2：Streams 备选）提供方案级依据。
> 方法：逐方案从原理、实施细则、注意事项、优点、缺陷五个维度展开，最后横向对比并给出选型建议。所有关键论断附来源。

---

## 1. 结论先行

1. **四种方案对应四种可靠性档位，没有一种能原生做到 exactly-once**。默认语义从弱到强：Pub/Sub（fire-and-forget）≤ List（弹出即失）≈ ZSet（取走即失）< Stream（组内 PEL，原生 at-least-once）。at-least-once 之上必须叠加业务幂等，这是所有方案共同的安全前提。
2. **想要"真正的消息队列"语义，Stream 是唯一原生答案**：ack（XACK）、未确认账本（PEL）、消费者组、死信接管（XCLAIM/XAUTOCLAIM）、消费滞后可观测（XINFO lag）。代价是全生命周期管理复杂度和略高的每消息开销。
3. **List 仍是吞吐与内存效率最高的"简单任务队列"**，但其可靠性模式（BLMOVE 到 processing list + 超时回收扫描）本质上是手工实现 Stream PEL 的功能，且回收是 O(N) 全列表扫描，不规模化。
4. **ZSet 的不可替代性在"延迟/定时/优先级"**——这是 List 和 Stream 都没有的原生能力（score 排序）；它的短板是无阻塞条件弹出（必须 Lua 轮询）、无 ack。Asynq 用「List + 多个 ZSet」的组合验证了这条路的工程可行性。
5. **Pub/Sub 只适合"允许丢失的实时广播"**：不落盘、无 ack、慢消费者会被服务端主动断连（client-output-buffer-limit 默认 32MB/8MB/60s）。它适合缓存失效广播、锁唤醒信号、在线端推送，不适合承载任何业务投递语义。
6. **所有方案共享 Redis 层的先决风险**：异步复制 failover 丢最近写入、AOF everysec 崩溃丢 ~1s、`maxmemory-policy` 误配导致静默淘汰丢消息。这些是"Redis 不是持久性消息队列"论断的根源，与选哪种方案无关——**Redis 做队列时，队列上游必须可重放（如 stateflux 的 PG 权威 + 对账），或者接受丢失窗口**。
7. **对 stateflux 的直接结论**：v1 维持 LIST+BRPOP + PG 对账（tech-research §4 的论证成立）；v2 若要消除「BRPOP 后、注册 inprocess 前」的 90s grace 重复窗口，Streams 的 `XAUTOCLAIM` 是唯一能把该窗口缩到秒级且不引入新中间件的原生方案（见 §9）。

### 1.1 一页选型表

| 你的需求 | 选 | 理由 |
| --- | --- | --- |
| 简单任务队列，上游可重放/可对账 | **List** | 每消息 1~2 OPS，延迟最低，语义最少 |
| 任务必须不丢（进程崩溃也不能丢），有 ack/积压/回放需求 | **Stream** | 唯一原生 at-least-once + PEL + 消费组 |
| 延迟执行、定时任务、优先级、指数退避重试 | **ZSet**（或 ZSet+List 组合） | score 排序是唯一原生支持 |
| 实时广播、事件通知、允许丢失 | **Pub/Sub** | 零存储、零 ack、fan-out 原生 |
| 既要延迟又要不丢 | ZSet（延迟）+ Stream（可靠投递）或 ZSet+List 组合 | 两者的短板互补 |
| 任何场景下想要 exactly-once | 无（含专业 MQ） | 只能 at-least-once + 幂等去重 |

---

## 2. 背景与评价基准

### 2.1 为什么用 Redis 做消息队列

- **复用已有依赖**：系统已依赖 Redis（缓存/租约/协调），引入 Kafka/RabbitMQ 意味着新增一套运维、监控、备份体系；Redis 队列的边际运维成本接近零。
- **延迟与吞吐**：内存操作 + 单线程命令执行，简单命令 10 万+ OPS/实例（pipeline 数十万），端到端延迟亚毫秒级，远低于磁盘型 MQ 的典型投递延迟。
- **语义足够时是最短路径**：任务队列、事件通知这类场景，多数需求落在"投出去、有人处理、崩了能找回"三件事上，Redis 四种方案覆盖了这个谱系。

### 2.2 评价维度

本报告用以下维度横向比较，与 tech-research 的可靠性语义（G4）对齐：

1. **投递语义**：at-most-once / at-least-once / 是否可逼近 exactly-once；
2. **ack 与崩溃恢复**：消息交给消费者后崩溃，谁负责发现并重派；
3. **积压与回放**：消费者离线/落后时消息是否保留、能否重放历史；
4. **消费模型**：竞争消费（work queue）、广播消费（fan-out）、消费组横向扩展；
5. **调度能力**：优先级、延迟投递、重试退避、死信；
6. **成本**：每消息 Redis 开销、内存占用、实现复杂度、可观测性。

### 2.3 四种方案共享的 Redis 层约束（先读这个，再看方案）

这些风险与选型无关，是任何"Redis 队列"的地基缺陷：

| 约束 | 细节 | 对队列的影响 |
| --- | --- | --- |
| **异步复制** | 主从复制是异步的，failover 提升的副本可能缺少最近若干条写入 | 主节点收到消息、尚未复制到新主 → failover 后消息消失。WAIT/WAITAOF（7.2+）可同步等待但牺牲吞吐 |
| **持久化窗口** | AOF `everysec` 崩溃最多丢 ~1s 写入；`always` 拖慢吞吐且仍有磁盘缓存边界；RDB 快照间隔内的写入全丢 | 重启后队列尾部/PEL 可能缺失 |
| **maxmemory 淘汰** | `maxmemory-policy` 若为 `allkeys-lru` 等，内存满时**队列 key 本身会被淘汰** | 静默丢消息，且从日志几乎看不出来。队列实例必须 `noeviction`（写满报错优于静默丢） |
| **单线程命令执行** | 大集合的 O(N) 命令（LRANGE 全表、大 DEL、ZREM 海量成员）阻塞所有命令 | 回收扫描、大 key 清理要拆批 + `UNLINK` |
| **key TTL 是 key 级的** | Redis 没有"消息 TTL"概念，EXPIRE 作用于整个 key | 单条消息过期需自己编码进消息体并在消费时判断 |

---

## 3. 方案一：List（LPUSH + BRPOP / 可靠队列 BLMOVE）

### 3.1 原理

双向链表（quicklist，7.0 起由 listpack 节点组成）实现 FIFO/LIFO 队列：生产端 `LPUSH` 从头部进，消费端 `BRPOP` 从尾部出（尾出避免与生产端争抢同一端，利于 pipeline 合并）。`BRPOP`/`BLPOP` 是阻塞原语：队列为空时客户端挂起直到超时或有消息，**不轮询、零空转开销**。

### 3.2 实施细则

**基础模式（at-most-once）**：

```
-- 生产者
LPUSH task:queue <payload-json>

-- 消费者（独占连接）
BRPOP task:queue 0        -- timeout=0 表示永久阻塞；返回 [key, payload]
```

要点：

- **阻塞命令必须独占连接**：`BRPOP` 挂起期间该连接不能复用（go-redis 等客户端会为阻塞调用走独立连接池）。优雅停机用 `CLIENT UNPAUSE`/缩短 timeout（如 1s）+ 循环检查退出标志。
- **公平调度**：多个消费者 `BRPOP` 同一 key 时，Redis 按阻塞先后顺序交付（先阻塞先得），天然实现 work queue。
- **静态优先级**：`BRPOP key:high key:low 0` 按 key 给定顺序检查，可实现"高级队列优先"。
- **批量**：`LPOP/RPOP key 3`（6.2+ 支持-count 参数）一次弹多条，配合本地处理提升吞吐。
- **事务限制**：6.2 起 MULTI/EXEC 内不允许阻塞命令（此前按非阻塞语义执行），编排原子迁移要改用 Lua。

**可靠队列模式（at-least-once，社区标准做法）**：

```
-- 消费者：弹出并原子转入 processing 列表（BLMOVE，6.2+；前身 BRPOPLPUSH 已弃用）
BLMOVE task:queue task:processing RIGHT LEFT 0

-- 回收者（任意节点定期执行）：扫描 processing，超时的回滚主队列或进死信
```

```lua
-- 回收脚本（示意）：processing 列表全量扫描，按消息内自带 deadline 判定
local msgs = redis.call('LRANGE', KEYS[1], 0, -1)
local now, out = tonumber(ARGV[1]), {}
for _, raw in ipairs(msgs) do
  local deadline = extract_deadline(raw)   -- 消息体必须自带租约/入队时间字段
  if now > deadline then
    redis.call('LREM', KEYS[1], 1, raw)    -- O(N)：按值删除要遍历
    table.insert(out, raw)                 -- 调用方 LPUSH 回主队列或写死信
  end
end
return out
```

关键实现约束：

- **消息体必须自带元数据**（入队时间/租约 deadline/attempt 计数/唯一 ID），List 元素本身无任何属性；
- **processing 列表必须保持短小**：回收是 LRANGE 全扫 + 逐条 LREM（每条 O(N)），积压在 processing 里会让回收退化为 O(N²)；
- 消费成功后无需对 processing 做任何事之外的确认（消息已在消费前弹出），**ack 语义 = 从 processing 删除**，要么由回收者做，要么消费成功后主动 `LREM`。

### 3.3 注意事项与缺陷

| # | 问题 | 说明 |
| --- | --- | --- |
| 1 | **弹出即失** | BRPOP 之后消费者崩溃，消息已不在 Redis，无任何账本可查。可靠模式是必须项而非可选项（只要不能容忍丢） |
| 2 | **无 ack、无 PEL、无消费位点** | "谁取走了哪条"在 Redis 里不可见，监控与审计只能靠 LLEN 推断 |
| 3 | **无回放** | 弹出即离开，历史不可重查；上游重放是唯一补救 |
| 4 | **回收不规模化** | 见 3.2；processing 稍大即成瓶颈，这是可靠 List 队列最常见的生产事故点 |
| 5 | **无原生延迟/定时** | 需要组合 ZSet（§5） |
| 6 | **无广播** | 一条消息一个消费者；fan-out 要为每个订阅者维护独立 list，生产端逐个 LPUSH |
| 7 | **连接占用** | 每个阻塞消费者占一条连接；大规模消费者数受 maxclients 约束 |
| 8 | **重复投递** | BLMOVE 后崩溃 → 回收重派 → 至少执行两次；幂等仍不可省 |
| 9 | **大 key 风险** | 百万级积压时 DEL 阻塞，用 UNLINK；LLEN 监控要告警 |

### 3.4 优点

- **极致简单**：两条命令起步，语义 10 分钟能讲清楚；
- **吞吐/延迟最优**：每消息 1~2 OPS，无账本、无排序开销，quicklist 内存紧凑；
- **阻塞消费省 CPU**：BRPOP 挂起无空转，空队列时 Redis 负载为零；
- **监控直观**：LLEN 即积压量。

### 3.5 缺陷汇总

无 ack / 无回放 / 无消费组 / 无延迟 / 可靠性要手工构建且回收 O(N)。一句话：**它把消息队列最难的部分（投递确认与崩溃恢复）完全留给了应用层**——这正是 stateflux v1 选择它时必须配套 PG 对账体系（R1~R4）的原因。

---

## 4. 方案二：Pub/Sub（PUBLISH / SUBSCRIBE / Sharded Pub/Sub）

### 4.1 原理

主题广播模型：`SUBSCRIBE channel` 的客户端登记频道interest，`PUBLISH channel payload` 时 Redis 把消息**实时推送给所有在线的订阅者，且不做任何存储**。没人订阅就等于没发过（PUBLISH 返回收到的订阅者数，可为 0）。模式订阅 `PSUBSCRIBE news.*` 支持通配符。

### 4.2 实施细则

**基础流程**：

```
-- 订阅端（独占连接，循环处理推送；RESP2 下此连接只能执行 SUBSCRIBE 族命令）
SUBSCRIBE events:order.created

-- 发布端（任意连接）
PUBLISH events:order.created <payload>
```

要点：

- **RESP3（6.0+）解耦了订阅与命令**：`HELLO 3` 后 pub/sub 消息以 push 类型带外送达，同一连接可以同时执行普通命令；RESP2 下消息伪装成普通回复数组，订阅连接不能混用命令。生产环境建议显式协商 RESP3。
- **重连必补订阅**：任何断线后重连，订阅状态归零。客户端必须有"重连 → 重新 SUBSCRIBE"循环（主流客户端内置），并接受断线窗口内的消息丢失——要补齐丢失，得另配 Stream/List 通道。
- **慢消费者保护（重要配置）**：`client-output-buffer-limit pubsub 32mb 8mb 60`（默认值）——输出缓冲超过 32MB 或持续 60s 超 8MB，**服务端主动断开该订阅者**。这意味着：消费慢 → 被踢 → 重连期间消息全丢，且发布方完全无感知。这是 Pub/Sub 最大的生产暗坑。
- **Cluster 行为**：普通 `PUBLISH` 会把消息复制到集群所有节点再分发给各节点本地订阅者（一条消息 × N 节点的广播放大）。**Sharded Pub/Sub（7.0+）**：`SSUBSCRIBE`/`SPUBLISH` 把频道按 hash slot 归属到分片，消息只在 owning 节点内传播，广播成本消失；限制是不支持模式订阅，且 slot 迁移时频道跟随迁移。
- **Keyspace Notifications（特殊用法）**：`CONFIG SET notify-keyspace-events "Ex"` 可把 key 过期/删除等事件发布到 `__keyspace@*__` / `__keyevent@*__` 频道（8.2 又新增 `overwritten`/`type_changed` 事件类型）。**注意它底层就是 Pub/Sub，所有不可靠性原样继承**，且过期事件触发时机是"惰性删除 + 定期采样"的合成结果，**不是精确调度器**——用它做延迟队列是常见的错误用法。
- **监控**：`PUBSUB CHANNELS/NUMSUB/NUMPAT/SHARDCHANNELS`，INFO 中的 `pubsub_channels`、`pubsubshard_channels`；关注 `client_output_buffer_limit` 触发的断连计数。

### 4.3 注意事项与缺陷

| # | 问题 | 说明 |
| --- | --- | --- |
| 1 | **零持久化** | 消息不进 AOF/RDB、不参与主从复制回放；Redis 重启 = 频道清空 |
| 2 | **无 ack、无重试、无死信** | 发布即终结，投递结果对发布方不可见 |
| 3 | **离线即丢** | 订阅前、断线中、被 buffer-limit 踢出期间的消息全部丢失 |
| 4 | **慢消费者被断连** | 见 4.2；且被踢的一方若没有监控，表现为"偶发丢消息"最难排查 |
| 5 | **Cluster 广播放大** | 普通 PUBLISH O(节点数) 复制；大集群高频 PUBLISH 会打满总线，必须用 Sharded Pub/Sub |
| 6 | **可靠性无提升路径** | List 有可靠模式、Stream 有 PEL，Pub/Sub 没有任何机制可以"补"，要可靠就得换方案 |
| 7 | **模式订阅成本** | PSUBSCRIBE 每条 PUBLISH 都要做模式匹配，大量模式会拖慢发布路径 |

### 4.4 优点

- **延迟最低**：无存储路径，发布即推送，端到端微秒级；
- **原生 fan-out**：一个频道 N 个订阅者，广播语义天然正确；
- **零内存占用**：消息不留痕，无 trim/积压治理负担；
- **实现最简**：客户端一行订阅代码。

### 4.5 定位结论

**Pub/Sub 不是队列，是通知机制。** 它的正确用途：缓存失效广播、配置变更通知、分布式锁释放唤醒（替代轮询）、在线终端实时推送（丢失可容忍或应用层有序号补拉）。任何"消息必须到"的需求都不该落在 Pub/Sub 上。

---

## 5. 方案三：ZSet（延迟队列 / 定时任务 / 优先级队列）

### 5.1 原理

有序集合按 score 排序，`ZRANGE ... BYSCORE` 按 score 区间 O(logN+M) 取数。两种队列用法：

- **延迟/定时队列**：score = 执行到期时间戳（ms），消费端持续取 `score <= now` 的成员；
- **优先级队列**：score = 优先级数值，`ZPOPMIN` 弹最高优先级。

ZSet 是四种方案中**唯一原生具备"按未来时间/权重排序"能力**的——List 和 Stream 都是到达序，无法表达"这条消息 10 分钟后再处理"。

### 5.2 实施细则

**延迟队列标准实现**：

```
-- 生产：score = 到期时间（ms 时间戳）
ZADD task:delay 1726000000000 <payload-json>

-- 消费：轮询原子取出（ZRANGEBYSCORE 与 ZREM 两步之间有竞态，必须 Lua）
```

```lua
-- 取出到期任务（KEYS[1]=队列, ARGV[1]=now(ms), ARGV[2]=batch 上限）
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
if #items > 0 then
  redis.call('ZREM', KEYS[1], unpack(items))
end
return items
```

- **时间权威**：用哪台机器的时钟？建议 Lua 内 `redis.call('TIME')` 取 Redis 服务器时间（Redis 5 起非确定命令可在 Lua 中使用，复制按效果传播），消除多客户端时钟漂移；若用客户端时间，接受漂移并让"提前弹出"无害（幂等）。
- **轮询间隔**：空转成本与延迟精度的权衡。常用策略：有产出 → 立即再取；空 → 按下一个到期时间自适应 sleep（上限如 1s）。1s 轮询 = 平均延迟抖动 ≤1s + 每消费者 ~1 OPS/s 空载。
- **`BZPOPMIN` 为何不够用**：阻塞弹最小 score 成员，但**没有 score 条件**（不能表达"只弹 score ≤ now"）——到期时间未到的消息也会被弹出，"弹-还回"产生竞态与空转。因此条件延迟弹出只有 Lua 轮询一条路。
- **优先级 + 延迟复合**：score 单调编码（如 `priority*1e13 + (2^53 - dueMs)` 类技巧）或双队列分别排序；复合编码时注意双精度 score 的 53 位安全整数上限。

**可靠性扩展（at-least-once）**：取出的任务转入 processing ZSet，score = 租约到期时间：

```
ZADD task:processing <now+lease> <payload>     -- 认领
-- 处理成功 → ZREM task:processing
-- 回收者：ZRANGEBYSCORE task:processing -inf <now> → 逐条 ZREM + 回主队列/死信
```

**重试退避与死信**：失败后 `ZADD task:delay <now + backoff(attempt)> payload`（backoff 带 jitter）；attempt 超限 → `ZADD task:dead <ts> payload` 并对 dead ZSet 做 `ZREMRANGEBYSCORE` 保留窗口。**这套"delay/processing/dead 三 ZSet"正是 Asynq 的 scheduled/retry/archived 结构，是被数千生产用户验证过的形态。**

### 5.3 注意事项与缺陷

| # | 问题 | 说明 |
| --- | --- | --- |
| 1 | **无阻塞条件弹出** | 只能轮询（见 5.2）；延迟精度与 Redis 空载负载成反比 |
| 2 | **无 ack / 无消费组** | 取走即失；可靠性要自建 processing ZSet + 租约回收（等于手工实现 PEL） |
| 3 | **原子性靠 Lua** | 取出必须 Lua（或 WATCH 重试，代价更高）；多步状态迁移（delay→processing→dead）每步都要原子设计 |
| 4 | **时钟语义自定义** | score 是时间就要回答"谁的时钟"，见 5.2；跨机时钟漂移会让"未到期被弹出/到期未弹出" |
| 5 | **单 key 热 shard** | 大 ZSET 是单 key 单 slot：Cluster 下无法水平拆分，百万级成员时内存与轮询扫描（LIMIT 分页）都集中在一个分片；需按业务键分片成多 ZSET |
| 6 | **内存开销中等偏上** | skiplist + dict 双结构，单成员开销高于 List 元素（小集合用 listpack 编码缓解） |
| 7 | **score 精度** | 双精度浮点，安全整数上限 2^53；ms 时间戳可用但复合编码要小心 |
| 8 | **同 score 顺序** | 按 member 字典序，可利用它实现稳定排序（member 前缀放序号） |

### 5.4 优点

- **唯一原生延迟/定时/优先级**：score 语义直接表达，O(logN) 范围操作；
- **重试退避的天然载体**：next-retry 时间即 score，指数退避 + 死信收纳一气呵成；
- **按时间范围批量治理**：`ZREMRANGEBYSCORE` 清理过期死信、`ZCOUNT` 统计区间负载，运维顺手；
- **与 List/Stream 组合成熟**：ZSet 管"何时投递"，List/Stream 管"可靠投递"，职责清晰。

### 5.5 定位结论

ZSet 是**调度器**而不是队列：它解决"什么时候该处理"，不解决"处理丢没丢"。生产级形态几乎总是组合拳（ZSet 延迟 → 转入 List/Stream 可靠消费），Asynq 与 stateflux 的租约 ZSet 都是这个思路。

---

## 6. 方案四：Stream（XADD / XREADGROUP / XACK / XAUTOCLAIM）

### 6.1 原理

Redis 5.0 引入的追加式日志：基数树（radix tree）承载按 `<ms>-<seq>` 单调递增 ID 排列的 entry，天然支持按 ID 范围回放（XRANGE）与尾部阻塞跟随（XREAD BLOCK）。**消费者组（consumer group）**在其上实现队列语义：组内维护 `last-delivered-id`（组消费位点）和 **PEL（Pending Entries List，已投递未确认账本）**，组内消费者竞争消费、各自持有自己的 PEL，`XACK` 确认后从 PEL 移除（entry 本体保留至 trim）。

与 Kafka 的类比：stream ≈ partition，consumer group ≈ 消费组，last-delivered-id ≈ offset，PEL ≈ 未提交 offset 的消息集合；差别是没有再均衡协议（消费者分配完全靠"谁先 XREADGROUP 谁得"）。

### 6.2 实施细则（生产级 checklist）

**生产端**：

```
XADD tasks * job dispatch-job payload <json> MAXLEN ~ 100000
```

- ID 用 `*` 自动生成；**MAXLEN ~ 近似裁剪**（按基数树节点粒度删，O(1)~O(logN)）远优于精确 MAXLEN（O(N)）；有明确保留期用 `MINID ~ <ts>`；
- 大 payload 用 claim-check 模式（stream 里放引用，数据进对象存储），entry 过大直接拖垮整树的紧凑编码（`stream-node-max-bytes` 默认 4KB / `stream-node-max-entries` 默认 100 控制节点打包）。

**消费组与消费端**：

```
XGROUP CREATE tasks workers 0 MKSTREAM          -- 0=回放全部历史；$=只消费建组后新消息
XREADGROUP GROUP workers c-<node-id> COUNT 32 BLOCK 5000 STREAMS tasks >
XACK tasks workers <entry-id>
```

- **消费者命名**：`c-<host>-<pid>-<epoch>` 之类保证唯一——消费者只是个字符串名，两个进程用同名会互相交错 PEL 记账（常见事故）。7.0+ 有 `XGROUP CREATECONSUMER` 显式建消费者；
- `>` 取从未投递的新消息；`0`（或不带 >）重读本消费者 PEL 中的未确认消息——消费者重启后先补自己的 PEL，再做接管扫描；
- **NOACK**：XREADGROUP 带 NOACK 则不入 PEL，近似 at-most-once，仅适合可丢弃数据。

**崩溃恢复与死信接管（核心）**：

```
XPENDING tasks workers IDLE 60000 - + 64          -- 6.2+：列出挂起超 60s 的 entry
XAUTOCLAIM tasks workers c-<node-id> 60000 0-0 COUNT 64   -- 6.2+：原子地"查找+认领"超时挂起消息
XGROUP DELCONSUMER tasks workers c-dead-node       -- 慎用！见下
```

- **`min-idle-time` 是防止"偷走正在处理的消息"的关键参数**：只接管挂起超过该时长的消息，时长应 ≥ 正常处理时间的上界（租约思路）；
- XAUTOCLAIM（7.0 起）对已被 XDEL/trim 掉的 entry 返回 nil 并给出下一页游标，可顺带清理 PEL 幽灵条目；`JUSTID` 只认领不搬运数据；
- 反复被接管且 RETRYCOUNT 超限的 entry → 搬入死信（独立 stream 或 ZSet），这是**必须自建的**——Stream 有接管原语但没有自动死信；
- **`XGROUP DELCONSUMER` 会连消费者名下的 PEL 一起删掉**，这些消息从此不再被该组任何消费者重投（相当于从投递视角丢失）。删消费者前必须先确认其 PEL 已清空（XAUTOCLAIM 收尾）。

**保留与清理**：Stream 的 entry **不会自动消失**——不 trim 就无界增长吃光内存。三种策略：XADD 内联 `MAXLEN ~ n`（最常用）、后台定期 `XTRIM ~`/`MINID`、8.2 新增的引用计数删除（`XDELEX`/`XACKDEL` + XADD/XTRIM 的 ACKED/DELREF 策略，可精确清理"所有组都已确认"的 entry，解决 PEL 幽灵与保守 trim 的矛盾）。

**可观测性（四方案中最强）**：

```
XINFO GROUPS tasks      -- 每组：pending 数、consumers 数、lag（7.0+，消费滞后条数）
XINFO CONSUMERS tasks   -- 每消费者：idle 时长、pending 数 → 直接暴露死消费者
XPENDING tasks workers  -- PEL 摘要：最小/最大 ID、总数
```

`lag` 可直接作为告警指标（对比 Kafka 的 consumer lag）；`XINFO STREAM` 的 `entries-added`/`recorded-first-entry-id` 支撑吞吐与保留监控。

### 6.3 注意事项与缺陷

| # | 问题 | 说明 |
| --- | --- | --- |
| 1 | **生命周期管理复杂度全在应用侧** | 建组、消费者命名与清理、min-idle-time 取值、死信、trim 策略，五件事都要设计；写错任意一件表现为消息滞留 PEL 或内存无界 |
| 2 | **PEL 可能无界增长** | 消费者死掉不 ack → entry 永挂 PEL；需要定期 XAUTOCLAIM + 死信 + DELCONSUMER 流水线 |
| 3 | **单 key 单 slot** | 一个 stream 无法被 Cluster 水平拆分；超大吞吐要应用层按 key 分片（类似手工 partition），每组/分片独立维护 |
| 4 | **trim 必须显式** | 无自动过期；精确 MAXLEN O(N) 且阻塞，只能用 `~`；XDEL 只打洞不回收内存（直至 trim 到该位置） |
| 5 | **消息不可变** | 已写入的 entry 不能修改/延后（改期只能 XDEL+XADD 换 ID，顺序语义随之破坏）；延迟投递仍要 ZSet 配合 |
| 6 | **每消息开销高于 List** | ID 生成 + PEL 记账 + XACK，写读确认典型 3~4 OPS；百万级 QPS 场景成本可见 |
| 7 | **版本差异多** | 6.2 才有 XAUTOCLAIM/XPENDING IDLE；7.0 才有 lag/ENTRIESREAD；8.2 才有 XDELEX/XACKDEL。兼容老实例时要降级实现 |
| 8 | **无再均衡** | 消费者增减不触发重分配，靠 min-idle-time 接管兜底；消费速率倾斜要自己监控（XINFO CONSUMERS pending 分布） |

### 6.4 优点

- **唯一原生完整队列语义**：ack、PEL 崩溃恢复、消费组竞争、多组广播、历史回放、阻塞读，全部内建；
- **可观测性最好**：lag/pending/idle 三类指标直接覆盖"积压了多少、卡在谁手里"两大运维问题；
- **内存可控**：MAXLEN/MINID 保留策略 + 8.2 引用计数删除，保留期语义清晰；
- **官方推荐与生态位**：Redis 文档将 Streams 列为 job queue 的推荐形态；go-redis 的 XReadGroup/XAutoClaim 一等封装，落地成本低。

### 6.5 定位结论

Stream 是"在 Redis 里做一个正经消息队列"的标准答案。它的对手不是 List（定位不同），而是 Kafka/RabbitMQ——在这个量级上它的短板是单 stream 吞吐上限、无再均衡、无磁盘级积压成本优势（内存贵）。**中等规模（每秒万级消息、积压可控在内存预算内）它是最优性价比。**

---

## 7. 横向对比

### 7.1 能力矩阵

| 维度 | List | Pub/Sub | ZSet | Stream |
| --- | --- | --- | --- | --- |
| 消息存储 | 有（内存） | **无** | 有（内存） | 有（追加日志，需 trim） |
| 默认投递语义 | at-most-once | at-most-once | at-most-once | **at-least-once**（组 + PEL） |
| 可达最高语义 | at-least-once（手工 processing list） | at-most-once（无提升路径） | at-least-once（手工租约 ZSet） | at-least-once（原生；exactly-once 均不可能，需幂等） |
| ack | 无（手工删 processing） | 无 | 无（手工） | **原生 XACK** |
| 崩溃恢复 | 手工超时回收扫描 | 无 | 手工租约回收 | **原生 PEL + XAUTOCLAIM** |
| 积压容忍 | 好 | **差**（慢消费者被断连） | 好 | 好（受内存预算约束） |
| 历史回放 | 无 | 无 | 无（弹出即走） | **有（XRANGE 按 ID）** |
| 广播 fan-out | 无（多 list 模拟） | **原生** | 无（多 zset 模拟） | **多消费组** |
| 组内竞争消费 | 天然（弹出即得） | 不适用 | 原子取（Lua） | **原生消费组** |
| 优先级 | 多队列 + BRPOP 顺序 | 无 | **原生（score）** | 无（分 stream） |
| 延迟/定时投递 | 无（组合 ZSet） | 无 | **原生（score=时间）** | 无（组合 ZSet） |
| 阻塞消费 | **原生 BRPOP/BLMOVE** | 原生（订阅挂起） | 无（Lua 轮询） | **原生 XREAD BLOCK** |
| 有序性 | FIFO/LIFO | 发布序（对每个订阅者） | score 序 | 严格 ID 序（单 stream） |
| 每消息 Redis 开销 | 1~2 OPS | 1 OPS（无存储） | 2~3 OPS + 轮询空转 | 3~4 OPS（读+ack，+接管） |
| 内存效率 | **高** | 最低（零留存） | 中 | 中 |
| 实现复杂度 | 低（基础）/中（可靠模式） | 最低 | 中（Lua+租约+时钟） | 高（生命周期管理） |
| 可观测性 | LLEN | PUBSUB NUMSUB | ZCOUNT/ZCARD | **lag/pending/idle 全套** |
| 典型场景 | 简单任务队列 | 实时通知/广播 | 延迟/定时/优先级 | 生产级任务队列、事件流 |

### 7.2 投递语义达成路径对比

以"消费者处理中崩溃，消息必须最终被处理"为基准场景：

- **List**：BLMOVE → processing list → 回收者全扫 LRANGE 判定超时 → LREM + 回队。成本：全扫描 O(处理中数量)，处理中数量必须可控。
- **ZSet**：取出 → processing ZSet（score=租约到期）→ 回收者 ZRANGEBYSCORE 取超时 → 回队。成本：轮询 + 手工租约；比 List 的回收好一点（按 score 索引），但同样是自建。
- **Stream**：XREADGROUP 自动入 PEL → 崩溃后 XAUTOCLAIM(min-idle) 原子接管。成本：一条命令，Redis 服务端维护账本。**只有 Stream 的恢复路径是"查询"而不是"扫描"。**
- **Pub/Sub**：无路径，场景不成立。

### 7.3 故障场景逐项对照

| 故障场景 | List | Pub/Sub | ZSet | Stream |
| --- | --- | --- | --- | --- |
| 生产者发出前崩溃 | 消息未产生（上游重试解决） | 同左 | 同左 | 同左（XADD 原子） |
| **消费者拿到消息后崩溃** | 丢（无可靠模式）/ 回收重派（可靠模式） | 丢（本来就没账本） | 丢 / 租约重派（自建） | **PEL 保留，接管重投** |
| Redis failover（异步复制缺口） | 最近消息可能丢 | 最近通知丢 | 同 List | **最近 entry/ack 可能丢**，PEL 可能丢 ack 记录 → 重投（幂等兜底） |
| Redis 重启（AOF everysec） | 尾部 ~1s 丢 | 全丢 | 尾部 ~1s 丢 | 尾部 ~1s 丢，PEL 结构持久化 |
| 消费慢/积压 | 队列变长，正常 | **订阅者被断连，消息丢失** | 到期任务延迟处理 | 正常积压，lag 可监控 |
| 死信/毒消息 | 自建 | 无概念 | 自建（dead ZSet 成熟） | 有接管原语，死信策略自建 |

### 7.4 吞吐与延迟量级

均为内存操作，量级差距主要来自每消息 OPS 数与是否阻塞：Pub/Sub 与 List 单实例均可到 10 万+ 消息/s（pipeline 更高）；ZSet 受轮询与 Lua 影响略低但同量级；Stream 的 XADD+XREADGROUP+XACK 组合约打对折（仍可达数万~10 万/s），PEL 巨大时 XAUTOCLAIM/XPENDING 会变慢（PEL 也是内存结构）。**实际瓶颈通常先出现在消费者侧与网络 RTT，而非 Redis 命令吞吐**（与 tech-research §4.3 的 5~6 OPS/任务推演一致）。

---

## 8. 与专业消息中间件的定位对比（简）

| | Redis（Stream 为代表） | RabbitMQ | Kafka | RocketMQ |
| --- | --- | --- | --- | --- |
| 存储 | 内存（AOF 兜底） | 磁盘 + 内存 | 磁盘顺序日志 | 磁盘 |
| 副本与持久性 | 异步复制，failover 有缺口 | 镜像/quorum 队列 | 多副本 ISR | 多副本 |
| 积压成本 | 内存（贵，必须 trim） | 低 | **极低** | 低 |
| 再均衡/分区 | 无 / 手工分片 | 有（quorum） | 有（完整协议） | 有 |
| 事务/顺序 | Lua 原子；单 stream 有序 | publisher confirms | 事务 + 分区有序 | 事务消息 |
| 运维面 | **零新增**（已在用） | 中 | 高 | 高 |
| 适配规模 | 万级 msg/s、内存级积压 | 中 | 大数据量级 | 交易级 |

**不该用 Redis 当 MQ 的信号**：消息丢失等于资损且无上游重放（合规/交易流水）、积压常态达到磁盘量级（亿条）、需要复杂路由拓扑/死信自动化/消息轨迹审计、多团队共享的高吞吐总线。反之，任务队列、内部事件通知、进程间解耦这类场景，Redis 方案的运维成本优势是压倒性的。

---

## 9. 对 stateflux 的选型建议

结合设计目标（G2 PG 权威、G4 at-least-once + 幂等、G5 LIST 通道）与 tech-research §4/§7 的既有结论：

1. **v1 维持 LIST+BRPOP + PG 对账**（成立）。本报告进一步确认：stateflux 把「processing list」搬进 PG（inprocess 注册 + R1 对账重置），在语义上等价于 List 可靠模式，且用 PG 把回收从 O(N) 扫描换成了索引查询——**比原生 List 可靠模式更优**。已知的重复执行窗口（BRPOP 后、inprocess 注册前）= grace 周期（90s）是这一设计的准确边界。
2. **v2 优先评估 Stream 替换异步投递通道**（呼应 tech-research P2）。收益可量化：`XADD`（派发）+ `XREADGROUP`（认领）+ `XACK`（终态回写后确认）+ `XAUTOCLAIM`（接管），把 90s grace 窗口缩到 `min-idle-time`（如 30~60s，且秒级生效），并免费获得 lag 监控。成本：Executor 需管理消费者命名/接管/trim；Redis 侧需 ≥7.0（lag 指标）建议 ≥8.2（XDELEX/XACKDEL 清理已确认 entry）；每任务增加 ~2 OPS。保持「PG 权威 + Redis 可重建」分层不变，Stream 仅承载投递信号。
3. **Pub/Sub 明确划入控制面**：锁唤醒信号、配置/缓存失效广播——凡是"丢了无害或可对账"的通知都可用它，业务投递永不走 Pub/Sub。
4. **ZSet 的两个既定用法延续**：租约/超时索引（score=到期时间，与 §5.2 的 processing ZSet 同构，stateflux 已在用）与未来的延迟任务/重试退避（若引入，score=next-fire-time，Redis 服务器时间为权威）。
5. **生产配置红线**（任一队列方案共用）：队列实例 `maxmemory-policy noeviction`、AOF everysec、主从 + 哨兵、阻塞命令独立连接池、大 key 用 UNLINK、`R4 重建`演练常态化（Redis 全量丢失后队列信号可从 PG 重建是 stateflux 架构的安全底线）。

---

## 10. 参考来源

- Redis 官方文档：[Lists](https://redis.io/docs/latest/develop/data-types/lists/) · [Redis pub/sub](https://redis.io/docs/latest/develop/develop/pubsub/) · [Sharded Pub/Sub](https://redis.io/docs/latest/develop/develop/pubsub/#sharded-pubsub) · [Streams](https://redis.io/docs/latest/develop/data-types/streams/) · [Streams consumer groups 教程](https://redis.io/tutorials/redis-backed-job-queue-for-background-workers/) · [Keyspace notifications](https://redis.io/docs/latest/develop/use/keyspace-notifications/) · [Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) · [client-output-buffer-limit](https://redis.io/docs/latest/operate/oss_and_stack/management/config/#client-output-buffer-limit)
- antirez：[Streams consumer group patterns](https://redis.antirez.com/fundamental/streams-consumer-patterns.html)（XCLAIM/XAUTOCLAIM 接管与死消费者处理的原始论述）
- Svix：[Reliable Queues with Redis](https://www.svix.com/resources/redis/reliable-queue/)（BRPOPLPUSH/BLMOVE 可靠队列模式）
- Asynq（hibiken/asynq）：scheduled/retry/archived ZSet + List 的组合实现（§5 的工程先例）
- Redis 8.0 / 8.2 Release Notes：[8.0 GA（三许可）](https://redis.io/blog/redis-8-ga/) · [8.2（XDELEX/XACKDEL、XADD/XTRIM 扩展）](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/)
- 内部交叉引用：`.docs/research/stateflux-tech-research.md` §4（Redis 队列与可靠性）、§7 P2（Streams v2 建议）；`.docs/research/task-queue-framework-survey.md`（Asynq 状态存储同构性）

> 未确认项：Redis 8.2 的 XADD/XTRIM 新参数的完整签名未逐字核对（仅确认特性存在与 bugfix 轨迹），实施前以 redis.io 命令页为准。
