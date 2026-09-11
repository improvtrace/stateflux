# stateflux 设计 · 版本历史

> 当前版本见 [README](./README.md)；各主题文件头部标注所对应的版本。

## v3.17（2026-09-11）：控制面运行时归组目录更名

- `internal/controller/` 更名 `internal/runtime/`（目录名对齐「运行时」归组语义；
  scheduler/collector/reconcile 子包不变，纯目录更名、运行语义零变更）。

## v3.16（2026-09-10）：契约收编 api/ + 两运行时模型 + 契约面收敛

- 所有 proto 收拢至顶层 `api/`（`stateflux/task/v1` 调度↔执行契约、`cluster/v1` 外置集群契约，
  生成码与 proto 同目录）；
- 业务接入 RPC 契约不做（原模式 B `CreateTasks`、`GetResults` 长轮询、死信 RPC 移除），业务方按
  outbox 式直写任务表组接入；
- 运行时模型改为 **biz（task/v1 服务端业务）+ 控制面运行时（controller：scheduler/collector/
  reconcile）+ 执行侧运行时（worker，整体承接原 executor）**，`internal/server` 仅编排三者装配启停；
- Redis 队列视图下沉 `domain/cacheview`，迁移独立 `domain/migration`；`obs` 扩展为 metrics +
  tracing；ent 生成码不入库（构建时经 `make generate` 从 `domain/schema` 产出）。

## v3.15（2026-09-10）：内核并入 domain + 任务运行时归组 + 仓储 ent 化

- `sdk` 清空，共享内核（Task/Callback/Result 模型、Handler/Registry/Precondition 契约、退避、
  雪花 ID）按 model 并入 `internal/domain`（task.go/callback.go/result.go/ops.go/handler.go/
  retry.go/snowflake.go）；
- `factory`/`queue`/`dispatch` 收拢至 `internal/task/`（任务运行时归组）；
- 仓储读写全面改走 ent 类型安全 API（CreateBulk + OnConflict upsert、FOR UPDATE 行锁承载
  attempt fencing），仅认领挪行（§9.1 SKIP LOCKED 单条 SQL）与迁移引导 DDL 保留原生执行；
- 测试代码清空（后续按需重建，§8）。

## v3.14（2026-09-10）：domain 四层完整落地

领域层细分为 `domain`（聚合接口 + 组合根）、`domain/repository`（仓储实现，聚合语义）、
`domain/data`（数据源基建：连接池/ent client/迁移/SQL 套件 + ent 生成码）、`domain/schema`
（ent 表定义，代码生成输入），实现数据库操作的完整分层（§8）。

## v3.13（2026-09-10）：角色包归组

`collector`/`reconcile`/`executor` 收拢至 `internal/server/` 下（服务编排归组；executor 为常驻
角色（§2.1），仅物理位置变化，运行语义不变，§8）。

## v3.12（2026-09-10）：数据访问分层（domain/repository）

`internal/store`（纯接口）与 `internal/storepg`（ent 实现）重组为 `internal/domain`：聚合接口
（Task/Result/Ops）+ 组合根 Store + 语义错误在包根，实现层 `domain/repository/pg`（ent 生成码随包）；
消费侧按聚合依赖窄接口（§8）。

## v3.11（2026-09-10）：布局微调：sdk 提升为顶层公开包

Task/Handler 契约类型是执行接入（§7）的稳定依赖面，公开以支撑构建时注册与 v2 执行接入演进
（§14 遗留）；新增 `build/`（服务镜像 + 本地中间件编排）与 `hack/`（开发脚本）。
（sdk 公开状态后于 v3.15 收回。）

## v3.10（2026-09-10）：目录布局修订：Go 标准布局收编

引擎包全部移入 `internal/`（「不承诺三方库 API」由编译器强制，§1.2.8），顶层收敛为
`cmd/stateflux` + `internal/` + `proto/`（仅对外契约；内部契约 `dispatch.proto` 移入
`internal/proto/`），测试基座归并 `internal/test/`（§8，附 §8.1 机械迁移清单）。

## v3.9（2026-09-10）：定位修订：独立服务，而非三方框架库

§1.2.8 重写为「服务优先」，移除嵌入形态承诺；`sdk/` 从「业务方唯一依赖面」调整为引擎共享内核；
执行逻辑经 Handler 在服务构建时注册（§7/§8）。

## v3.8（2026-09-09）：技术栈约定

数据库操作统一走 ent（entgo.io/ent，Kratos 官方集成指南推荐的 ORM，§3.1）、proto 契约统一
proto3（§3.3）、观测采用 OpenTelemetry 指标采集（§6.5）。

## v3.7：结果集设计

`task_results` 独立结果表（终态事务一次性写入、不可变，保留期与历史归档解耦）+ 执行侧异步结果
WAL（append-only 磁盘日志、Ack 水位截断、重启重放、高水位反压，§3.1/§5.4）。

## 更早

- v3.6：阶段集合逻辑抽象、PG 分区表为默认实现（§3.1）；
- v3.5：调度侧集权/执行侧无状态（§1.2.7）；
- v3.4：调度预处理层（§5.2）；
- 更早：Machinery callback（§5.8）、TaskMessage（§3.3）、创建双模式（§5.1，v3.16 移除 RPC 模式）、
  TaskFactory（§5.7）、目录平铺 + sdk/（§8）。
