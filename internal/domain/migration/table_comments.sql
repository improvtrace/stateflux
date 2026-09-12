-- 表注释（COMMENT ON TABLE）——由迁移引导 DDL 执行（§8：migration 承载 bootstrap DDL）。
--
-- 为什么不用 ent：ent v0.14.6 的 entsql.Annotation 没有表级 comment 字段，只有
-- WithComments（把**列**注释写进 DDL）；列注释已由 schema 的 .Comment() 生成，
-- 表注释在首次迁移时用本文件补齐。
--
-- 表名、列名变更时本文件必须同步；幂等：可重复执行。

COMMENT ON TABLE pending_tasks IS
  '四阶段账本·待晋升：已创建未晋升的任务；约束晋升的扫描对象（设计 §5.2）';

COMMENT ON TABLE schedulable_tasks IS
  '四阶段账本·就绪：约束已通过、等待 fenced claim 的任务；认领扫描对象（设计 §5.2）';

COMMENT ON TABLE processing_tasks IS
  '四阶段账本·在途：已 claim 执行中；在途事实只由本表决定，通道投递状态不改写它（设计 §4）';

COMMENT ON TABLE completed_tasks IS
  '四阶段账本·终态历史：outcome ∈ {succeeded,failed,dead}，内联 payload、不内联 result（设计 §3.1）';

COMMENT ON TABLE task_payloads IS
  '任务载荷分离表：阶段表查询面不背 payload，终态时合并入 completed 并删除本行（设计 §5.5）';

COMMENT ON TABLE task_results IS
  '不可变终态结果集：终态事务一次性写入，task_id 主键幂等；保留期与 completed 归档解耦（设计 §3.1）';
