-- 000003_20260914_notify_trigger.sql
-- 创建唤醒通道（§5.1）：task_pendings 插入后 pg_notify 唤醒调度器。
-- NOTIFY 只是唤醒优化，tick 不可关闭；通知丢失不影响正确性（§5.1、§6.2）。
-- 幂等：CREATE OR REPLACE + DROP TRIGGER IF EXISTS，可重复执行。

CREATE OR REPLACE FUNCTION stateflux.notify_task_pending()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- 只发 id 作为载荷，避免 8000 字节上限；通知在事务提交时投递。
    PERFORM pg_notify('stateflux_tasks', NEW.id::text);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_task_pendings_notify ON task_pendings;
CREATE TRIGGER trg_task_pendings_notify
AFTER INSERT ON task_pendings
FOR EACH ROW
EXECUTE FUNCTION stateflux.notify_task_pending();
