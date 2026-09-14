-- 000002_20260914_enqueue_tasks_and_indexes.sql
-- 业务接入面（§5.1、§14.10）与次级索引（§5.6、§6.2）：
--   1) stateflux schema + next_task_id()（服务端 ID 生成，雪花式 ms<<12|seq）
--   2) stateflux.enqueue_tasks(...)：业务在自己事务内调用的唯一写入函数，SECURITY DEFINER，
--      业务 role 无需内部表 INSERT 权限；返回稳定 task_id（幂等键命中返回原 ID）
--   3) stateflux_biz 角色：仅 EXECUTE 函数 + SELECT task_results
--   4) 关键次级索引：晋升/认领排序、R1 扫描、工厂批次
-- 幂等：IF NOT EXISTS / CREATE OR REPLACE / 幂等 DO 块，可重复执行。
-- 说明：函数体直接写内部表；业务不得绕过函数写入（§1.2.8）。

CREATE SCHEMA IF NOT EXISTS stateflux;

-- 服务端任务 ID：毫秒时间戳 << 12 | 序列低 12 位，与 Go 雪花同量级（§3.1）。
CREATE SEQUENCE IF NOT EXISTS stateflux.task_id_seq;

CREATE OR REPLACE FUNCTION stateflux.next_task_id()
RETURNS BIGINT
LANGUAGE sql
VOLATILE
AS $$
  SELECT ((EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT << 12)
         | (nextval('stateflux.task_id_seq') & 4095)
$$;

-- 业务创建函数：单个函数内以身份账本裁决幂等，原子写 identity/payload/pending（§5.1）。
CREATE OR REPLACE FUNCTION stateflux.enqueue_tasks(
    p_task_id         BIGINT,
    p_type            TEXT,
    p_operator        TEXT        DEFAULT '',
    p_priority        INTEGER     DEFAULT 50,
    p_channel         TEXT        DEFAULT 'default',
    p_payload         JSONB       DEFAULT '{}'::jsonb,
    p_timeout_ms      BIGINT      DEFAULT 60000,
    p_max_attempts    INTEGER     DEFAULT 3,
    p_idempotency_key TEXT        DEFAULT '',
    p_callback        JSONB       DEFAULT NULL,
    p_parent_task_id  BIGINT      DEFAULT 0,
    p_vpc             TEXT        DEFAULT '',
    p_node            TEXT        DEFAULT '',
    p_label           TEXT        DEFAULT '',
    p_hash_bucket     INTEGER     DEFAULT 0,
    p_biz_race_labels TEXT[]      DEFAULT NULL,
    p_biz_race_entry  TEXT        DEFAULT '',
    p_biz_group       TEXT        DEFAULT '',
    p_biz_batch_id    TEXT        DEFAULT ''
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    v_task_id  BIGINT := p_task_id;
    v_resolved BIGINT;
BEGIN
    IF p_type IS NULL OR p_type = '' THEN
        RAISE EXCEPTION 'stateflux.enqueue_tasks: p_type is required';
    END IF;
    IF p_channel IS NULL OR p_channel = '' THEN
        RAISE EXCEPTION 'stateflux.enqueue_tasks: p_channel is required';
    END IF;
    IF p_priority IS NULL OR p_priority < 0 OR p_priority > 100 THEN
        RAISE EXCEPTION 'stateflux.enqueue_tasks: p_priority must be within [0,100]';
    END IF;

    IF v_task_id IS NULL OR v_task_id = 0 THEN
        v_task_id := stateflux.next_task_id();
    END IF;

    -- 幂等：账本裁决；命中返回原 task_id，不重复写 payload/pending（§5.1）。
    IF p_idempotency_key IS NOT NULL AND p_idempotency_key <> '' THEN
        INSERT INTO task_identities (task_id, idempotency_key, created_at)
        VALUES (v_task_id, p_idempotency_key, now())
        ON CONFLICT (idempotency_key) DO NOTHING;

        SELECT task_id INTO v_resolved
          FROM task_identities
         WHERE idempotency_key = p_idempotency_key;

        IF v_resolved IS NOT NULL AND v_resolved <> v_task_id THEN
            RETURN v_resolved;
        END IF;
        -- 同一 task_id 已入队（重复调用）：直接返回，避免主键冲突。
        IF EXISTS (SELECT 1 FROM task_pendings WHERE id = v_task_id) THEN
            RETURN v_task_id;
        END IF;
    END IF;

    INSERT INTO task_payloads (task_id, payload, created_at)
    VALUES (v_task_id, COALESCE(p_payload, '{}'::jsonb), now())
    ON CONFLICT (task_id) DO NOTHING;

    INSERT INTO task_pendings (
        id, type, operator, priority, channel, timeout_ms, max_attempts,
        idempotency_key, callback, parent_task_id, vpc, node, label, hash_bucket,
        biz_race_labels, biz_race_entry, biz_group, biz_batch_id, created_at, updated_at
    ) VALUES (
        v_task_id, p_type, COALESCE(p_operator, ''), COALESCE(p_priority, 50)::SMALLINT,
        p_channel, COALESCE(p_timeout_ms, 60000), COALESCE(p_max_attempts, 3),
        COALESCE(p_idempotency_key, ''), p_callback, COALESCE(p_parent_task_id, 0),
        COALESCE(p_vpc, ''), COALESCE(p_node, ''), COALESCE(p_label, ''),
        COALESCE(p_hash_bucket, 0)::SMALLINT, to_jsonb(p_biz_race_labels), COALESCE(p_biz_race_entry, ''),
        COALESCE(p_biz_group, ''), COALESCE(p_biz_batch_id, ''), now(), now()
    )
    ON CONFLICT (id) DO NOTHING;

    RETURN v_task_id;
END;
$$;

-- 业务角色：只拿到函数 EXECUTE 与结果查询权限，内部表不可直写（§5.1、§14.10）。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stateflux_biz') THEN
        CREATE ROLE stateflux_biz NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA stateflux TO stateflux_biz;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA stateflux TO stateflux_biz;
GRANT SELECT ON task_results TO stateflux_biz;
GRANT SELECT ON task_identities TO stateflux_biz;
-- 显式收回内部表写权限（PUBLIC 默认无权限，此处为防御性声明）。
REVOKE INSERT, UPDATE, DELETE ON task_pendings, task_schedulables, task_processings,
    task_completeds, task_payloads, task_results, task_identities FROM stateflux_biz;

-- 次级索引：晋升/认领排序、R1 扫描、工厂批次判定（§5.2、§6.2、§5.6）。
CREATE INDEX IF NOT EXISTS idx_task_pendings_priority
    ON task_pendings (priority DESC, id);
CREATE INDEX IF NOT EXISTS idx_task_schedulables_priority
    ON task_schedulables (priority DESC, id);
CREATE INDEX IF NOT EXISTS idx_task_processings_updated_at
    ON task_processings (updated_at) WHERE claimed_node <> '';
CREATE INDEX IF NOT EXISTS idx_task_completeds_biz_batch_id
    ON task_completeds (biz_batch_id) WHERE biz_batch_id <> '';
CREATE INDEX IF NOT EXISTS idx_task_processings_biz_batch_id
    ON task_processings (biz_batch_id) WHERE biz_batch_id <> '';
CREATE INDEX IF NOT EXISTS idx_task_pendings_biz_batch_id
    ON task_pendings (biz_batch_id) WHERE biz_batch_id <> '';
CREATE INDEX IF NOT EXISTS idx_task_schedulables_biz_batch_id
    ON task_schedulables (biz_batch_id) WHERE biz_batch_id <> '';
