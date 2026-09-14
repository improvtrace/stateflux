-- 000001_20260914_create_task_tables.sql
-- 创建 schema 中定义的全部 7 张表（§3.1）：四阶段账本 + payload / result / identity。
-- 与 internal/domain/schema/*.go（ent 表定义）一一对应；类型映射遵循 ent v0.14 Postgres 方言
-- （Int8/16→SMALLINT、Int32→INTEGER、Int64→BIGINT、String→character varying、JSON→jsonb、
-- Time→timestamptz）。列注释由 ent 生成落到列 DDL。
--
-- 次级索引本期不建（延后优化）；未来加索引/改结构一律新增版本文件，不改本文件。
-- 幂等：CREATE TABLE IF NOT EXISTS，可重复执行。

CREATE TABLE IF NOT EXISTS task_pendings (
  id              BIGINT NOT NULL,
  type            character varying NOT NULL,
  operator        character varying NOT NULL DEFAULT '',
  priority        SMALLINT NOT NULL DEFAULT 50,
  channel         character varying NOT NULL,
  timeout_ms      BIGINT NOT NULL DEFAULT 60000,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  idempotency_key character varying NOT NULL DEFAULT '',
  callback        jsonb NULL,
  parent_task_id  BIGINT NOT NULL DEFAULT 0,
  vpc             character varying NOT NULL DEFAULT '',
  node            character varying NOT NULL DEFAULT '',
  label           character varying NOT NULL DEFAULT '',
  hash_bucket     SMALLINT NOT NULL DEFAULT 0,
  biz_race_labels jsonb NULL,
  biz_race_entry  character varying NOT NULL DEFAULT '',
  biz_group       character varying NOT NULL DEFAULT '',
  biz_batch_id    character varying NOT NULL DEFAULT '',
  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,
  CONSTRAINT task_pendings_pkey PRIMARY KEY (id),
  CONSTRAINT priority_range CHECK (priority >= 0 AND priority <= 100),
  CONSTRAINT bucket_range CHECK (hash_bucket >= 0 AND hash_bucket <= 255)
);

CREATE TABLE IF NOT EXISTS task_schedulables (
  id              BIGINT NOT NULL,
  type            character varying NOT NULL,
  operator        character varying NOT NULL DEFAULT '',
  priority        SMALLINT NOT NULL DEFAULT 50,
  channel         character varying NOT NULL,
  timeout_ms      BIGINT NOT NULL DEFAULT 60000,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  attempts        BIGINT NOT NULL DEFAULT 0,
  claimed_node    character varying NOT NULL DEFAULT '',
  idempotency_key character varying NOT NULL DEFAULT '',
  callback        jsonb NULL,
  parent_task_id  BIGINT NOT NULL DEFAULT 0,
  vpc             character varying NOT NULL DEFAULT '',
  node            character varying NOT NULL DEFAULT '',
  label           character varying NOT NULL DEFAULT '',
  hash_bucket     SMALLINT NOT NULL DEFAULT 0,
  biz_race_labels jsonb NULL,
  biz_race_entry  character varying NOT NULL DEFAULT '',
  biz_group       character varying NOT NULL DEFAULT '',
  biz_batch_id    character varying NOT NULL DEFAULT '',
  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,
  CONSTRAINT task_schedulables_pkey PRIMARY KEY (id),
  CONSTRAINT priority_range CHECK (priority >= 0 AND priority <= 100),
  CONSTRAINT bucket_range CHECK (hash_bucket >= 0 AND hash_bucket <= 255)
);

CREATE TABLE IF NOT EXISTS task_processings (
  id              BIGINT NOT NULL,
  type            character varying NOT NULL,
  operator        character varying NOT NULL DEFAULT '',
  priority        SMALLINT NOT NULL DEFAULT 50,
  channel         character varying NOT NULL,
  timeout_ms      BIGINT NOT NULL DEFAULT 60000,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  attempts        BIGINT NOT NULL DEFAULT 0,
  claimed_node    character varying NOT NULL DEFAULT '',
  idempotency_key character varying NOT NULL DEFAULT '',
  callback        jsonb NULL,
  parent_task_id  BIGINT NOT NULL DEFAULT 0,
  vpc             character varying NOT NULL DEFAULT '',
  node            character varying NOT NULL DEFAULT '',
  label           character varying NOT NULL DEFAULT '',
  hash_bucket     SMALLINT NOT NULL DEFAULT 0,
  biz_race_labels jsonb NULL,
  biz_race_entry  character varying NOT NULL DEFAULT '',
  biz_group       character varying NOT NULL DEFAULT '',
  biz_batch_id    character varying NOT NULL DEFAULT '',
  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,
  CONSTRAINT task_processings_pkey PRIMARY KEY (id),
  CONSTRAINT priority_range CHECK (priority >= 0 AND priority <= 100),
  CONSTRAINT bucket_range CHECK (hash_bucket >= 0 AND hash_bucket <= 255)
);

CREATE TABLE IF NOT EXISTS task_completeds (
  id              BIGINT NOT NULL,
  type            character varying NOT NULL,
  operator        character varying NOT NULL DEFAULT '',
  priority        SMALLINT NOT NULL DEFAULT 50,
  channel         character varying NOT NULL,
  timeout_ms      BIGINT NOT NULL DEFAULT 60000,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  attempts        BIGINT NOT NULL DEFAULT 0,
  claimed_node    character varying NOT NULL DEFAULT '',
  error           character varying NOT NULL DEFAULT '',
  idempotency_key character varying NOT NULL DEFAULT '',
  callback        jsonb NULL,
  parent_task_id  BIGINT NOT NULL DEFAULT 0,
  vpc             character varying NOT NULL DEFAULT '',
  node            character varying NOT NULL DEFAULT '',
  label           character varying NOT NULL DEFAULT '',
  biz_race_labels jsonb NULL,
  biz_race_entry  character varying NOT NULL DEFAULT '',
  biz_group       character varying NOT NULL DEFAULT '',
  biz_batch_id    character varying NOT NULL DEFAULT '',
  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,
  outcome         SMALLINT NOT NULL,
  completed_at    timestamptz NOT NULL,
  CONSTRAINT task_completeds_pkey PRIMARY KEY (id),
  CONSTRAINT priority_range CHECK (priority >= 0 AND priority <= 100)
);

CREATE TABLE IF NOT EXISTS task_identities (
  task_id         BIGINT NOT NULL,
  idempotency_key character varying NOT NULL,
  created_at      timestamptz NOT NULL,
  CONSTRAINT task_identities_pkey PRIMARY KEY (task_id),
  CONSTRAINT task_identities_idempotency_key_key UNIQUE (idempotency_key)
);

CREATE TABLE IF NOT EXISTS task_payloads (
  task_id    BIGINT NOT NULL,
  payload    jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  CONSTRAINT task_payloads_pkey PRIMARY KEY (task_id)
);

CREATE TABLE IF NOT EXISTS task_results (
  task_id      BIGINT NOT NULL,
  outcome      SMALLINT NOT NULL,
  attempt      BIGINT NOT NULL,
  payload      jsonb NULL,
  result       jsonb NULL,
  error        character varying NOT NULL DEFAULT '',
  completed_at timestamptz NOT NULL,
  CONSTRAINT task_results_pkey PRIMARY KEY (task_id)
);
