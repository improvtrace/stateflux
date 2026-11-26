-- Create "task_persistents" table
CREATE TABLE "task_persistents" (
    "id"              bigint NOT NULL,
    "resource"        character varying NOT NULL,
    "operator"        character varying NOT NULL DEFAULT '',
    "phase"           smallint NOT NULL DEFAULT 0,
    "outcome"         smallint NOT NULL DEFAULT 0,
    "error_summary"   character varying NOT NULL DEFAULT '',
    "timeout_ms"      bigint NOT NULL DEFAULT 0,
    "max_attempts"    integer NOT NULL DEFAULT 3,
    "claimed_node"    character varying NOT NULL DEFAULT '',
    "idempotency_key" character varying NOT NULL DEFAULT '',
    "payload"         jsonb,
    "vpc"             character varying NOT NULL DEFAULT '',
    "node"            character varying NOT NULL DEFAULT '',
    "label"           character varying NOT NULL DEFAULT '',
    "hash_bucket"     smallint NOT NULL DEFAULT 0,
    "finished_at"     timestamptz,
    "created_at"      timestamptz NOT NULL,
    "updated_at"      timestamptz NOT NULL,
    PRIMARY KEY ("id")
);

-- Create index "taskpersistent_idempotency_key" to table: "task_persistents"
CREATE UNIQUE INDEX "taskpersistent_idempotency_key"
    ON "task_persistents" ("idempotency_key")
    WHERE idempotency_key <> '';

-- Create index "taskpersistent_resource_phase" to table: "task_persistents"
CREATE INDEX "taskpersistent_resource_phase" ON "task_persistents" ("resource", "phase");

-- Create index "taskpersistent_phase_updated_at" to table: "task_persistents"
CREATE INDEX "taskpersistent_phase_updated_at" ON "task_persistents" ("phase", "updated_at");
