#!/usr/bin/env bash
# 本地开发辅助：中间件启停 + demo 模式冒烟运行（§15：配置全部经 STATEFLUX_* 环境变量）。
#   hack/dev.sh up    拉起 PG + Redis（build/docker-compose.yml，仅绑定 127.0.0.1）
#   hack/dev.sh down  停止并清理中间件
#   hack/dev.sh migrate 应用 schema 迁移 SQL
#   hack/dev.sh run   构建并以 static 单机模式启动服务（gRPC :9090 / HTTP :9091）
set -euo pipefail
cd "$(dirname "$0")/.."

PG_DSN_DEFAULT="postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable"

case "${1:-}" in
  up)
    docker compose -f build/docker-compose.yml up -d --wait
    ;;
  down)
    docker compose -f build/docker-compose.yml down -v
    ;;
  migrate)
    docker exec -i build-postgres-1 psql -U stateflux -d stateflux \
      < internal/domain/migration/000001_20260914_create_task_tables.sql
    ;;
  run)
    go build -o stateflux ./cmd/stateflux
    export STATEFLUX_PG_DSN="${STATEFLUX_PG_DSN:-$PG_DSN_DEFAULT}"
    export STATEFLUX_CLUSTER_TRANSPORT="${STATEFLUX_CLUSTER_TRANSPORT:-static}"
    export STATEFLUX_CLUSTER_POLL_INTERVAL="${STATEFLUX_CLUSTER_POLL_INTERVAL:-2s}"
    export STATEFLUX_COHERENCE_SYNC_INTERVAL="${STATEFLUX_COHERENCE_SYNC_INTERVAL:-2s}"
    export STATEFLUX_RUNTIME_SCHEDULER_TRIGGERS="${STATEFLUX_RUNTIME_SCHEDULER_TRIGGERS:-tick,coherence}"
    export STATEFLUX_RUNTIME_FACTORY_INTERVAL="${STATEFLUX_RUNTIME_FACTORY_INTERVAL:-5s}"
    exec ./stateflux
    ;;
  *)
    echo "usage: $0 up|down|migrate|run" >&2
    exit 2
    ;;
esac
