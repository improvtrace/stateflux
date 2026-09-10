#!/usr/bin/env bash
# 本地开发辅助：中间件启停 + demo 模式冒烟运行。
#   hack/dev.sh up    拉起 PG + Redis（build/docker-compose.yml，仅绑定 127.0.0.1）
#   hack/dev.sh down  停止并清理中间件
#   hack/dev.sh run   构建并以 demo Handler 启动服务（static 单机闭环，API :7000）
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  up)
    docker compose -f build/docker-compose.yml up -d --wait
    ;;
  down)
    docker compose -f build/docker-compose.yml down -v
    ;;
  run)
    go build -o stateflux ./cmd/stateflux
    exec ./stateflux -demo \
      -pg-dsn "postgres://stateflux:stateflux@127.0.0.1:5432/stateflux?sslmode=disable" \
      -redis-addr 127.0.0.1:6379
    ;;
  *)
    echo "usage: $0 up|down|run" >&2
    exit 2
    ;;
esac
