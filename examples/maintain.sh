#!/bin/sh
# stateflux examples 依赖组件维护脚本：以 Docker 容器形式在本地安装
# 部署 / 卸载 Redis 与 PostgreSQL，供 examples 场景测试使用。
#
# 用法:
#   ./maintain.sh install     # 拉取镜像并启动容器（容器已存在则直接启动）
#   ./maintain.sh uninstall   # 停止并删除容器（连同数据卷）
#
# 端口绑定在 127.0.0.1，与场景默认环境变量一致：
#   Redis       127.0.0.1:6379
#   PostgreSQL  127.0.0.1:5432（用户/密码 postgres/postgres）
# 端口已被其他服务占用时（如 apt 安装的原生 redis-server/postgresql），
# 直接复用该服务、不重复起容器，uninstall 也不会动它。镜像与端口可用
# 环境变量覆盖，见 README.md。
set -e

REDIS_CONTAINER=schedule-examples-redis
PG_CONTAINER=schedule-examples-pg
REDIS_IMAGE=${SCHEDULE_STABLE_EXAMPLES_REDIS_IMAGE:-redis:7-alpine}
PG_IMAGE=${SCHEDULE_STABLE_EXAMPLES_PG_IMAGE:-postgres:14-alpine}
REDIS_PORT=${SCHEDULE_STABLE_EXAMPLES_REDIS_PORT:-6379}
PG_PORT=${SCHEDULE_STABLE_EXAMPLES_PG_PORT:-5432}

usage() {
	echo "用法: $0 install|uninstall" >&2
	exit 1
}

[ "$1" = "install" ] || [ "$1" = "uninstall" ] || usage

container_running() {
	[ "$(docker container inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

container_exists() {
	docker container inspect "$1" >/dev/null 2>&1
}

# port_in_use 判断本机是否已有服务监听该端口（ss 常见自带，退回 netstat；
# 两者皆无时返回未占用，由 docker run 自行报错）。正则要求端口后紧跟空白
# 或行尾，避免 6379 误匹配 16379 之类的端口。
port_in_use() {
	if command -v ss >/dev/null 2>&1; then
		ss -ltn 2>/dev/null | grep -qE "[:.]$1([[:space:]]|$)"
	elif command -v netstat >/dev/null 2>&1; then
		netstat -ltn 2>/dev/null | grep -qE "[:.]$1([[:space:]]|$)"
	else
		return 1
	fi
}

# ensure_dep 保证依赖在指定端口可用：容器在跑则不动；端口空闲则起容器
# （已存在则 start）；端口被其他服务占用则复用，并清掉起不来的残留容器。
ensure_dep() {
	name=$1
	image=$2
	port=$3
	shift 3
	if container_running "$name"; then
		echo "容器 $name 运行中"
	elif port_in_use "$port"; then
		if container_exists "$name"; then
			echo "端口 $port 已被其他服务占用，删除起不来的残留容器 $name"
			docker rm -f "$name" >/dev/null
		fi
		echo "端口 $port 已有服务监听，直接复用（uninstall 不会动它）"
	elif container_exists "$name"; then
		echo "容器 $name 已存在，直接启动"
		docker start "$name" >/dev/null
	else
		docker run -d --name "$name" --restart unless-stopped "$@" "$image" >/dev/null
	fi
}

# redis_ok/pg_ok 探活：容器模式用 docker exec；复用模式没有容器可进，
# 用一次性容器走 host 网络探测宿主端口。
redis_ok() {
	if container_running "$REDIS_CONTAINER"; then
		docker exec "$REDIS_CONTAINER" redis-cli ping 2>/dev/null | grep -q PONG
	else
		docker run --rm --network host "$REDIS_IMAGE" \
			redis-cli -h 127.0.0.1 -p "$REDIS_PORT" ping 2>/dev/null | grep -q PONG
	fi
}

pg_ok() {
	if container_running "$PG_CONTAINER"; then
		docker exec "$PG_CONTAINER" pg_isready -U postgres 2>/dev/null | grep -q "accepting connections"
	else
		docker run --rm --network host "$PG_IMAGE" \
			pg_isready -h 127.0.0.1 -p "$PG_PORT" -U postgres 2>/dev/null | grep -q "accepting connections"
	fi
}

# wait_until 轮询执行探活命令直到成功，超时报错退出（msg 为排查提示）。
wait_until() {
	times=$1
	msg=$2
	shift 2
	i=0
	until "$@"; do
		i=$((i + 1))
		[ "$i" -ge "$times" ] && { echo "$msg" >&2; exit 1; }
		sleep 1
	done
}

case "$1" in
install)
	ensure_dep "$REDIS_CONTAINER" "$REDIS_IMAGE" "$REDIS_PORT" \
		-p "127.0.0.1:$REDIS_PORT:6379"
	wait_until 30 "redis 未就绪，请检查 127.0.0.1:$REDIS_PORT 或 docker logs $REDIS_CONTAINER" redis_ok
	ensure_dep "$PG_CONTAINER" "$PG_IMAGE" "$PG_PORT" \
		-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
		-p "127.0.0.1:$PG_PORT:5432"
	wait_until 60 "postgresql 未就绪，请检查 127.0.0.1:$PG_PORT 或 docker logs $PG_CONTAINER" pg_ok
	echo "依赖就绪：redis 127.0.0.1:$REDIS_PORT，postgresql 127.0.0.1:$PG_PORT（postgres/postgres）"
	;;
uninstall)
	for name in "$REDIS_CONTAINER" "$PG_CONTAINER"; do
		if container_exists "$name"; then
			docker rm -f -v "$name" >/dev/null
			echo "已删除容器 $name"
		else
			echo "容器 $name 不存在，跳过"
		fi
	done
	echo "镜像保留，如需清理请手动 docker rmi $REDIS_IMAGE $PG_IMAGE"
	;;
esac
