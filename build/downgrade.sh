#!/usr/bin/env bash
# stateflux 降级脚本（随安装包分发）：用**本包**（旧版本安装包）内的文件完整降级——
# 相比 upgrade.sh 的 --downgrade 仅回退数据库，本脚本同时回退二进制与配置。
#
# 用法：解压目标（旧）版本的安装包，在其解压目录执行
#   sudo ./downgrade.sh
# 前提：本包版本必须旧于已安装版本；/usr/local/stateflux/dumps/ 下留存过本包版本的
# dump（每次升级 / 降级前都会自动备份，命名 ${timestamp}_${version}.dump）。
#
# 数据丢失警告：降级的数据来源是「本包版本最近一次**历史 dump**」——从那份 dump
# 到降级时刻的全部增量（新任务/终态）不会保留，仅存于降级前自动创建的当前版本
# dump 中，需要时须人工 pg_restore。
#
# 流程：
#   0) 若 serve/cluster 在运行：SIGTERM 优雅停止（完成后不自动重启，人工拉起）
#   1) pg_dump 探针校验连通性与版本兼容；pg_dump 备份当前库（临时文件 + 原子改名）
#   2) 用包内文件替换 bin/stateflux、VERSION；conf/config.yaml 遵循 install.sh 契约
#      **不覆盖**，包内差异旁路保存为 conf/config.yaml.new
#   3) 替换后的（旧版本）二进制执行 stateflux migrate --target <包版本> --rebuild：
#      仅清理本产品自有对象并重放该版本迁移（不写版本记录，由下一步回填）
#   4) pg_restore --data-only 加载该版本最近一份历史 dump（业务数据与版本记录）
#
# 可覆盖环境变量：PREFIX、PG_DSN（缺省从 conf/config.yaml 的 pg.dsn 解析）、
# PG_DUMP / PG_RESTORE（缺省 PATH 上的 pg_dump / pg_restore）。
set -euo pipefail

PREFIX="${PREFIX:-/usr/local/stateflux}"
PG_DUMP="${PG_DUMP:-pg_dump}"
PG_RESTORE="${PG_RESTORE:-pg_restore}"
STOP_TIMEOUT="${STOP_TIMEOUT:-15}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMPS_DIR="$PREFIX/dumps"

log() { printf '[stateflux-downgrade] %s\n' "$*"; }
die() { printf '[stateflux-downgrade] 错误: %s\n' "$*" >&2; exit 1; }

# 读取 VERSION 文件里的 version= 行。
read_version() {
	sed -n 's/^version=//p' "$1" 2>/dev/null | head -1
}

# 从 conf/config.yaml 解析 pg.dsn：pg 段的 dsn 是文件中首个 dsn: 键（cluster 段在其后），
# 兼容 postgres:// URL 与 host=... keyword=value 两种形态。
pg_dsn() {
	if [ -n "${PG_DSN:-}" ]; then
		printf '%s' "$PG_DSN"
		return
	fi
	sed -n 's/^[[:space:]]*dsn:[[:space:]]*"\?\(postgres:\/\/[^"[:space:]]*\|host=[^"]*\)"\?.*/\1/p' \
		"$PREFIX/conf/config.yaml" | head -1
}

# ver_cmp 比较两个 ${alias}.${major}.${minor} 版本：输出 -1/0/1。
ver_cmp() {
	awk -v A="$1" -v B="$2" 'BEGIN{
		if (A == B) { print 0; exit }
		split(A, ka, "."); split(B, kb, ".")
		if (ka[1] != kb[1]) { print (ka[1] < kb[1] ? -1 : 1); exit }
		if (ka[2] + 0 != kb[2] + 0) { print (ka[2] + 0 < kb[2] + 0 ? -1 : 1); exit }
		if (ka[3] + 0 != kb[3] + 0) { print (ka[3] + 0 < kb[3] + 0 ? -1 : 1); exit }
		print 0
	}'
}

# stop_service 优雅停止运行中的 stateflux 进程（serve / cluster）。
STOPPED_SERVICES=""
stop_service() {
	local pids p waited=0
	pids="$(pgrep -f 'stateflux (serve|cluster)' 2>/dev/null || true)"
	[ -n "$pids" ] || return 0
	log "检测到运行中的 stateflux 进程: $pids（先优雅停止，降级完成后需人工重启）"
	STOPPED_SERVICES="$pids"
	kill $pids 2>/dev/null || true
	while [ "$waited" -lt "$STOP_TIMEOUT" ]; do
		pids="$(pgrep -f 'stateflux (serve|cluster)' 2>/dev/null || true)"
		[ -z "$pids" ] && return 0
		sleep 1
		waited=$((waited + 1))
	done
	pids="$(pgrep -f 'stateflux (serve|cluster)' 2>/dev/null || true)"
	if [ -n "$pids" ]; then
		log "优雅退出超时，SIGKILL 兜底: $pids"
		kill -9 $pids 2>/dev/null || true
	fi
}

# check_dump_compat 用一次 schema-only 探针 dump 校验连通性与客户端/服务端兼容性，
# 必须发生在替换任何文件之前。
check_dump_compat() {
	if ! "$PG_DUMP" --schema-only --format=custom --file=/dev/null "$DSN" 2>/tmp/stateflux-downgrade-dumpcheck.$$; then
		local detail
		detail="$(tr '\n' ' ' </tmp/stateflux-downgrade-dumpcheck.$$ 2>/dev/null | head -c 300)"
		rm -f /tmp/stateflux-downgrade-dumpcheck.$$
		die "pg_dump 探针失败（PG 不可达或 pg_dump 客户端版本低于服务端）: ${detail:-无详情}。可经 PG_DUMP=/path/to/pg_dump 指定匹配版本的客户端"
	fi
	rm -f /tmp/stateflux-downgrade-dumpcheck.$$
	log "pg_dump 探针通过: $("$PG_DUMP" --version)"
}

# dump_current 用指定版本号做一次 pg_dump（custom 格式）。写临时文件成功后原子改名：
# 失败不残留半成品 dump（恢复源按最新文件选取，残缺文件是隐患）。
# 日志走 stderr：stdout 用于回传文件路径（供命令替换）。
dump_current() {
	local version="$1" ts out
	ts="$(date -u +%Y%m%dT%H%M%SZ)"
	out="$DUMPS_DIR/${ts}_${version}.dump"
	if ! "$PG_DUMP" --format=custom --file="$out.tmp" "$DSN"; then
		rm -f "$out.tmp"
		die "pg_dump 失败（检查 PG 可达性与 pg_dump 版本）"
	fi
	mv "$out.tmp" "$out"
	log "已备份当前库: $out" >&2
	printf '%s' "$out"
}

[ $# -eq 0 ] || die "本脚本不接收参数（用法: sudo ./downgrade.sh）"

# 校验包内容。
[ -x "$SCRIPT_DIR/bin/stateflux" ] || die "包内容不完整: 缺少 $SCRIPT_DIR/bin/stateflux"
[ -f "$SCRIPT_DIR/conf/config.yaml" ] || die "包内容不完整: 缺少 $SCRIPT_DIR/conf/config.yaml"
PKG_VERSION="$(read_version "$SCRIPT_DIR/VERSION" || true)"
[ -n "$PKG_VERSION" ] || die "包内 VERSION 缺少 version= 行"

# 目标目录不可写且非 root 时经 sudo 重入（同 install.sh）。
need_sudo=0
if [ "$(id -u)" -ne 0 ]; then
	mkdir -p "$PREFIX" 2>/dev/null || true
	[ -w "$PREFIX" ] || need_sudo=1
fi
if [ "$need_sudo" -eq 1 ]; then
	command -v sudo >/dev/null 2>&1 || die "操作 $PREFIX 需要 root 权限（当前非 root 且系统无 sudo）"
	exec sudo env PREFIX="$PREFIX" PG_DSN="${PG_DSN:-}" PG_DUMP="$PG_DUMP" PG_RESTORE="$PG_RESTORE" "$SCRIPT_DIR/downgrade.sh"
fi

# 安装态校验。
CUR_VERSION="$(read_version "$PREFIX/VERSION" || true)"
[ -n "$CUR_VERSION" ] || die "未检测到已安装版本（$PREFIX/VERSION），首次安装请使用 ./install.sh"
DSN="$(pg_dsn)"
[ -n "$DSN" ] || die "无法从 $PREFIX/conf/config.yaml 解析 pg.dsn（可用 PG_DSN 环境变量覆盖）"
command -v "$PG_DUMP" >/dev/null 2>&1 || die "缺少 $PG_DUMP（安装 postgresql-client）"
command -v "$PG_RESTORE" >/dev/null 2>&1 || die "缺少 $PG_RESTORE（安装 postgresql-client）"

CMP="$(ver_cmp "$PKG_VERSION" "$CUR_VERSION")"
if [ "$CMP" -ge 0 ]; then
	die "包版本 $PKG_VERSION 不旧于已安装的 $CUR_VERSION；如需升级执行本包 ./upgrade.sh"
fi

mkdir -p "$DUMPS_DIR"

# 该版本最近一份 dump：降级的数据来源。
RESTORE_DUMP="$(ls -1t "$DUMPS_DIR"/*_"$PKG_VERSION".dump 2>/dev/null | head -1 || true)"
[ -n "$RESTORE_DUMP" ] || die "未找到 $PKG_VERSION 的历史 dump（$DUMPS_DIR/*_$PKG_VERSION.dump）；须曾在该版本留存过备份"

log "降级: $CUR_VERSION -> $PKG_VERSION（用包内文件回退二进制/配置，并重建数据库）"
log "警告: $RESTORE_DUMP 之后产生的全部数据将被丢弃（仅保留在降级前的当前版本 dump 中）"

check_dump_compat
stop_service
CUR_DUMPED="$(dump_current "$CUR_VERSION")"
log "当前版本数据已另存: $CUR_DUMPED（需要时人工 pg_restore 恢复增量）"

# 用包内文件替换二进制 / VERSION；conf/config.yaml 遵循 install.sh 契约不覆盖，
# 包内差异旁路保存为 conf/config.yaml.new（直接覆盖会把节点改好的 pg/redis 地址
# 重置回包内默认值）。
install -m 0755 "$SCRIPT_DIR/bin/stateflux" "$PREFIX/bin/stateflux"
if [ -f "$PREFIX/conf/config.yaml" ] && ! cmp -s "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"; then
	install -m 0644 "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml.new"
	log "包内配置与节点配置存在差异，已旁路保存为 conf/config.yaml.new（不覆盖现有配置）"
fi
install -m 0644 "$SCRIPT_DIR/VERSION" "$PREFIX/VERSION"

log "重建 schema 至 $PKG_VERSION（不写版本记录，随后由 dump 回填）"
"$PREFIX/bin/stateflux" migrate --target "$PKG_VERSION" --rebuild --config "$PREFIX/conf/config.yaml" \
	|| die "migrate --rebuild 失败，降级中止（二进制已替换，可重试本脚本）"

log "加载数据: $RESTORE_DUMP"
"$PG_RESTORE" --data-only --disable-triggers --dbname="$DSN" "$RESTORE_DUMP" \
	|| die "pg_restore 失败；schema 已重建至 $PKG_VERSION，可人工重试: pg_restore --data-only --disable-triggers --dbname=<dsn> $RESTORE_DUMP"

if [ -n "$STOPPED_SERVICES" ]; then
	log "注意：降级前已停止 stateflux 进程，请按原部署方式重启（systemd / 启动脚本）"
fi
log "降级完成: 二进制 / 配置 / 数据库均已回到 $PKG_VERSION"
"$PREFIX/bin/stateflux" --version
