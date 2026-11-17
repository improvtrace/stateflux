#!/usr/bin/env bash
# stateflux 升级脚本（随安装包分发，与 install.sh 同级）：升级到**本包**版本。
# 降级不在此处——解压旧版本安装包执行其内的 ./downgrade.sh（用包内文件完整回退）。
#
# 流程：
#   0) 若 serve/cluster 在运行：SIGTERM 优雅停止（替换运行中二进制会 Text file busy，
#      且半换状态不可恢复）；完成后不自动重启——由运维按原方式（systemd/脚本）拉起
#   1) pg_dump 探针校验连通性与客户端/服务端兼容性，随后 pg_dump 备份当前库
#      → $PREFIX/dumps/${timestamp}_${当前版本}.dump（临时文件 + 原子改名，失败不残留）
#   2) 用包内文件替换 bin/stateflux、VERSION；conf/config.yaml 遵循 install.sh 契约
#      **不覆盖**节点本地配置，包内新版差异旁路保存为 conf/config.yaml.new
#   3) 新二进制执行 stateflux migrate（stateflux_version 表为每个应用版本插入记录）
#
# 可覆盖环境变量：PREFIX、PG_DSN（缺省从 conf/config.yaml 的 pg.dsn 解析）、
# PG_DUMP / PG_RESTORE（缺省 PATH 上的 pg_dump / pg_restore）、STATEFLUX_BIN。
set -euo pipefail

PREFIX="${PREFIX:-/usr/local/stateflux}"
PG_DUMP="${PG_DUMP:-pg_dump}"
PG_RESTORE="${PG_RESTORE:-pg_restore}"
STATEFLUX_BIN="${STATEFLUX_BIN:-$PREFIX/bin/stateflux}"
STOP_TIMEOUT="${STOP_TIMEOUT:-15}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMPS_DIR="$PREFIX/dumps"

log() { printf '[stateflux-upgrade] %s\n' "$*"; }
die() { printf '[stateflux-upgrade] 错误: %s\n' "$*" >&2; exit 1; }

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

# stop_service 优雅停止运行中的 stateflux 进程（serve / cluster / migrate 之外的长驻进程）。
# 记录是否停过（STOPPED_SERVICES），预算 STOP_TIMEOUT 秒后 SIGKILL 兜底。
STOPPED_SERVICES=""
stop_service() {
	local pids p waited=0
	pids="$(pgrep -f 'stateflux (serve|cluster)' 2>/dev/null || true)"
	[ -n "$pids" ] || return 0
	log "检测到运行中的 stateflux 进程: $pids（先优雅停止，升级完成后需人工重启）"
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

# check_dump_compat 用一次 schema-only 探针 dump 校验连通性与 pg_dump 客户端/服务端
# 兼容性：客户端主版本低于服务端时 pg_dump 自身会拒绝；失败必须发生在替换任何文件
# 之前。pg_restore 探针仅校验存在性。
check_dump_compat() {
	if ! "$PG_DUMP" --schema-only --format=custom --file=/dev/null "$DSN" 2>/tmp/stateflux-upgrade-dumpcheck.$$; then
		local detail
		detail="$(tr '\n' ' ' </tmp/stateflux-upgrade-dumpcheck.$$ 2>/dev/null | head -c 300)"
		rm -f /tmp/stateflux-upgrade-dumpcheck.$$
		die "pg_dump 探针失败（PG 不可达或 pg_dump 客户端版本低于服务端）: ${detail:-无详情}。可经 PG_DUMP=/path/to/pg_dump 指定匹配版本的客户端"
	fi
	rm -f /tmp/stateflux-upgrade-dumpcheck.$$
	log "pg_dump 探针通过: $("$PG_DUMP" --version)"
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

# dump_current 用当前安装版本号做一次 pg_dump（custom 格式）。写临时文件成功后
# 原子改名：失败不残留半成品 dump（downgrade 按最新文件选取恢复源，残缺文件是隐患）。
# 日志走 stderr：本函数多在命令替换中调用，stdout 用于回传文件路径。
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

[ $# -eq 0 ] || die "本脚本不接收参数（用法: sudo ./upgrade.sh；降级请用旧版本包内的 ./downgrade.sh）"

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
	exec sudo env PREFIX="$PREFIX" PG_DSN="${PG_DSN:-}" PG_DUMP="$PG_DUMP" PG_RESTORE="$PG_RESTORE" \
		STATEFLUX_BIN="$STATEFLUX_BIN" "$SCRIPT_DIR/upgrade.sh"
fi

# 安装态校验。
CUR_VERSION="$(read_version "$PREFIX/VERSION" || true)"
[ -n "$CUR_VERSION" ] || die "未检测到已安装版本（$PREFIX/VERSION），首次安装请使用 ./install.sh"
[ -x "$STATEFLUX_BIN" ] || die "缺少已安装二进制 $STATEFLUX_BIN"
DSN="$(pg_dsn)"
[ -n "$DSN" ] || die "无法从 $PREFIX/conf/config.yaml 解析 pg.dsn（可用 PG_DSN 环境变量覆盖）"
command -v "$PG_DUMP" >/dev/null 2>&1 || die "缺少 $PG_DUMP（安装 postgresql-client）"
mkdir -p "$DUMPS_DIR"

CMP="$(ver_cmp "$PKG_VERSION" "$CUR_VERSION")"
if [ "$CMP" -lt 0 ]; then
	die "包版本 $PKG_VERSION 旧于当前 $CUR_VERSION；降级请解压旧版本安装包执行其中的 ./downgrade.sh"
fi
if [ "$CMP" -eq 0 ]; then
	log "当前已是 $CUR_VERSION，仅执行迁移检查"
else
	log "升级: $CUR_VERSION -> $PKG_VERSION"
fi

check_dump_compat
stop_service
DUMPED="$(dump_current "$CUR_VERSION")"

# 替换二进制与 VERSION。conf/config.yaml 遵循 install.sh 契约：节点本地状态不覆盖，
# 包内新版差异旁路保存为 conf/config.yaml.new 供人工确认合并（直接覆盖会把改好的
# pg/redis 地址重置回包内默认值，升级完成即失联）。
install -m 0755 "$SCRIPT_DIR/bin/stateflux" "$PREFIX/bin/stateflux"
if [ -f "$PREFIX/conf/config.yaml" ] && ! cmp -s "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"; then
	install -m 0644 "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml.new"
	log "包内配置与节点配置存在差异，已旁路保存为 conf/config.yaml.new（不覆盖现有配置）"
fi
install -m 0644 "$SCRIPT_DIR/VERSION" "$PREFIX/VERSION"
STATEFLUX_BIN="$PREFIX/bin/stateflux"

log "执行迁移（stateflux_version 表记录每个应用版本）"
"$STATEFLUX_BIN" migrate --config "$PREFIX/conf/config.yaml" || {
	log "迁移失败。数据库可用 $DUMPED 恢复：pg_restore --clean --if-exists --dbname=<dsn> $DUMPED"
	die "migrate 失败，升级中止"
}
if [ -n "$STOPPED_SERVICES" ]; then
	log "注意：升级前已停止 stateflux 进程，请按原部署方式重启（systemd / 启动脚本）"
fi
log "升级完成: $PKG_VERSION（降级方案: 解压旧版本包执行 ./downgrade.sh）"
