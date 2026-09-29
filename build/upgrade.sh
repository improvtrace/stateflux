#!/usr/bin/env bash
# stateflux 升级脚本（随安装包分发，与 install.sh 同级）：升级到**本包**版本。
# 降级不在此处——解压旧版本安装包执行其内的 ./downgrade.sh（用包内文件完整回退）。
#
# 流程：
#   1) pg_dump 备份当前库 → $PREFIX/dumps/${timestamp}_${当前版本}.dump
#   2) 用包内文件替换 bin/stateflux、conf/config.yaml（旧配置时间戳备份）、VERSION
#   3) 新二进制执行 stateflux migrate（version 表为每个应用版本插入记录）
#
# 可覆盖环境变量：PREFIX、PG_DSN（缺省从 conf/config.yaml 的 pg.dsn 解析）、
# PG_DUMP / PG_RESTORE（缺省 PATH 上的 pg_dump / pg_restore）、STATEFLUX_BIN。
set -euo pipefail

PREFIX="${PREFIX:-/usr/local/stateflux}"
PG_DUMP="${PG_DUMP:-pg_dump}"
STATEFLUX_BIN="${STATEFLUX_BIN:-$PREFIX/bin/stateflux}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMPS_DIR="$PREFIX/dumps"

log() { printf '[stateflux-upgrade] %s\n' "$*"; }
die() { printf '[stateflux-upgrade] 错误: %s\n' "$*" >&2; exit 1; }

# 读取 VERSION 文件里的 version= 行。
read_version() {
	sed -n 's/^version=//p' "$1" 2>/dev/null | head -1
}

# 从 conf/config.yaml 解析 pg.dsn（全文件唯一的 dsn: 键）。
pg_dsn() {
	if [ -n "${PG_DSN:-}" ]; then
		printf '%s' "$PG_DSN"
		return
	fi
	sed -n 's/^[[:space:]]*dsn:[[:space:]]*"\?\(postgres:\/\/[^"[:space:]]*\)"\?.*/\1/p' \
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

# dump_current 用当前安装版本号做一次 pg_dump（custom 格式）。
# 日志走 stderr：本函数多在命令替换中调用，stdout 用于回传文件路径。
dump_current() {
	local version="$1" ts out
	ts="$(date -u +%Y%m%dT%H%M%SZ)"
	out="$DUMPS_DIR/${ts}_${version}.dump"
	"$PG_DUMP" --format=custom --file="$out" "$DSN" || die "pg_dump 失败（检查 PG 可达性与 pg_dump 版本）"
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
	exec sudo env PREFIX="$PREFIX" PG_DSN="${PG_DSN:-}" STATEFLUX_BIN="$STATEFLUX_BIN" "$SCRIPT_DIR/upgrade.sh"
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

DUMPED="$(dump_current "$CUR_VERSION")"

# 替换二进制与配置（配置先做时间戳备份，便于人工比对）。
install -m 0755 "$SCRIPT_DIR/bin/stateflux" "$PREFIX/bin/stateflux"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
if [ -f "$PREFIX/conf/config.yaml" ] && ! cmp -s "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"; then
	install -m 0644 "$PREFIX/conf/config.yaml" "$PREFIX/conf/config.yaml.bak-$TS"
	log "旧配置备份为 conf/config.yaml.bak-$TS"
fi
install -m 0644 "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"
install -m 0644 "$SCRIPT_DIR/VERSION" "$PREFIX/VERSION"
STATEFLUX_BIN="$PREFIX/bin/stateflux"

log "执行迁移（version 表记录每个应用版本）"
"$STATEFLUX_BIN" migrate --config "$PREFIX/conf/config.yaml" || {
	log "迁移失败。数据库可用 $DUMPED 恢复：pg_restore --clean --if-exists --dbname=<dsn> $DUMPED"
	die "migrate 失败，升级中止"
}
log "升级完成: $PKG_VERSION（降级方案: 解压旧版本包执行 ./downgrade.sh）"
