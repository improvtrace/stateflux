#!/usr/bin/env bash
# stateflux 降级脚本（随安装包分发）：用**本包**（旧版本安装包）内的文件完整降级——
# 相比 upgrade.sh 的 --downgrade 仅回退数据库，本脚本同时回退二进制与配置。
#
# 用法：解压目标（旧）版本的安装包，在其解压目录执行
#   sudo ./downgrade.sh
# 前提：本包版本必须旧于已安装版本；/usr/local/stateflux/dumps/ 下留存过本包版本的
# dump（每次升级 / 降级前都会自动备份，命名 ${timestamp}_${version}.dump）。
#
# 流程：
#   1) pg_dump 备份当前库 → $PREFIX/dumps/${timestamp}_${当前版本}.dump
#   2) 用包内文件替换 bin/stateflux、conf/config.yaml（旧配置时间戳备份）、VERSION
#   3) 替换后的（旧版本）二进制执行 stateflux migrate --target <包版本> --rebuild：
#      DROP/重建 schema 并重放该版本迁移（不写 version 记录，由下一步回填）
#   4) pg_restore --data-only 加载该版本最近一份历史 dump（业务数据与 version 记录）
#
# 可覆盖环境变量：PREFIX、PG_DSN（缺省从 conf/config.yaml 的 pg.dsn 解析）、
# PG_DUMP / PG_RESTORE（缺省 PATH 上的 pg_dump / pg_restore）。
set -euo pipefail

PREFIX="${PREFIX:-/usr/local/stateflux}"
PG_DUMP="${PG_DUMP:-pg_dump}"
PG_RESTORE="${PG_RESTORE:-pg_restore}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMPS_DIR="$PREFIX/dumps"

log() { printf '[stateflux-downgrade] %s\n' "$*"; }
die() { printf '[stateflux-downgrade] 错误: %s\n' "$*" >&2; exit 1; }

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

# dump_current 用指定版本号做一次 pg_dump（custom 格式）。
# 日志走 stderr：stdout 用于回传文件路径（供命令替换）。
dump_current() {
	local version="$1" ts out
	ts="$(date -u +%Y%m%dT%H%M%SZ)"
	out="$DUMPS_DIR/${ts}_${version}.dump"
	"$PG_DUMP" --format=custom --file="$out" "$DSN" || die "pg_dump 失败（检查 PG 可达性与 pg_dump 版本）"
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
	exec sudo env PREFIX="$PREFIX" PG_DSN="${PG_DSN:-}" "$SCRIPT_DIR/downgrade.sh"
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

dump_current "$CUR_VERSION" >/dev/null

# 用包内文件替换二进制 / 配置 / VERSION（旧配置时间戳备份，便于人工比对）。
install -m 0755 "$SCRIPT_DIR/bin/stateflux" "$PREFIX/bin/stateflux"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
if [ -f "$PREFIX/conf/config.yaml" ] && ! cmp -s "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"; then
	install -m 0644 "$PREFIX/conf/config.yaml" "$PREFIX/conf/config.yaml.bak-$TS"
	log "旧配置备份为 conf/config.yaml.bak-$TS"
fi
install -m 0644 "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"
install -m 0644 "$SCRIPT_DIR/VERSION" "$PREFIX/VERSION"

log "重建 schema 至 $PKG_VERSION（不写 version 记录，随后由 dump 回填）"
"$PREFIX/bin/stateflux" migrate --target "$PKG_VERSION" --rebuild --config "$PREFIX/conf/config.yaml" \
	|| die "migrate --rebuild 失败，降级中止（二进制已替换，可重试本脚本）"

log "加载数据: $RESTORE_DUMP"
"$PG_RESTORE" --data-only --disable-triggers --dbname="$DSN" "$RESTORE_DUMP" \
	|| die "pg_restore 失败；schema 已重建至 $PKG_VERSION，可人工重试: pg_restore --data-only --disable-triggers --dbname=<dsn> $RESTORE_DUMP"

log "降级完成: 二进制 / 配置 / 数据库均已回到 $PKG_VERSION"
"$PREFIX/bin/stateflux" --version | head -1
