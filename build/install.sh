#!/usr/bin/env bash
# stateflux 离线安装脚本（随安装包分发，包内布局与安装后目录一致）。
#
# 用法：解开 stateflux-<version>-<os>-<arch>.tar.gz 后在解压目录执行
#   sudo ./install.sh
# 安装到 /usr/local/stateflux，目录布局：
#   /usr/local/stateflux/bin/stateflux    二进制（符号链接到 /usr/local/bin/stateflux）
#   /usr/local/stateflux/conf/config.yaml 配置文件
#   /usr/local/stateflux/VERSION          构建信息（version/commit/builddate）
#   /usr/local/stateflux/dumps/           升级/降级前的 pg_dump 备份目录
#
# 行为约定：
#   - PREFIX / BIN_LINK_DIR 可经环境变量覆盖（无需 root 的测试沙箱同样适用）：
#       PREFIX=/tmp/sandbox ./install.sh
#   - 幂等：重复执行视为升级，二进制始终覆盖；
#   - conf/config.yaml 视为节点本地状态：已存在则不覆盖（避免升级冲掉运维改动），
#     包内新版有差异时旁路保存为 conf/config.yaml.new，由人工确认合并；
#   - 旧版平铺布局自动迁移（$PREFIX/stateflux、$PREFIX/config.yaml → bin/、conf/）。
#   - 安装完成后需执行 stateflux migrate 初始化 PG（version 表记录首条版本）。
set -euo pipefail

PREFIX="${PREFIX:-/usr/local/stateflux}"
BIN_LINK_DIR="${BIN_LINK_DIR:-/usr/local/bin}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { printf '[stateflux-install] %s\n' "$*"; }
die() { printf '[stateflux-install] 错误: %s\n' "$*" >&2; exit 1; }

[ -x "$SCRIPT_DIR/bin/stateflux" ] || die "包内容不完整: 缺少可执行二进制 $SCRIPT_DIR/bin/stateflux"
[ -f "$SCRIPT_DIR/conf/config.yaml" ] || die "包内容不完整: 缺少配置文件 $SCRIPT_DIR/conf/config.yaml"

# 目标目录不可写且非 root 时经 sudo 重入（保留 PREFIX/BIN_LINK_DIR 覆盖，
# 指向可写目录的沙箱安装因此无需 root）。
need_sudo=0
if [ "$(id -u)" -ne 0 ]; then
	for d in "$PREFIX" "$BIN_LINK_DIR"; do
		mkdir -p "$d" 2>/dev/null || true
		[ -w "$d" ] || need_sudo=1
	done
fi
if [ "$need_sudo" -eq 1 ]; then
	command -v sudo >/dev/null 2>&1 || die "安装到 $PREFIX / $BIN_LINK_DIR 需要 root 权限（当前非 root 且系统无 sudo）"
	exec sudo env PREFIX="$PREFIX" BIN_LINK_DIR="$BIN_LINK_DIR" "$SCRIPT_DIR/install.sh"
fi

mkdir -p "$PREFIX/bin" "$PREFIX/conf" "$PREFIX/dumps" "$BIN_LINK_DIR"

# 旧版平铺布局迁移：老配置优先挪进 conf/（保留运维改动），老二进制直接淘汰。
if [ -f "$PREFIX/config.yaml" ] && [ ! -e "$PREFIX/conf/config.yaml" ]; then
	mv "$PREFIX/config.yaml" "$PREFIX/conf/config.yaml"
	log "旧布局迁移: $PREFIX/config.yaml -> $PREFIX/conf/config.yaml"
fi
if [ -f "$PREFIX/stateflux" ]; then
	rm -f "$PREFIX/stateflux"
	log "旧布局迁移: 移除平铺二进制 $PREFIX/stateflux（新版位于 bin/）"
fi

install -m 0755 "$SCRIPT_DIR/bin/stateflux" "$PREFIX/bin/stateflux"

if [ -f "$PREFIX/conf/config.yaml" ]; then
	if ! cmp -s "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"; then
		install -m 0644 "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml.new"
		log "已有配置与包内不同: 新版本保存为 $PREFIX/conf/config.yaml.new（未覆盖现有配置）"
	fi
else
	install -m 0644 "$SCRIPT_DIR/conf/config.yaml" "$PREFIX/conf/config.yaml"
fi

if [ -f "$SCRIPT_DIR/VERSION" ]; then
	install -m 0644 "$SCRIPT_DIR/VERSION" "$PREFIX/VERSION"
fi

ln -sfn "$PREFIX/bin/stateflux" "$BIN_LINK_DIR/stateflux"

log "安装完成: $PREFIX（命令: $BIN_LINK_DIR/stateflux）"
log "每个节点使用同一份配置，按以下顺序初始化与启动："
log "  0) 初始化 PG（建表 + version 表版本记录，幂等）: stateflux migrate --config $PREFIX/conf/config.yaml"
log "  1) 本机外置集群服务: stateflux cluster --config $PREFIX/conf/config.yaml"
log "  2) 节点服务:         stateflux serve  --config $PREFIX/conf/config.yaml"
log "注意: pg / redis 指向共享实例，部署前先在 $PREFIX/conf/config.yaml 中确认地址"
log "注意: 后续升级用同目录的 ./upgrade.sh；降级解压旧版本安装包执行其中的 ./downgrade.sh（均自动 dump 备份到 $PREFIX/dumps/）"
