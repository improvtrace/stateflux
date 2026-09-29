#!/usr/bin/env bash
# 构建 stateflux 发布安装包（make install 的实现；Docker 构建走同一脚本）。
#
# 版本唯一来源是 build/version（末行，${alias}.${major}.${minor} 如 chronos.0.1），
# 经 go:embed 嵌入二进制——包名与二进制 --version 天然一致；commit / builddate 经
# 环境变量注入（Makefile export，直接调用时回退 git / date）。
#
# 产物：${OUT_DIR}/stateflux-${VERSION}-${os}-${arch}.tar.gz。
# 包内布局与安装后目录（/usr/local/stateflux）保持一致（bin/ + conf/ + VERSION），
# 仅额外携带 install.sh（安装）与 upgrade.sh（升级 / 降级）两个脚本：
#
#   install.sh          安装脚本（安装到 /usr/local/stateflux，bin/ + conf/ 分层）
#   upgrade.sh          升级脚本（dump → 替换 → migrate）
#   downgrade.sh        降级脚本（在旧版本包内执行：dump → 替换 → rebuild → 回载 dump）
#   bin/stateflux       静态二进制（CGO_ENABLED=0）
#   conf/config.yaml    全节点共用配置（internal/config/config.yaml）
#   VERSION             构建信息（version / commit / builddate）
#
# 用法：build/package.sh [os] [arch] [输出目录]（缺省为当前平台、dist/package）
# 交叉编译示例：build/package.sh linux arm64 dist/package
set -euo pipefail
cd "$(dirname "$0")/.."

GOOS_TARGET="${1:-$(go env GOOS)}"
GOARCH_TARGET="${2:-$(go env GOARCH)}"
OUT_DIR="${3:-dist/package}"

# 版本：build/version 末行为当前版本（与二进制嵌入内容同源，不提供环境变量覆盖，
# 避免产物名与二进制报告不一致）。
VERSION="$(tail -n 1 build/version | tr -d '[:space:]')"
[ -n "$VERSION" ] || { echo "build/version 缺少版本行" >&2; exit 1; }
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
BUILD_DATE="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
LDFLAGS="-s -w -X github.com/improvtrace/stateflux/pkg/buildinfo.Commit=${COMMIT} -X github.com/improvtrace/stateflux/pkg/buildinfo.Date=${BUILD_DATE}"
PKG_NAME="stateflux-${VERSION}-${GOOS_TARGET}-${GOARCH_TARGET}"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "$STAGE/bin" "$STAGE/conf"
CGO_ENABLED=0 GOOS="$GOOS_TARGET" GOARCH="$GOARCH_TARGET" \
	go build -trimpath -ldflags "$LDFLAGS" -o "$STAGE/bin/stateflux" ./cmd

cp internal/config/config.yaml "$STAGE/conf/config.yaml"
install -m 0755 build/install.sh "$STAGE/install.sh"
install -m 0755 build/upgrade.sh "$STAGE/upgrade.sh"
install -m 0755 build/downgrade.sh "$STAGE/downgrade.sh"
{
	printf 'version=%s\n' "$VERSION"
	printf 'commit=%s\n' "$COMMIT"
	printf 'builddate=%s\n' "$BUILD_DATE"
} >"$STAGE/VERSION"

mkdir -p "$OUT_DIR"
# 成员顺序 install.sh 在前（解压后第一步即运行它）；属主归零 + 按名排序，构建可复现。
tar -czf "${OUT_DIR:?}/${PKG_NAME}.tar.gz" \
	--owner=0 --group=0 --numeric-owner --sort=name \
	-C "$STAGE" install.sh upgrade.sh downgrade.sh bin conf VERSION

echo "package: ${OUT_DIR}/${PKG_NAME}.tar.gz (os=${GOOS_TARGET} arch=${GOARCH_TARGET} version=${VERSION} commit=${COMMIT} builddate=${BUILD_DATE})"
