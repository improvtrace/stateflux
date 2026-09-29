#!/usr/bin/env bash
# 构建 stateflux Docker 镜像并导出（make docker 的实现）：
#   1) docker build（build/Dockerfile）：与 make install 同一条装配路径
#      （build/package.sh 打包 → 解压 → install.sh 安装到 /usr/local/stateflux）；
#      VERSION / COMMIT / BUILD_DATE 经 --build-arg 透传（容器内不继承宿主环境），
#      镜像内二进制的构建信息与产物命名的 version 保持一致。
#   2) 镜像 tag：stateflux:<version>（附 stateflux:latest 便利标签）。
#   3) docker save 导出：dist/image/stateflux_<version>_linux_<arch>.tar.gz。
#
# 用法：build/docker.sh [arch] [输出目录]（缺省为当前架构、dist/image；
#       镜像平台固定 linux，交叉示例：build/docker.sh arm64 dist/image）
# 环境变量：DOCKER 覆盖 docker 命令；GOPROXY 透传给镜像内 go mod download /
#       go build（缺省取宿主 go env GOPROXY）；RUNTIME_IMAGE 覆盖运行时基础镜像
#       （缺省 alpine:3.22，网络可达 gcr.io 时可换
#        gcr.io/distroless/static-debian12:nonroot 收敛攻击面）。
set -euo pipefail
cd "$(dirname "$0")/.."

GOARCH_TARGET="${1:-$(go env GOARCH)}"
OUT_DIR="${2:-dist/image}"
DOCKER_CMD="${DOCKER:-docker}"

# 版本：build/version 末行（${alias}.${major}.${minor}），与二进制嵌入内容同源；
# 经 --build-arg 透传给镜像内的 package.sh，镜像内二进制版本与镜像 tag 一致。
VERSION="$(tail -n 1 build/version | tr -d '[:space:]')"
[ -n "$VERSION" ] || { echo "build/version 缺少版本行" >&2; exit 1; }
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
BUILD_DATE="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
GOPROXY_ARG="${GOPROXY:-$(go env GOPROXY)}"

"$DOCKER_CMD" build \
	--platform "linux/${GOARCH_TARGET}" \
	--build-arg VERSION="$VERSION" \
	--build-arg COMMIT="$COMMIT" \
	--build-arg BUILD_DATE="$BUILD_DATE" \
	--build-arg GOPROXY="$GOPROXY_ARG" \
	${RUNTIME_IMAGE:+--build-arg RUNTIME_IMAGE="$RUNTIME_IMAGE"} \
	-t "stateflux:${VERSION}" \
	-t stateflux:latest \
	-f build/Dockerfile .

mkdir -p "$OUT_DIR"
"$DOCKER_CMD" save "stateflux:${VERSION}" | gzip >"${OUT_DIR:?}/stateflux_${VERSION}_linux_${GOARCH_TARGET}.tar.gz"

echo "image:  stateflux:${VERSION} (linux/${GOARCH_TARGET}, also tagged stateflux:latest)"
echo "export: ${OUT_DIR}/stateflux_${VERSION}_linux_${GOARCH_TARGET}.tar.gz"
