# stateflux 服务镜像。与 `make install` 走同一条装配路径（build/package.sh 打包 →
# 解压 → install.sh 安装），镜像内即安装包部署后的目录布局（/usr/local/stateflux
# 的 bin/ + conf/），保证「tar 包安装」与「镜像运行」两份产物一致。
# 构建上下文为仓库根：docker build -f build/Dockerfile -t stateflux .（或 make docker）。

# syntax=docker/dockerfile:1
# 运行时基础镜像可覆盖（二进制为 CGO_ENABLED=0 纯静态，无 libc 依赖）：
# 默认 alpine（Docker Hub 可达，自带 ca-certificates-bundle）；网络可达 gcr.io 时可换
# distroless 进一步收敛攻击面：
#   make docker RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot
ARG RUNTIME_IMAGE=alpine:3.22
FROM golang:1.27 AS build
# 受限网络（proxy.golang.org 不可达）可覆盖：--build-arg GOPROXY=https://goproxy.cn,direct
ARG GOPROXY=""
ENV GOPROXY=${GOPROXY}
# 版本信息经 --build-arg 透传（容器内不继承宿主环境，也取不到被 .dockerignore 排除
# 的 .git）：package.sh 据此注入二进制构建信息并命名包内产物，与宿主导出的镜像
# tag / 文件名使用同一 version。
ARG VERSION=""
ARG COMMIT=""
ARG BUILD_DATE=""
ENV VERSION=${VERSION} COMMIT=${COMMIT} BUILD_DATE=${BUILD_DATE}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
# 打包（静态二进制 + install.sh + config.yaml）→ 解压 → 按安装包同款流程安装到
# /usr/local/stateflux（root 下执行，install.sh 的 sudo 分支不触发）。
# 另备 nonroot（uid 65532）可写的 WAL 工作目录：serve 默认在 WORKDIR 下建 .stateflux-wal。
RUN ./build/package.sh "${TARGETOS}" "${TARGETARCH}" /out \
	&& mkdir -p /tmp/pkg \
	&& tar -xzf /out/stateflux-*.tar.gz -C /tmp/pkg \
	&& PREFIX=/usr/local/stateflux BIN_LINK_DIR=/usr/local/bin /tmp/pkg/install.sh \
	&& install -d -m 0755 -o 65532 -g 65532 /var/lib/stateflux

FROM ${RUNTIME_IMAGE}
COPY --from=build /usr/local/stateflux /usr/local/stateflux
# --chown 显式指定：源目录为空时 COPY --from 不保留源属主，需强制归 nonroot（65532）
COPY --from=build --chown=65532:65532 /var/lib/stateflux /var/lib/stateflux
# 9090/9091 = serve gRPC/HTTP；9190/9191 = cluster 子命令（本机外置集群服务）。
# 配置文件监听 127.0.0.1（本机 cluster 服务拓扑），容器对外暴露经环境变量覆盖。
ENV STATEFLUX_SERVER_GRPC_ADDR=0.0.0.0:9090 \
	STATEFLUX_SERVER_HTTP_ADDR=0.0.0.0:9091
WORKDIR /var/lib/stateflux
EXPOSE 9090 9091 9190 9191
# 数值 uid：alpine 无 nonroot 用户名，distroless nonroot 即 65532，两者通用。
USER 65532
ENTRYPOINT ["/usr/local/stateflux/bin/stateflux"]
CMD ["serve", "--config", "/usr/local/stateflux/conf/config.yaml"]
