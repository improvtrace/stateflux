# stateflux 服务镜像：多阶段构建，产物为静态二进制（distroless 运行）。
# 构建上下文为仓库根：docker build -f build/Dockerfile -t stateflux .

# syntax=docker/dockerfile:1
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/stateflux ./cmd/stateflux

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/stateflux /stateflux
# 7000 = 业务接入 gRPC（api），7001 = 内部 gRPC（Execute/Collect，集群可见）
EXPOSE 7000 7001
USER nonroot
ENTRYPOINT ["/stateflux"]
