.PHONY: build vet fmt api generate wire clean install docker

GO ?= go

# 目标平台与产物根目录（交叉编译示例：make install GOOS=linux GOARCH=arm64）
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)
DIST_DIR ?= dist

# 版本唯一来源：build/version 末行（${alias}.${major}.${minor}，如 chronos.0.1）。
# 该文件经 go:embed 嵌入二进制（根包 embed.go），产物命名与二进制 --version 自动一致，
# 因此不接受命令行覆盖（覆盖会破坏「产物名 = 二进制版本」的约定）。
VERSION := $(shell tail -n 1 build/version 2>/dev/null | tr -d '[:space:]')
ifeq ($(strip $(VERSION)),)
VERSION := unknown
endif
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
export COMMIT BUILD_DATE
# commit / builddate 编译注入（version 来自嵌入的 build/version，无需注入）。
LDFLAGS ?= -s -w -X github.com/improvtrace/stateflux/pkg/buildinfo.Commit=$(COMMIT) -X github.com/improvtrace/stateflux/pkg/buildinfo.Date=$(BUILD_DATE)

# api/ 下全部 proto（自动发现，含以后新增的文件）；$(sort) 保证稳定顺序
API_PROTO_FILES := $(shell find api -name '*.proto' 2>/dev/null | sort)
# protoc include 路径：仓库根（模块内相互 import）+ third_party/（第三方 proto 依赖，
# 如 google/protobuf 与网关注解等，目录存在则生效）
API_INCLUDES := -I. $(if $(wildcard third_party),-Ithird_party)

build:
	@mkdir -p $(DIST_DIR)/bin
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/bin/stateflux ./cmd

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

# 需要 protoc >= 25 及 protoc-gen-go / protoc-gen-go-grpc（PATH 内）。
# 契约统一在 api/ 下，go_package 决定输出路径（module 模式，生成码与 proto 同目录）；
# 单次调用传入全部 proto：跨文件 import 与公共消息只需声明一次。
api:
ifneq ($(strip $(API_PROTO_FILES)),)
	protoc $(API_INCLUDES) \
		--go_out=. --go_opt=module=github.com/improvtrace/stateflux \
		--go-grpc_out=. --go-grpc_opt=module=github.com/improvtrace/stateflux \
		$(API_PROTO_FILES)
else
	@echo "api/ 下没有 .proto 文件，跳过生成"
endif

# wire 依赖注入代码生成（§15.1#1）：需要 github.com/google/wire/cmd/wire 在 PATH 内。
wire:
	$(GO) tool github.com/google/wire/cmd/wire ./cmd/stateflux

# ent 代码生成（schema 位于 internal/domain/schema，输出至 internal/domain/data/ent；
# 生成码不入库——.gitignore 已排除，克隆/拉取后需先执行本目标）
# 特性：sql/upsert（CreateBulk + OnConflict 幂等批量写）、sql/lock（FOR UPDATE fencing）、
# sql/execquery（认领挪行等原生 SQL 经 ent 连接执行）
generate:
	$(GO) tool entgo.io/ent/cmd/ent generate --feature sql/upsert,sql/lock,sql/execquery \
		./internal/domain/schema --target ./internal/domain/data/ent

# 构建 .tar.gz 发布安装包（build/package.sh）：包内布局与安装后目录一致
# （bin/stateflux + conf/config.yaml + VERSION），另带 install.sh / upgrade.sh /
# downgrade.sh；解压后 sudo ./install.sh 安装到 /usr/local/stateflux。Docker 镜像构建
# （make docker）复用同一打包+安装路径。产物：
# $(DIST_DIR)/package/stateflux-<version>-<os>-<arch>.tar.gz（version 来自 build/version）。
install:
	./build/package.sh $(GOOS) $(GOARCH) $(DIST_DIR)/package

# 构建并导出 Docker 镜像（build/docker.sh）：tag stateflux:<version>（附 latest），
# 镜像内走与 install 相同的打包+安装路径；导出 $(DIST_DIR)/image/ 下含 version 的
# tar.gz（docker save）。镜像平台固定 linux，GOARCH 决定架构。
# 运行时基础镜像默认 alpine:3.22（Docker Hub 可达）；网络可达 gcr.io 时可换 distroless：
#   make docker RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot
docker:
	./build/docker.sh $(GOARCH) $(DIST_DIR)/image

clean:
	rm -rf $(DIST_DIR)/ bin/
