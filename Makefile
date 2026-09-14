.PHONY: build vet fmt api generate clean

GO ?= go

# api/ 下全部 proto（自动发现，含以后新增的文件）；$(sort) 保证稳定顺序
API_PROTO_FILES := $(shell find api -name '*.proto' 2>/dev/null | sort)
# protoc include 路径：仓库根（模块内相互 import）+ third_party/（第三方 proto 依赖，
# 如 google/protobuf 与网关注解等，目录存在则生效）
API_INCLUDES := -I. $(if $(wildcard third_party),-Ithird_party)

build:
	$(GO) build -o stateflux ./cmd/stateflux

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

# ent 代码生成（schema 位于 internal/domain/schema，输出至 internal/domain/data/ent；
# 生成码不入库——.gitignore 已排除，克隆/拉取后需先执行本目标）
# 特性：sql/upsert（CreateBulk + OnConflict 幂等批量写）、sql/lock（FOR UPDATE fencing）、
# sql/execquery（认领挪行等原生 SQL 经 ent 连接执行）
generate:
	$(GO) tool entgo.io/ent/cmd/ent generate --feature sql/upsert,sql/lock,sql/execquery \
		./internal/domain/schema --target ./internal/domain/data/ent

clean:
	rm -rf bin/
