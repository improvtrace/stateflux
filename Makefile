.PHONY: help build test integration deps deps-clean vet fmt generate ent wire api migrate clean

GO ?= go

# 二进制输出目录（/bin/ 已被 .gitignore 排除）；交叉编译示例：make build GOOS=linux GOARCH=arm64
BIN_DIR ?= bin

# 追加给 go test 的参数，例如 make test TEST_FLAGS=-race（-race 需要 cgo）
TEST_FLAGS ?=

build: ## 构建 stateflux、datamigrate 与 mock-dm 到 bin/
	$(GO) build -o $(BIN_DIR)/ ./cmd/datamigrate ./cmd/mock-dm ./cmd/stateflux

help: ## 显示全部目标
	@awk 'BEGIN {FS = ":.*##"; printf "用法: make <目标>\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  make %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# -count=1 不走测试缓存，保证每次都真实执行。
test: ## 单元测试（不含 examples 集成场景，见 integration）
	$(GO) test -count=1 $(TEST_FLAGS) ./...

# 依赖不可达时用例整体 Skip 而不算失败：通过但全是 SKIP 时，先检查依赖。
integration: ## 集成测试：migrations 建库迁移 + examples 场景（需先 make deps）
	$(GO) test -tags=integration -count=1 -v $(TEST_FLAGS) ./migrations/ ./examples/

# 复用 examples/maintain.sh（幂等）：容器已在运行则不动；端口已被其他服务占用
# （如原生 redis-server / postgresql）时直接复用该服务，uninstall 也不会动它。
deps: ## 以 Docker 容器部署 examples 依赖（PostgreSQL + Redis）
	./examples/maintain.sh install

deps-clean: ## 停止并删除 examples 依赖容器（连同数据卷）
	./examples/maintain.sh uninstall

# examples 场景与 migrations 的集成测试带 integration 构建标签：不带标签时 go vet
# 与 go test 都不会编译它，因此这里带上标签，不跑用例也能静态检查到。
vet: ## go vet（带 integration 标签，覆盖 examples 场景与 migrations 集成测试代码）
	$(GO) vet -tags=integration ./...

fmt: ## gofmt 格式化全部 Go 源码
	gofmt -w .

# 生成码（ent / wire / protobuf）均随仓库提交，克隆后无需生成即可构建；修改来源后重新生成：
#   states/ent/schema  → make ent（参数以 states/ent/generate.go 的 go:generate 指令为准）
#   entry.go（ProviderSet）→ make wire
#   api/ 下的 proto    → make api
generate: ent wire ## 重新生成 ent 与 wire 代码（不含 protobuf，见 api）

ent: ## 由 states/ent/schema 重新生成 ent 代码
	$(GO) generate ./states/ent

# wire 已声明为 go.mod 的 tool 依赖：go tool 按模块版本解析，不依赖 PATH 上的安装。
wire: ## 重新生成 wire_gen.go（注入器在根包 wire.go）
	$(GO) tool github.com/google/wire/cmd/wire .

# api/ 下全部 proto（自动发现，含以后新增的文件），生成码与 proto 同目录入库。
# 需要 protoc 及 protoc-gen-go / protoc-gen-go-grpc 在 PATH 内。
API_PROTO_FILES := $(shell find api -name '*.proto' 2>/dev/null | sort)

api: ## 由 api/ 下的 proto 重新生成 Go 代码
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(API_PROTO_FILES)

# 修改 states/ent/schema 后生成增量迁移（先 make ent）：写入 migrations/<时间戳>_<名称>.sql
# 并更新 atlas.sum，schema 无变更时不产文件。开发库必须是空库（仅用于计算 diff，用完即清），
# DSN 经环境变量 STATEFLUX_DEV_URL 给出：
#   STATEFLUX_DEV_URL='postgres://postgres:postgres@127.0.0.1:5432/stateflux_dev?sslmode=disable' \
#     make migrate MIGRATION_NAME=add_xxx
MIGRATION_NAME ?= changes

migrate: ## 生成增量迁移：make migrate MIGRATION_NAME=<名称>（DSN 取环境变量 STATEFLUX_DEV_URL）
	$(GO) run migrations/gen/main.go -name $(MIGRATION_NAME)

clean: ## 删除构建产物（bin/）
	rm -rf $(BIN_DIR)
