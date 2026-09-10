.PHONY: build vet fmt proto generate clean

GO ?= go

build:
	$(GO) build -o stateflux ./cmd/stateflux

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

# 需要 protoc >= 25 及 protoc-gen-go / protoc-gen-go-grpc（PATH 内）
# 契约统一在 api/ 下，go_package 决定输出路径（module 模式，生成码与 proto 同目录）
proto:
	protoc --go_out=. --go_opt=module=github.com/improvtrace/stateflux \
		--go-grpc_out=. --go-grpc_opt=module=github.com/improvtrace/stateflux \
		api/stateflux/task/v1/task.proto
	protoc --go_out=. --go_opt=module=github.com/improvtrace/stateflux \
		--go-grpc_out=. --go-grpc_opt=module=github.com/improvtrace/stateflux \
		api/cluster/v1/cluster.proto

# ent 代码生成（schema 位于 internal/domain/schema，输出至 internal/domain/data/ent；
# 生成码不入库——.gitignore 已排除，克隆/拉取后需先执行本目标）
# 特性：sql/upsert（CreateBulk + OnConflict 幂等批量写）、sql/lock（FOR UPDATE fencing）、
# sql/execquery（认领挪行等原生 SQL 经 ent 连接执行）
generate:
	$(GO) tool entgo.io/ent/cmd/ent generate --feature sql/upsert,sql/lock,sql/execquery \
		./internal/domain/schema --target ./internal/domain/data/ent

clean:
	rm -rf bin/
