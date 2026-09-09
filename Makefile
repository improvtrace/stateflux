.PHONY: build test vet fmt proto generate clean

GO ?= go

build:
	$(GO) build ./cmd/...

test:
	$(GO) test ./... -count=1 -timeout 900s

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

# 需要 protoc >= 25 及 protoc-gen-go / protoc-gen-go-grpc（PATH 内）
proto:
	protoc --go_out=. --go_opt=module=github.com/improvtrace/stateflux \
		--go-grpc_out=. --go-grpc_opt=module=github.com/improvtrace/stateflux \
		proto/*.proto

# ent 代码生成（schema 位于 store/ent/schema）
generate:
	$(GO) run entgo.io/ent/cmd/ent generate ./store/ent/schema

clean:
	rm -rf bin/
