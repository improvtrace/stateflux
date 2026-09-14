# third_party

第三方 `.proto` 依赖的放置目录（作为 `make api` 的 protoc include 路径自动生效），
例如 `google/protobuf/*.proto`（Well-Known Types）、grpc-gateway 的 `protoc-gen-openapiv2`
注解等。目录不存在或为空时 `make api` 自动跳过该 include 路径。
