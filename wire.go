//go:build wireinject
// +build wireinject

package schedule

import (
	"github.com/google/wire"
)

// InitializeRuntime 装配调度器组件集，实现由 wire 生成于 wire_gen.go。
// nodeID 是启动时经 dm 查询到的本节点 ID（dispatch.LocalNodeID）。
func InitializeRuntime(cfg Config, nodeID string) (*Runtime, error) {
	wire.Build(ProviderSet, wire.Struct(new(Runtime), "*"))
	return nil, nil
}
