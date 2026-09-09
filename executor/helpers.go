package executor

import (
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"
)

// protoMarshal 包装 proto.Marshal。
func protoMarshal(msg *dispatchv1.TaskMessage) ([]byte, error) {
	return proto.Marshal(msg)
}

// protoUnmarshal 包装 proto.Unmarshal（统一错误语义）。
func protoUnmarshal(data []byte, msg *dispatchv1.TaskMessage) error {
	return proto.Unmarshal(data, msg)
}

// measureType handler 耗时直方图的 type 标签（低基数纪律：type 白名单由调用方保证）。
func measureType(taskType string) metric.RecordOption {
	return metric.WithAttributes(attribute.String("type", taskType))
}
