package collector

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

// decodeResult 把 result topic 上的载荷解码为 ResultEvent。
// 约定：result topic 的载荷是 ResultEvent 的 protobuf 二进制（§3.3）。
func decodeResult(payload []byte) (*taskv1.ResultEvent, error) {
	ev := &taskv1.ResultEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		return nil, fmt.Errorf("runtime/collector: decode result event: %w", err)
	}
	return ev, nil
}
