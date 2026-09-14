package domain

import (
	"encoding/json"
	"errors"
)

// MaxCallbackDepth 是回调派生链的深度上限（§5.6）：超过即不再派生，防止无限回调。
const MaxCallbackDepth = 8

// CallbackSpec 是终态回调规格（§5.6），存于任务行的 callback jsonb 列。JSON 字段为
// snake_case，与 proto 契约 api/stateflux/task/v1.CallbackSpec 的字段一一对应；
// 使用标准库编码以便仓储层（internal/domain/data）不依赖 protobuf 运行时。
type CallbackSpec struct {
	OnSuccess *CallbackTask `json:"on_success,omitempty"`
	OnError   *CallbackTask `json:"on_error,omitempty"`
}

// CallbackTask 是派生任务的创建规格（对齐 proto TaskSpec）。
type CallbackTask struct {
	Type           string          `json:"type"`
	Operator       string          `json:"operator,omitempty"`
	Priority       int32           `json:"priority,omitempty"`
	Channel        string          `json:"channel,omitempty"`
	VPC            string          `json:"vpc,omitempty"`
	Node           string          `json:"node,omitempty"`
	Label          string          `json:"label,omitempty"`
	HashBucket     int32           `json:"hash_bucket,omitempty"`
	TimeoutMS      int64           `json:"timeout_ms,omitempty"`
	MaxAttempts    int32           `json:"max_attempts,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Callback       *CallbackSpec   `json:"callback,omitempty"`
	BizRaceLabels  []string        `json:"biz_race_labels,omitempty"`
	BizRaceEntry   string          `json:"biz_race_entry,omitempty"`
	BizGroup       string          `json:"biz_group,omitempty"`
	BizBatchID     string          `json:"biz_batch_id,omitempty"`
}

// MarshalCallback 把回调规格编码为可写入 jsonb 列的字节；nil 返回 nil。
func MarshalCallback(spec *CallbackSpec) ([]byte, error) {
	if spec == nil {
		return nil, nil
	}
	return json.Marshal(spec)
}

// UnmarshalCallback 解析任务行的 callback jsonb；空字节返回 nil。
func UnmarshalCallback(raw []byte) (*CallbackSpec, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	spec := &CallbackSpec{}
	if err := json.Unmarshal(raw, spec); err != nil {
		return nil, err
	}
	if spec.OnSuccess == nil && spec.OnError == nil {
		return nil, nil
	}
	return spec, nil
}

// Derive 依据终态结果选择派生规格：succeeded 走 OnSuccess，否则走 OnError；无对应规格返回 nil。
func (s *CallbackSpec) Derive(succeeded bool) *CallbackTask {
	if s == nil {
		return nil
	}
	if succeeded {
		return s.OnSuccess
	}
	return s.OnError
}

// Validate 校验回调规格：派生任务必须有 type。
func (s *CallbackSpec) Validate() error {
	if s == nil {
		return nil
	}
	for _, t := range []*CallbackTask{s.OnSuccess, s.OnError} {
		if t != nil && t.Type == "" {
			return errors.New("domain: callback task requires a type")
		}
	}
	return nil
}
