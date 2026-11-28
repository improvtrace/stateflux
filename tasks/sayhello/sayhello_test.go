package sayhello

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpec 断言任务定义合法：可通过注册表校验，且按演示定位是非 Durable
// 直发、显式启用。
func TestSpec(t *testing.T) {
	spec := Spec()
	require.NoError(t, spec.Validate())
	assert.Equal(t, Resource, spec.Resource)
	assert.Equal(t, Cron, spec.Cron)
	assert.True(t, spec.Enabled, "Enabled 零值即禁用，任务必须显式开启")
	assert.False(t, spec.Durable, "演示任务走直发，不落表")
	assert.NotNil(t, spec.Handler)

	// 固定参数与缺省问候语一致（两个常量须同步维护）
	var p Payload
	require.NoError(t, json.Unmarshal(spec.Payload, &p))
	assert.Equal(t, defaultGreeting, p.Greeting)
}

// TestHandle 断言执行语义：空参数与自定义问候都成功，非法 payload 报错。
func TestHandle(t *testing.T) {
	require.NoError(t, Handle("", 0), "空参数回退缺省问候")
	require.NoError(t, Handle(`{"greeting":"hi"}`, 0), "自定义问候")
	require.NoError(t, Handle(`{"greeting":""}`, 42), "空问候回退缺省")
	assert.ErrorContains(t, Handle("{", 0), "decode payload")
}
