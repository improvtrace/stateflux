package tasks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// okHandler 满足 handler 签名约定的空实现。
func okHandler(string, int64) error { return nil }

// TestRegistryEnabled 断言 Enabled 的过滤语义：Get/Specs 保留全部定义（禁用
// 任务仍可查询），Enabled/Timed 只返回启用的——定时条目对禁用任务不注册。
func TestRegistryEnabled(t *testing.T) {
	on := &TaskSpec{Resource: "on", Enabled: true, Handler: okHandler, Cron: "@every 1s"}
	off := &TaskSpec{Resource: "off", Handler: okHandler} // 零值 = 禁用
	offCron := &TaskSpec{Resource: "off_cron", Handler: okHandler, Cron: "@every 1s"}
	reg, err := NewRegistry(on, off, offCron)
	require.NoError(t, err)

	spec, ok := reg.Get("off")
	require.True(t, ok)
	assert.False(t, spec.Enabled, "零值即禁用")
	assert.Len(t, reg.Specs(), 3, "Specs 保留全部定义")

	assert.Equal(t, []*TaskSpec{on}, reg.Enabled())
	assert.Equal(t, []*TaskSpec{on}, reg.Timed(), "禁用任务不注册 cron 条目")
}
