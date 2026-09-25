// Package idgen 提供任务 ID 生成（§3.1：任务 ID 由客户端雪花生成，非自增）。
//
// 核心实现基于 github.com/bwmarrin/snowflake（41 位毫秒时间戳 + 10 位节点 + 12 位序列，
// 位布局与设计一致，Node 并发安全）。本包只固化项目策略，不做第二套生成逻辑：
//   - epoch 固定 2026-01-01 UTC（bwmarrin 默认 2010-11-04；epoch 只影响 ID 数值范围，
//     不影响唯一性，但固定它才能保证 ID 空间与设计/历史数据一致）；
//   - 节点位取节点 ID 字符串的 FNV-32a 哈希低 10 位——集群节点身份是字符串而非编号，
//     同一节点 ID 在全集群派生同一节点位，不同节点不撞号。
//
// 账本入队（biz/task）、终态回调派生（domain/data）与装配层共用同一生成器。
package idgen

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
)

// epochMs 是本项目固定的雪花纪元：2026-01-01 UTC（§3.1）。
var epochMs = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

var epochOnce sync.Once

// Snowflake 是任务 ID 生成器：包装 snowflake.Node，对内暴露 Next() int64。
type Snowflake struct {
	node *snowflake.Node
}

// NewSnowflake 用节点 ID 构造生成器（节点 ID 为空时节点位为 0）。
func NewSnowflake(nodeID string) *Snowflake {
	epochOnce.Do(func() { snowflake.Epoch = epochMs })
	node, err := snowflake.NewNode(nodeBits(nodeID))
	if err != nil {
		// nodeBits 已把节点位收敛到 [0,1023]，NewNode 不会失败；失败即实现 bug，
		// 在装配期尽早暴露。
		panic("idgen: " + err.Error())
	}
	return &Snowflake{node: node}
}

// Next 生成下一个 ID；同一毫秒内序列耗尽时自旋到下一毫秒。
//
// 与早期自研实现的差异：上游库对时钟回拨不做保守等待，回拨窗口内理论上可能生成
// 重复 ID；task_id 是账本主键，重复写入会被 PG 显式拒绝（报错可见），不会静默。
func (s *Snowflake) Next() int64 { return s.node.Generate().Int64() }

// nodeBits 把节点 ID 字符串映射到 10 位节点编号（0–1023）：FNV-32a 稳定哈希，
// 保证同一集群内不同节点不撞号。
func nodeBits(nodeID string) int64 {
	if nodeID == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeID))
	return int64(h.Sum32() & 0x3FF)
}
