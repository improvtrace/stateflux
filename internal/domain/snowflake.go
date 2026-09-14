// Package domain 是领域层与引擎共享内核（§8）：任务模型、聚合接口与执行接入契约。
// 本文件提供任务 ID 生成（§3.1：任务 ID 由客户端雪花生成，非自增）。
package domain

import (
	"hash/fnv"
	"sync"
	"time"
)

// Snowflake 是 64 位雪花 ID 生成器：41 位毫秒时间戳 + 10 位节点 + 12 位序列。
// 节点位来自节点 ID 的稳定哈希，保证同一集群内不同节点不撞号。
type Snowflake struct {
	mu       sync.Mutex
	epoch    int64
	nodeID   int64
	lastMs   int64
	sequence int64
}

// NewSnowflake 用节点 ID 构造生成器（节点 ID 为空时用 0）。
func NewSnowflake(nodeID string) *Snowflake {
	return &Snowflake{
		epoch:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		nodeID: nodeBits(nodeID),
	}
}

// Next 生成下一个 ID；同一毫秒内序列耗尽时自旋到下一毫秒。
func (s *Snowflake) Next() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	if now < s.lastMs {
		// 时钟回拨：保守等待到上次时间，避免产生重复 ID。
		now = s.lastMs
	}
	if now == s.lastMs {
		s.sequence = (s.sequence + 1) & 0xFFF
		if s.sequence == 0 {
			for now <= s.lastMs {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		s.sequence = 0
	}
	s.lastMs = now
	return ((now - s.epoch) << 22) | (s.nodeID << 12) | s.sequence
}

// NextN 批量生成 n 个 ID。
func (s *Snowflake) NextN(n int) []int64 {
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s.Next())
	}
	return out
}

func nodeBits(nodeID string) int64 {
	if nodeID == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeID))
	return int64(h.Sum32() & 0x3FF)
}
