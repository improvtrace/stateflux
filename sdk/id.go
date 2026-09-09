package sdk

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// 雪花 ID（§3.1 id 字段）：41 位毫秒时间戳 + 10 位节点 ID + 12 位序列。
// 单节点每毫秒最多 4096 个 ID；时间基准 epoch 为 2025-01-01（约可用 69 年）。
const (
	snowflakeEpoch int64 = 1735689600000 // 2025-01-01T00:00:00Z (ms)
	nodeIDBits           = 10
	sequenceBits         = 12
	maxNodeID            = (1 << nodeIDBits) - 1
	maxSequence          = (1 << sequenceBits) - 1
	nodeIDShift          = sequenceBits
	timestampShift       = sequenceBits + nodeIDBits
)

// ErrSnowflakeBackward 时钟回拨超过容忍窗口。
var ErrSnowflakeBackward = errors.New("sdk: snowflake clock moved backwards")

// Snowflake 生成任务 ID。同一进程内应持有单一实例（NewSnowflake 对同节点返回共享实例）。
type Snowflake struct {
	mu       sync.Mutex
	nodeID   int64
	lastTS   int64
	sequence int64
}

var (
	defaultSnowflakeOnce sync.Once
	defaultSnowflake     *Snowflake
)

// NewSnowflake 构造指定节点 ID（0 ~ 1023）的生成器。
func NewSnowflake(nodeID int64) (*Snowflake, error) {
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("sdk: snowflake node id %d out of range [0, %d]", nodeID, maxNodeID)
	}
	return &Snowflake{nodeID: nodeID}, nil
}

// DefaultSnowflake 返回节点 ID 0 的进程级默认生成器（单机/嵌入形态的便捷入口）。
func DefaultSnowflake() *Snowflake {
	defaultSnowflakeOnce.Do(func() {
		defaultSnowflake, _ = NewSnowflake(0)
	})
	return defaultSnowflake
}

// Next 生成下一个 ID。
func (s *Snowflake) Next() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	if now < s.lastTS {
		// 小幅回拨（< 5ms）自旋等待；更大回拨直接报错，避免生成重复 ID。
		if s.lastTS-now > 5 {
			return 0, fmt.Errorf("%w: %dms", ErrSnowflakeBackward, s.lastTS-now)
		}
		for time.Now().UnixMilli() <= s.lastTS {
			time.Sleep(time.Millisecond)
		}
		now = time.Now().UnixMilli()
	}
	if now == s.lastTS {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			for time.Now().UnixMilli() <= s.lastTS {
				time.Sleep(time.Millisecond)
			}
			now = time.Now().UnixMilli()
		}
	} else {
		s.sequence = 0
	}
	s.lastTS = now
	return ((now - snowflakeEpoch) << timestampShift) | (s.nodeID << nodeIDShift) | s.sequence, nil
}

// MustNext 同 Next，出错时 panic（时钟回拨超过容忍窗口属于部署级故障）。
func (s *Snowflake) MustNext() int64 {
	id, err := s.Next()
	if err != nil {
		panic(err)
	}
	return id
}

// ParseSnowflakeTime 从 ID 中还原毫秒时间戳。
func ParseSnowflakeTime(id int64) time.Time {
	ms := (id >> timestampShift) + snowflakeEpoch
	return time.UnixMilli(ms)
}
