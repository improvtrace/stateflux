package redis

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/improvtrace/stateflux/internal/eventbus/channel"
)

// envelopeMagic 是 Redis 载荷帧的魔数（'SFXE'）：用于快速识别非本框架写入的脏数据。
var envelopeMagic = [4]byte{'S', 'F', 'X', 'E'}

const envelopeVersion byte = 1

var errNilHandler = errors.New("eventbus/channel/redis: Subscribe requires a non-nil channel.Handler")

// encodeEnvelope 把信封序列化为自描述二进制帧：magic + version + 变长字段。
// 通道只搬运字节、不解释业务语义，因此这里保留 Topic/Key/Attempt/CorrelationID/Target/
// Headers/Payload 全部字段，供消费端还原信封（对标 machinery 的可辨识消息头）。
func encodeEnvelope(env channel.Envelope) ([]byte, error) {
	headers, err := json.Marshal(env.Headers)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 32+len(env.Payload))
	out = append(out, envelopeMagic[:]...)
	out = append(out, envelopeVersion)
	out = appendString(out, string(env.Topic))
	out = appendString(out, env.Key)
	out = appendString(out, env.CorrelationID)
	out = appendString(out, env.Target)
	out = binary.AppendVarint(out, env.Attempt)
	out = appendString(out, string(headers))
	out = appendString(out, string(env.Payload))
	return out, nil
}

// decodeEnvelope 还原信封；topic 作为缺省值在帧内 Topic 为空时使用。
func decodeEnvelope(data []byte, topic channel.Topic) (channel.Envelope, error) {
	if len(data) < 5 || string(data[:4]) != string(envelopeMagic[:]) || data[4] != envelopeVersion {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: bad envelope frame")
	}
	rest := data[5:]
	var (
		env        channel.Envelope
		topicStr   string
		rawHeaders string
		rawPayload string
		ok         bool
	)
	if rest, ok = readString(rest, &topicStr); !ok {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated topic")
	}
	env.Topic = channel.Topic(topicStr)
	if rest, ok = readString(rest, &env.Key); !ok {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated key")
	}
	if rest, ok = readString(rest, &env.CorrelationID); !ok {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated correlation")
	}
	if rest, ok = readString(rest, &env.Target); !ok {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated target")
	}
	attempt, n := binary.Varint(rest)
	if n <= 0 {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated attempt")
	}
	env.Attempt = attempt
	rest = rest[n:]
	if rest, ok = readString(rest, &rawHeaders); !ok {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated headers")
	}
	if rawHeaders != "" && rawHeaders != "null" {
		if err := json.Unmarshal([]byte(rawHeaders), &env.Headers); err != nil {
			return channel.Envelope{}, err
		}
	}
	if _, ok = readString(rest, &rawPayload); !ok {
		return channel.Envelope{}, errors.New("eventbus/channel/redis: truncated payload")
	}
	env.Payload = []byte(rawPayload)
	if env.Topic == "" {
		env.Topic = topic
	}
	return env, nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func readString(src []byte, dst *string) ([]byte, bool) {
	n, m := binary.Uvarint(src)
	if m <= 0 {
		return src, false
	}
	src = src[m:]
	if uint64(len(src)) < n {
		return src, false
	}
	*dst = string(src[:n])
	return src[n:], true
}
