package worker

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/improvtrace/stateflux/api/stateflux/task/v1"
)

// WAL 记录类型（§5.4）：add 追加未确认结果，ack 标记已发布。
const (
	walRecordAdd byte = 0x01
	walRecordAck byte = 0x02
)

// compactAckThreshold 是触发压缩的 ack 记录数：超过即重写文件，避免日志无界增长（§14.7）。
const compactAckThreshold = 1024

// DiskWAL 是 append-only 磁盘 WAL（§5.4、§14.7）：Add 追加记录，Ack 追加确认记录，
// 进程重启时重放并只保留未确认结果。超过高水位时 HighWater 报告反压（暂停新订阅）。
//
// 与设计一致：未确认结果可有意丢弃（压缩/截断只保留在途），正确性由 R1 兜底（§6.2）。
type DiskWAL struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	max     int
	maxByte int64

	entries map[walKey]*taskv1.ResultEvent
	order   []walKey
	bytes   int64

	acksSinceCompact int
}

// OpenDiskWAL 打开（必要时重放）磁盘 WAL。目录不存在时自动创建。
func OpenDiskWAL(path string, max int, maxByte int64) (*DiskWAL, error) {
	if path == "" {
		return nil, errors.New("worker: empty WAL path")
	}
	if max <= 0 {
		max = DefaultWALMax
	}
	if maxByte <= 0 {
		maxByte = DefaultWALMaxBytes
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	w := &DiskWAL{
		path:    path,
		max:     max,
		maxByte: maxByte,
		entries: map[walKey]*taskv1.ResultEvent{},
	}
	if err := w.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w.file = f
	return w, nil
}

// replay 读取全部记录重建未确认集合；损坏的尾部记录被忽略（易失派生数据，§14.7）。
func (w *DiskWAL) replay() error {
	f, err := os.Open(w.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		recType, err := r.ReadByte()
		if err != nil {
			return nil // EOF 或尾部损坏
		}
		switch recType {
		case walRecordAdd:
			n, err := binary.ReadUvarint(r)
			if err != nil {
				return nil
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil
			}
			ev := &taskv1.ResultEvent{}
			if err := proto.Unmarshal(buf, ev); err != nil {
				continue
			}
			w.applyAdd(ev)
		case walRecordAck:
			taskID, err := binary.ReadVarint(r)
			if err != nil {
				return nil
			}
			attempt, err := binary.ReadVarint(r)
			if err != nil {
				return nil
			}
			w.applyAck(taskID, attempt)
		default:
			return nil // 未知记录：停止重放，保底不阻塞启动
		}
	}
}

// Add 追加一条未确认结果。
func (w *DiskWAL) Add(ev *taskv1.ResultEvent) {
	if ev == nil {
		return
	}
	body, err := proto.Marshal(ev)
	if err != nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendAdd(body); err != nil {
		return
	}
	w.applyAdd(ev)
}

// Ack 追加确认记录并移出未确认集合。
func (w *DiskWAL) Ack(taskID, attempt int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.entries[walKey{TaskID: taskID, Attempt: attempt}]; !ok {
		return
	}
	if err := w.appendAck(taskID, attempt); err != nil {
		return
	}
	w.applyAck(taskID, attempt)
	w.acksSinceCompact++
	if w.acksSinceCompact >= compactAckThreshold {
		_ = w.compactLocked()
	}
}

func (w *DiskWAL) applyAdd(ev *taskv1.ResultEvent) {
	k := walKey{TaskID: ev.GetTaskId(), Attempt: ev.GetAttempt()}
	if old, ok := w.entries[k]; ok {
		w.bytes -= int64(len(old.GetResult()))
	} else {
		w.order = append(w.order, k)
	}
	w.entries[k] = ev
	w.bytes += int64(len(ev.GetResult()))
	for len(w.order) > w.max {
		oldest := w.order[0]
		w.order = w.order[1:]
		if old, ok := w.entries[oldest]; ok {
			w.bytes -= int64(len(old.GetResult()))
			delete(w.entries, oldest)
		}
	}
}

func (w *DiskWAL) applyAck(taskID, attempt int64) {
	k := walKey{TaskID: taskID, Attempt: attempt}
	ev, ok := w.entries[k]
	if !ok {
		return
	}
	w.bytes -= int64(len(ev.GetResult()))
	delete(w.entries, k)
	for i, key := range w.order {
		if key == k {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
}

func (w *DiskWAL) appendAdd(body []byte) error {
	if w.file == nil {
		return errors.New("worker: WAL is closed")
	}
	buf := make([]byte, 0, len(body)+10)
	buf = append(buf, walRecordAdd)
	buf = binary.AppendUvarint(buf, uint64(len(body)))
	buf = append(buf, body...)
	_, err := w.file.Write(buf)
	return err
}

func (w *DiskWAL) appendAck(taskID, attempt int64) error {
	if w.file == nil {
		return errors.New("worker: WAL is closed")
	}
	buf := make([]byte, 0, 20)
	buf = append(buf, walRecordAck)
	buf = binary.AppendVarint(buf, taskID)
	buf = binary.AppendVarint(buf, attempt)
	_, err := w.file.Write(buf)
	return err
}

// compactLocked 把文件重写为仅含未确认结果（调用方持锁）。
func (w *DiskWAL) compactLocked() error {
	if w.file == nil {
		return nil
	}
	tmp := w.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, k := range w.order {
		ev, ok := w.entries[k]
		if !ok {
			continue
		}
		body, err := proto.Marshal(ev)
		if err != nil {
			continue
		}
		buf := make([]byte, 0, len(body)+10)
		buf = append(buf, walRecordAdd)
		buf = binary.AppendUvarint(buf, uint64(len(body)))
		buf = append(buf, body...)
		if _, err := bw.Write(buf); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	reopened, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.file = reopened
	w.acksSinceCompact = 0
	return nil
}

// Compact 显式压缩（运维/测试）。
func (w *DiskWAL) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.compactLocked()
}

// Pending 返回未确认结果快照。
func (w *DiskWAL) Pending() []*taskv1.ResultEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*taskv1.ResultEvent, 0, len(w.order))
	for _, k := range w.order {
		if ev, ok := w.entries[k]; ok {
			out = append(out, ev)
		}
	}
	return out
}

// Len 返回未确认条目数。
func (w *DiskWAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

// Bytes 返回未确认载荷字节数。
func (w *DiskWAL) Bytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes
}

// HighWater 报告是否达到条目或字节高水位。
func (w *DiskWAL) HighWater() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries) >= w.max || (w.maxByte > 0 && w.bytes >= w.maxByte)
}

// Close 关闭文件句柄。
func (w *DiskWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

var _ WAL = (*DiskWAL)(nil)

// 保证 fmt 被引用（错误信息构造）。
var _ = fmt.Sprintf
