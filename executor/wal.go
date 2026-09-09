package executor

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	dispatchv1 "github.com/improvtrace/stateflux/proto/gen/dispatchv1"
)

// WAL 执行侧结果 WAL（§5.4，异步结果存储）：append-only 磁盘日志，handler 返回后先落
// WAL 再视为完成；内存索引供 Collect 拉取；Ack 推进截断水位（按段删除）；重启重放未 Ack
// 条目继续供拉，避免已执行任务重跑。
//
// WAL 是非权威派生状态（§1.2.7）：文件损坏/丢失 → 条目作废，由对账 R1 重跑兜底，不变式不变。
// 高水位反压：未 Ack 积压超过阈值（默认 10k 条或 256MB）时暂停 BRPOP（§6.4）。
//
// Ack 语义（at-least-once）：崩溃发生在「Ack 后、段删除前」时，重放会重新供拉已 Ack 条目，
// 由 PG 终态回写幂等（ON CONFLICT DO NOTHING）收敛。
type WAL struct {
	dir          string
	maxEntries   int64
	maxBytes     int64
	segmentBytes int64
	log          *slog.Logger

	mu      sync.Mutex
	seq     int                        // 当前段序号
	file    *os.File                   // 当前段
	writer  *bufio.Writer              // 当前段缓冲写
	size    int64                      // 当前段已写字节
	bytes   int64                      // 未 Ack 记录总字节（反压水位）
	entries map[int64]*walEntry        // task_id → 未 Ack 条目（内存索引）
	order   []int64                    // 追加序（FIFO 供拉）
	segs    map[int]map[int64]struct{} // 段 → 段内未删条目
	unacked int64                      // 未 Ack 条数（当前段内已 Ack 条目保留供重放，不计水位）
	closed  bool
}

type walEntry struct {
	msg     *dispatchv1.ResultEntry
	segment int
	size    int // header + payload 字节
	acked   bool
}

// WALConfig WAL 参数。
type WALConfig struct {
	Dir          string
	MaxEntries   int64
	MaxBytes     int64
	SegmentBytes int64
}

// OpenWAL 打开（或重放）WAL。重放读取全部段文件，重建未 Ack 内存索引；
// 单条记录损坏（CRC 不符/截断）即作废该记录及其后所有记录（非权威派生状态，R1 兜底）。
func OpenWAL(cfg WALConfig, log *slog.Logger) (*WAL, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("wal: dir is required")
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 10000
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 256 << 20
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = 64 << 20
	}
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", cfg.Dir, err)
	}
	w := &WAL{
		dir:          cfg.Dir,
		maxEntries:   cfg.MaxEntries,
		maxBytes:     cfg.MaxBytes,
		segmentBytes: cfg.SegmentBytes,
		entries:      make(map[int64]*walEntry),
		segs:         make(map[int]map[int64]struct{}),
		log:          log,
	}
	if err := w.replay(); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.rotate(); err != nil {
		_ = w.Close()
		return nil, err
	}
	return w, nil
}

// record 布局：magic(2) | len(4, LE) | crc32(4, LE, 对 payload) | payload。
const (
	walMagic     = uint16(0x574C) // "WL"
	walHeaderLen = 10
)

// Append 结果落 WAL（handler 返回后先落 WAL 再视为完成，§5.4）。每条 write + fsync。
func (w *WAL) Append(msg *dispatchv1.ResultEntry) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("wal: marshal: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal: closed")
	}
	recLen := walHeaderLen + len(payload)
	if int64(recLen) > w.segmentBytes {
		return fmt.Errorf("wal: record too large: %d", len(payload))
	}
	// 段滚动。
	if w.size+int64(recLen) > w.segmentBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	var header [walHeaderLen]byte
	binary.LittleEndian.PutUint16(header[0:2], walMagic)
	binary.LittleEndian.PutUint32(header[2:6], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[6:10], crc32.ChecksumIEEE(payload))
	if _, err := w.writer.Write(header[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	w.size += int64(recLen)
	w.bytes += int64(recLen)
	id := msg.GetTaskId()
	if _, exists := w.entries[id]; !exists {
		w.order = append(w.order, id)
		w.unacked++
	} else {
		// 同 task_id 重复落 WAL（重放窗口内重复执行）：从旧段计数中移除。
		if seg, ok := w.segs[w.entries[id].segment]; ok {
			delete(seg, id)
		}
		w.bytes -= int64(w.entries[id].size)
		if !w.entries[id].acked {
			w.unacked-- // 旧条目未 Ack 就被覆盖（罕见：重放窗口内重跑），水位以新条目计
		}
	}
	w.entries[id] = &walEntry{msg: msg, segment: w.seq, size: recLen}
	if w.segs[w.seq] == nil {
		w.segs[w.seq] = make(map[int64]struct{})
	}
	w.segs[w.seq][id] = struct{}{}
	return nil
}

// Collect 拉取未 Ack 条目（重入安全：WAL 保留，等 Ack，§5.5）。
func (w *WAL) Collect(limit int) []*dispatchv1.ResultEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := make([]*dispatchv1.ResultEntry, 0, min(limit, len(w.order)))
	for _, id := range w.order {
		if len(out) >= limit {
			break
		}
		if e, ok := w.entries[id]; ok && !e.acked {
			out = append(out, e.msg)
		}
	}
	return out
}

// Ack 推进截断水位：标记 Acked 并释放水位字节；某段全部 Acked 后删除段文件。
func (w *WAL) Ack(taskIDs []int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	acked := 0
	touched := make(map[int]struct{})
	for _, id := range taskIDs {
		e, ok := w.entries[id]
		if !ok || e.acked {
			continue
		}
		e.acked = true
		w.unacked--
		w.bytes -= int64(e.size)
		touched[e.segment] = struct{}{}
		acked++
	}
	// 段回收：非当前段且全部条目 Acked → 删文件、清索引。
	for seg := range touched {
		if seg == w.seq {
			continue
		}
		members, ok := w.segs[seg]
		if !ok {
			continue
		}
		full := true
		for id := range members {
			if e, exists := w.entries[id]; exists && !e.acked {
				full = false
				break
			}
		}
		if !full {
			continue
		}
		if err := os.Remove(w.segmentPath(seg)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return acked, fmt.Errorf("wal: remove segment %d: %w", seg, err)
		}
		for id := range members {
			delete(w.entries, id)
		}
		delete(w.segs, seg)
		kept := w.order[:0]
		for _, id := range w.order {
			if _, ok := w.entries[id]; ok {
				kept = append(kept, id)
			}
		}
		w.order = kept
	}
	return acked, nil
}

// Backlog 未 Ack 积压（条数、字节数）——高水位反压依据（§6.4）。
func (w *WAL) Backlog() (int64, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.unacked, w.bytes
}

// OverHighWatermark 是否达到高水位（暂停 BRPOP，§5.4）。
func (w *WAL) OverHighWatermark() bool {
	n, b := w.Backlog()
	return n >= w.maxEntries || b >= w.maxBytes
}

// Close 落盘并关闭。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.writer != nil {
		_ = w.writer.Flush()
	}
	if w.file != nil {
		_ = w.file.Sync()
		return w.file.Close()
	}
	return nil
}

func (w *WAL) segmentPath(seq int) string {
	return filepath.Join(w.dir, fmt.Sprintf("wal-%06d.log", seq))
}

// rotate 打开下一个段。
func (w *WAL) rotate() error {
	if w.file != nil {
		_ = w.writer.Flush()
		_ = w.file.Sync()
		_ = w.file.Close()
	}
	w.seq++
	f, err := os.OpenFile(w.segmentPath(w.seq), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	w.file = f
	w.writer = bufio.NewWriter(f)
	w.size = 0
	return nil
}

// replay 重放：按段序读取，重建未 Ack 索引。损坏记录作废该记录及其后内容。
func (w *WAL) replay() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("wal: read dir: %w", err)
	}
	var segs []int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		var seq int
		if _, err := fmt.Sscanf(name, "wal-%d.log", &seq); err != nil {
			continue
		}
		segs = append(segs, seq)
	}
	sort.Ints(segs)
	for _, seq := range segs {
		n, err := w.replaySegment(w.segmentPath(seq), seq)
		if err != nil {
			// 非权威派生状态：损坏即作废剩余条目（R1 重跑兜底），不阻断启动。
			w.log.Warn("wal: segment corrupt, discarding remaining records",
				"segment", seq, "recovered", n, "err", err)
		}
		if n > 0 {
			w.log.Info("wal: replayed segment", "segment", seq, "entries", n)
		}
	}
	if len(segs) > 0 {
		w.seq = segs[len(segs)-1] // 后续 rotate 从最后一段递增
		w.seq--
	}
	return nil
}

func (w *WAL) replaySegment(path string, seq int) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	count := 0
	var header [walHeaderLen]byte
	for {
		if err := readFull(f, header[:]); err != nil {
			return count, err // EOF（尾部半条，崩溃窗口）同样按损坏处理但通常为 0 条剩余
		}
		if magic := binary.LittleEndian.Uint16(header[0:2]); magic != walMagic {
			return count, fmt.Errorf("bad magic %#x", magic)
		}
		length := binary.LittleEndian.Uint32(header[2:6])
		sum := binary.LittleEndian.Uint32(header[6:10])
		if length == 0 || int(length) > int(w.segmentBytes) {
			return count, fmt.Errorf("bad length %d", length)
		}
		payload := make([]byte, length)
		if err := readFull(f, payload); err != nil {
			return count, fmt.Errorf("truncated payload: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != sum {
			return count, fmt.Errorf("crc mismatch")
		}
		msg := &dispatchv1.ResultEntry{}
		if err := proto.Unmarshal(payload, msg); err != nil {
			return count, fmt.Errorf("unmarshal: %w", err)
		}
		id := msg.GetTaskId()
		if _, exists := w.entries[id]; !exists {
			w.order = append(w.order, id)
		}
		w.entries[id] = &walEntry{msg: msg, segment: seq, size: walHeaderLen + int(length)}
		if w.segs[seq] == nil {
			w.segs[seq] = make(map[int64]struct{})
		}
		w.segs[seq][id] = struct{}{}
		w.bytes += int64(walHeaderLen + int(length))
		count++
	}
}

// readFull 恰好读满 buf；EOF 返回 errUnexpectedEOF 语义（尾部半条按损坏计）。
func readFull(f *os.File, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := f.Read(buf[total:])
		total += n
		if err != nil {
			return fmt.Errorf("read full: %w", err)
		}
	}
	return nil
}
