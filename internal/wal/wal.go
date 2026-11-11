// Package wal implements a checksummed, append-only log for accepted OTLP
// requests. Data and acknowledgement records share one ordered file.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

const (
	headerSize         = 10 // version, kind, sequence
	version            = 1
	dataRecord    byte = 1
	ackRecord     byte = 2
	maxRecordSize      = 64 << 20
)

var compactThreshold int64 = 64 << 20

const maxSyncInterval = time.Second

type Options struct {
	Sync         bool
	SyncInterval time.Duration
	MaxSyncBatch int
}

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	Sequence uint64
	Request  *logs.ExportLogsServiceRequest
}

type Log struct {
	mu           sync.Mutex
	ops          sync.WaitGroup
	file         *os.File
	path         string
	syncWrites   bool
	syncInterval time.Duration
	maxSyncBatch int
	syncWaiters  []chan error
	syncSignal   chan struct{}
	syncStop     chan struct{}
	syncDone     chan struct{}
	closed       bool
	fatal        error
	syncCount    uint64
	next         uint64
	pending      map[uint64]*logs.ExportLogsServiceRequest
}

func Open(dir string, syncWrites bool) (*Log, []Record, error) {
	return OpenWithOptions(dir, Options{Sync: syncWrites, SyncInterval: 2 * time.Millisecond, MaxSyncBatch: 32})
}

func OpenWithOptions(dir string, options Options) (*Log, []Record, error) {
	if dir == "" {
		return nil, nil, errors.New("WAL directory is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, nil, fmt.Errorf("create WAL directory: %w", err)
	}
	path := filepath.Join(dir, "collector.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("open WAL: %w", err)
	}
	if options.Sync && options.SyncInterval <= 0 {
		f.Close()
		return nil, nil, errors.New("WAL sync interval must be positive")
	}
	if options.Sync && options.SyncInterval > maxSyncInterval {
		f.Close()
		return nil, nil, fmt.Errorf("WAL sync interval must be <= %s", maxSyncInterval)
	}
	if options.Sync && options.MaxSyncBatch <= 0 {
		f.Close()
		return nil, nil, errors.New("WAL sync batch must be positive")
	}
	w := &Log{file: f, path: path, syncWrites: options.Sync, syncInterval: options.SyncInterval, maxSyncBatch: options.MaxSyncBatch, pending: make(map[uint64]*logs.ExportLogsServiceRequest)}
	if options.Sync {
		d, err := os.Open(dir)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		err = d.Sync()
		_ = d.Close()
		if err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("sync WAL directory: %w", err)
		}
	}
	if err := w.recover(); err != nil {
		f.Close()
		return nil, nil, err
	}
	if options.Sync {
		w.syncSignal = make(chan struct{}, 1)
		w.syncStop = make(chan struct{})
		w.syncDone = make(chan struct{})
		go w.syncLoop()
	}
	replay := make([]Record, 0, len(w.pending))
	for seq, req := range w.pending {
		replay = append(replay, Record{seq, req})
	}
	// Sequence order is needed to make recovery deterministic.
	sort.Slice(replay, func(i, j int) bool { return replay[i].Sequence < replay[j].Sequence })
	return w, replay, nil
}

func (w *Log) recover() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(w.file)
	var offset int64
	for {
		start := offset
		var size [4]byte
		_, err := io.ReadFull(r, size[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if err := w.file.Truncate(start); err != nil {
				return err
			}
			break
		}
		n := binary.BigEndian.Uint32(size[:])
		if n < headerSize+4 || n > maxRecordSize {
			if err := w.file.Truncate(start); err != nil {
				return err
			}
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			if err := w.file.Truncate(start); err != nil {
				return err
			}
			break
		}
		offset += int64(4 + n)
		if crc32.Checksum(body[:len(body)-4], checksumTable) != binary.BigEndian.Uint32(body[len(body)-4:]) {
			if err := w.file.Truncate(start); err != nil {
				return err
			}
			break
		}
		if body[0] != version {
			return fmt.Errorf("unsupported WAL version %d at offset %d", body[0], start)
		}
		kind := body[1]
		seq := binary.BigEndian.Uint64(body[2:10])
		payload := body[headerSize : len(body)-4]
		if seq >= w.next {
			w.next = seq + 1
		}
		switch kind {
		case dataRecord:
			var req logs.ExportLogsServiceRequest
			if err := proto.Unmarshal(payload, &req); err != nil {
				return fmt.Errorf("decode WAL request %d: %w", seq, err)
			}
			w.pending[seq] = &req
		case ackRecord:
			delete(w.pending, seq)
		default:
			return fmt.Errorf("unknown WAL record type %d at offset %d", kind, start)
		}
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	if err == nil && w.syncWrites {
		err = w.file.Sync()
	}
	return err
}

func (w *Log) Append(req *logs.ExportLogsServiceRequest) (uint64, error) {
	payload, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	if err := w.checkWritableLocked(); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	w.ops.Add(1)
	defer w.ops.Done()
	seq := w.next
	if err := writeFrame(w.file, dataRecord, seq, payload); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	w.next++
	copyReq := proto.Clone(req).(*logs.ExportLogsServiceRequest)
	w.pending[seq] = copyReq
	if !w.syncWrites {
		w.mu.Unlock()
		return seq, nil
	}
	done := w.queueSyncLocked()
	w.mu.Unlock()
	if err := <-done; err != nil {
		return 0, err
	}
	return seq, nil
}

func (w *Log) Ack(seq uint64) error {
	return w.AckBatch([]uint64{seq})
}

// AckBatch checkpoints a set of exported requests with one WAL sync. A
// successful return means the acknowledgement frames are durable when sync is
// enabled. Repeated or already-acknowledged sequences are ignored.
func (w *Log) AckBatch(sequences []uint64) error {
	w.mu.Lock()
	if err := w.checkWritableLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	pending := make([]uint64, 0, len(sequences))
	seen := make(map[uint64]struct{}, len(sequences))
	for _, seq := range sequences {
		if _, duplicate := seen[seq]; duplicate {
			continue
		}
		seen[seq] = struct{}{}
		if _, ok := w.pending[seq]; ok {
			pending = append(pending, seq)
		}
	}
	if len(pending) == 0 {
		w.mu.Unlock()
		return nil
	}
	w.ops.Add(1)
	defer w.ops.Done()
	for _, seq := range pending {
		if err := writeFrame(w.file, ackRecord, seq, nil); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	if w.syncWrites {
		done := w.queueSyncLocked()
		w.mu.Unlock()
		if err := <-done; err != nil {
			return err
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, seq := range pending {
			delete(w.pending, seq)
		}
		return w.compactIfNeeded()
	}
	for _, seq := range pending {
		delete(w.pending, seq)
	}
	err := w.compactIfNeeded()
	w.mu.Unlock()
	return err
}

func (w *Log) checkWritableLocked() error {
	if w.closed {
		return errors.New("WAL is closed")
	}
	if w.fatal != nil {
		return fmt.Errorf("WAL sync failed: %w", w.fatal)
	}
	return nil
}

func (w *Log) queueSyncLocked() <-chan error {
	done := make(chan error, 1)
	w.syncWaiters = append(w.syncWaiters, done)
	select {
	case w.syncSignal <- struct{}{}:
	default:
	}
	return done
}

func (w *Log) syncLoop() {
	defer close(w.syncDone)
	for {
		select {
		case <-w.syncSignal:
		case <-w.syncStop:
			w.flushSyncWaiters()
			return
		}
		w.mu.Lock()
		ready := len(w.syncWaiters) >= w.maxSyncBatch
		w.mu.Unlock()
		if ready {
			w.flushSyncWaiters()
			continue
		}
		timer := time.NewTimer(w.syncInterval)
		flush := false
		for !flush {
			select {
			case <-timer.C:
				flush = true
			case <-w.syncSignal:
				w.mu.Lock()
				count := len(w.syncWaiters)
				w.mu.Unlock()
				if count >= w.maxSyncBatch {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					flush = true
				}
			case <-w.syncStop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				w.flushSyncWaiters()
				return
			}
		}
		w.flushSyncWaiters()
	}
}

func (w *Log) flushSyncWaiters() {
	w.mu.Lock()
	waiters := w.syncWaiters
	w.syncWaiters = nil
	var err error
	if len(waiters) > 0 {
		err = w.file.Sync()
		w.syncCount++
		if err != nil && w.fatal == nil {
			w.fatal = err
		}
	}
	w.mu.Unlock()
	for _, done := range waiters {
		done <- err
		close(done)
	}
}

func (w *Log) compactIfNeeded() error {
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < compactThreshold {
		return nil
	}
	tmpPath := w.path + ".compact"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	seqs := make([]uint64, 0, len(w.pending))
	for seq := range w.pending {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		payload, err := proto.Marshal(w.pending[seq])
		if err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		if err := writeFrame(tmp, dataRecord, seq, payload); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
	}
	if w.syncWrites {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file, err = os.OpenFile(w.path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if w.syncWrites {
		dir, err := os.Open(filepath.Dir(w.path))
		if err != nil {
			return err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func writeFrame(file *os.File, kind byte, seq uint64, payload []byte) error {
	body := make([]byte, headerSize+len(payload)+4)
	body[0], body[1] = version, kind
	binary.BigEndian.PutUint64(body[2:10], seq)
	copy(body[headerSize:], payload)
	binary.BigEndian.PutUint32(body[len(body)-4:], crc32.Checksum(body[:len(body)-4], checksumTable))
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(body)))
	start, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	rollback := func(cause error) error { _ = file.Truncate(start); _, _ = file.Seek(start, io.SeekStart); return cause }
	if _, err := file.Write(size[:]); err != nil {
		return rollback(err)
	}
	if _, err := file.Write(body); err != nil {
		return rollback(err)
	}
	return nil
}

func (w *Log) Close() error {
	w.mu.Lock()
	if w.file == nil {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()
	w.ops.Wait()
	if w.syncWrites {
		close(w.syncStop)
		<-w.syncDone
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var syncErr error
	if w.syncWrites {
		syncErr = w.fatal
	}
	if w.syncWrites {
		if err := w.file.Sync(); syncErr == nil {
			syncErr = err
		}
	}
	err := w.file.Close()
	w.file = nil
	return errors.Join(syncErr, err)
}

// SyncCount returns the number of group filesystem syncs performed since Open.
func (w *Log) SyncCount() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncCount
}
