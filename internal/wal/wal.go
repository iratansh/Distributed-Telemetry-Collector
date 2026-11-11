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

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

const (
	headerSize            = 10 // version, kind, sequence
	version               = 1
	dataRecord       byte = 1
	ackRecord        byte = 2
	maxRecordSize         = 64 << 20
	compactThreshold      = 64 << 20
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	Sequence uint64
	Request  *logs.ExportLogsServiceRequest
}

type Log struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	syncWrites bool
	next       uint64
	pending    map[uint64]*logs.ExportLogsServiceRequest
}

func Open(dir string, syncWrites bool) (*Log, []Record, error) {
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
	w := &Log{file: f, path: path, syncWrites: syncWrites, pending: make(map[uint64]*logs.ExportLogsServiceRequest)}
	if syncWrites {
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
	defer w.mu.Unlock()
	seq := w.next
	if err := w.write(dataRecord, seq, payload); err != nil {
		return 0, err
	}
	w.next++
	copyReq := proto.Clone(req).(*logs.ExportLogsServiceRequest)
	w.pending[seq] = copyReq
	return seq, nil
}

func (w *Log) Ack(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.pending[seq]; !ok {
		return nil
	}
	if err := w.write(ackRecord, seq, nil); err != nil {
		return err
	}
	delete(w.pending, seq)
	return w.compactIfNeeded()
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
	w.file, err = os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
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

func (w *Log) write(kind byte, seq uint64, payload []byte) error {
	start, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := writeFrame(w.file, kind, seq, payload); err != nil {
		return err
	}
	if w.syncWrites {
		if err := w.file.Sync(); err != nil {
			_ = w.file.Truncate(start)
			_, _ = w.file.Seek(start, io.SeekStart)
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
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if w.syncWrites {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	err := w.file.Close()
	w.file = nil
	return err
}
