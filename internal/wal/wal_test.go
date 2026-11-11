package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func testRequest(body string) *logs.ExportLogsServiceRequest {
	return &logs.ExportLogsServiceRequest{ResourceLogs: []*logpb.ResourceLogs{{ScopeLogs: []*logpb.ScopeLogs{{LogRecords: []*logpb.LogRecord{{Body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: body}}}}}}}}}
}

func frameSize(req *logs.ExportLogsServiceRequest) int64 {
	return int64(4 + headerSize + proto.Size(req) + 4)
}

func appendRequests(t *testing.T, dir string, count int) []uint64 {
	t.Helper()
	w, _, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	seqs := make([]uint64, count)
	for i := range count {
		seqs[i], err = w.Append(testRequest(string(rune('a' + i))))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return seqs
}

func TestRecoveryTruncatesIncompleteFinalFrame(t *testing.T) {
	for _, cut := range []struct {
		name            string
		bytesIntoSecond int64
	}{
		{name: "partial length prefix", bytesIntoSecond: 2},
		{name: "partial checksum", bytesIntoSecond: frameSize(testRequest("b")) - 2},
	} {
		t.Run(cut.name, func(t *testing.T) {
			dir := t.TempDir()
			appendRequests(t, dir, 2)
			path := filepath.Join(dir, "collector.wal")
			firstSize := frameSize(testRequest("a"))
			if err := os.Truncate(path, firstSize+cut.bytesIntoSecond); err != nil {
				t.Fatal(err)
			}
			w, replay, err := Open(dir, true)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if len(replay) != 1 || replay[0].Sequence != 0 {
				t.Fatalf("replay=%+v, want first record only", replay)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != firstSize {
				t.Fatalf("WAL size=%d, want truncated size %d", info.Size(), firstSize)
			}
		})
	}
}

func TestRecoveryTruncatesCorruptRecordAndSuffix(t *testing.T) {
	dir := t.TempDir()
	appendRequests(t, dir, 3)
	path := filepath.Join(dir, "collector.wal")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	secondStart := int(frameSize(testRequest("a")))
	secondEnd := secondStart + int(frameSize(testRequest("b")))
	data[secondEnd-1] ^= 0xff // damage the stored CRC of record two
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	w, replay, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(replay) != 1 || replay[0].Sequence != 0 {
		t.Fatalf("replay=%+v, want only the valid prefix", replay)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(secondStart) {
		t.Fatalf("WAL size=%d, want truncation at record two (%d)", info.Size(), secondStart)
	}
}

func TestAcknowledgedRecordsAreSkippedAndSequenceContinues(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.Append(testRequest("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Append(testRequest("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Ack(first); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, replay, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != 1 || replay[0].Sequence != second {
		t.Fatalf("replay=%+v, want sequence %d", replay, second)
	}
	third, err := w.Append(testRequest("third"))
	if err != nil {
		t.Fatal(err)
	}
	if third != second+1 {
		t.Fatalf("next sequence=%d, want %d", third, second+1)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAckBatchUsesOneSyncAndClearsAllPendingRecords(t *testing.T) {
	dir := t.TempDir()
	w, _, err := OpenWithOptions(dir, Options{Sync: true, SyncInterval: 10 * time.Millisecond, MaxSyncBatch: 32})
	if err != nil {
		t.Fatal(err)
	}
	sequences := make([]uint64, 3)
	for i := range sequences {
		sequences[i], err = w.Append(testRequest("batch ack"))
		if err != nil {
			t.Fatal(err)
		}
	}
	before := w.SyncCount()
	if err := w.AckBatch([]uint64{sequences[0], sequences[1], sequences[1], sequences[2], 999}); err != nil {
		t.Fatal(err)
	}
	if got := w.SyncCount() - before; got != 1 {
		t.Fatalf("ack batch performed %d syncs, want 1", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, replay, err := OpenWithOptions(dir, Options{Sync: true, SyncInterval: 10 * time.Millisecond, MaxSyncBatch: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(replay) != 0 {
		t.Fatalf("recovered %d records after batch ack, want none", len(replay))
	}
}

func TestFrameLengthMatchesEncodedRecord(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	req := testRequest("length")
	if _, err := w.Append(req); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "collector.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if int64(binary.BigEndian.Uint32(b[:4]))+4 != frameSize(req) {
		t.Fatal("frame length prefix does not match encoded length")
	}
}

func TestCompactionRetainsOnlyUndeliveredRequests(t *testing.T) {
	previousThreshold := compactThreshold
	compactThreshold = 1
	defer func() { compactThreshold = previousThreshold }()
	dir := t.TempDir()
	w, _, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.Append(testRequest("delivered"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Append(testRequest("pending"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Ack(first); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "collector.wal")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := frameSize(testRequest("pending")); info.Size() != want {
		t.Fatalf("compacted WAL size=%d, want one pending frame (%d)", info.Size(), want)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, replay, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(replay) != 1 || replay[0].Sequence != second {
		t.Fatalf("compaction recovery=%+v, want pending sequence %d", replay, second)
	}
}

func TestConcurrentAppendsShareGroupSync(t *testing.T) {
	const batch = 8
	w, _, err := OpenWithOptions(t.TempDir(), Options{Sync: true, SyncInterval: time.Second, MaxSyncBatch: batch})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	start := make(chan struct{})
	errCh := make(chan error, batch)
	var wg sync.WaitGroup
	for range batch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := w.Append(testRequest("grouped"))
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	w.mu.Lock()
	syncs := w.syncCount
	w.mu.Unlock()
	if syncs != 1 {
		t.Fatalf("filesystem sync count=%d for %d concurrent appends, want 1", syncs, batch)
	}
}
