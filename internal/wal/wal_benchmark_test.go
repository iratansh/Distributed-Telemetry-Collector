package wal

import (
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func BenchmarkAppendAndAckGroupedSync(b *testing.B) {
	w, _, err := OpenWithOptions(b.TempDir(), Options{Sync: true, SyncInterval: 2 * time.Millisecond, MaxSyncBatch: 32})
	if err != nil {
		b.Fatal(err)
	}
	request := testRequest("benchmark WAL record")
	b.ReportAllocs()
	b.SetBytes(int64(proto.Size(request)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sequence, err := w.Append(request)
			if err != nil {
				b.Error(err)
				return
			}
			if err := w.Ack(sequence); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	if err := w.Close(); err != nil {
		b.Errorf("WAL close: %v", err)
	}
}

func BenchmarkAppendBatchAndAckBatch(b *testing.B) {
	benchmarkAppendBatchAndAcks(b, true)
}

func BenchmarkAppendBatchAndAckEach(b *testing.B) {
	benchmarkAppendBatchAndAcks(b, false)
}

func benchmarkAppendBatchAndAcks(b *testing.B, batchAck bool) {
	w, _, err := OpenWithOptions(b.TempDir(), Options{Sync: true, SyncInterval: 2 * time.Millisecond, MaxSyncBatch: 32})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			b.Errorf("WAL close: %v", err)
		}
	}()
	const batchSize = 8
	request := testRequest("benchmark batch WAL record")
	b.ReportAllocs()
	b.SetBytes(int64(proto.Size(request) * batchSize))
	b.ResetTimer()
	for range b.N {
		sequences := make([]uint64, batchSize)
		errs := make(chan error, batchSize)
		var wg sync.WaitGroup
		for i := range batchSize {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				sequence, err := w.Append(request)
				sequences[i] = sequence
				errs <- err
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
		if batchAck {
			if err := w.AckBatch(sequences); err != nil {
				b.Fatal(err)
			}
		} else {
			for _, sequence := range sequences {
				if err := w.Ack(sequence); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	b.StopTimer()
}
