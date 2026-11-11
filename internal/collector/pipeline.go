package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/iratansh/distributed-telemetry-collector/internal/config"
	"github.com/iratansh/distributed-telemetry-collector/internal/wal"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Exporter interface {
	// Export must return promptly when ctx is canceled so shutdown can finish.
	Export(context.Context, *logs.ExportLogsServiceRequest) (int64, error)
}
type item struct {
	request       *logs.ExportLogsServiceRequest
	records, size int
	sequence      uint64
}

type Pipeline struct {
	logs.UnimplementedLogsServiceServer
	cfg                config.Config
	exporter           Exporter
	wal                *wal.Log
	metrics            *Metrics
	log                *slog.Logger
	queue              chan item
	mu                 sync.Mutex
	admissions         sync.WaitGroup
	admissionsInFlight int
	accepting          bool
	queueClosed        bool
	stopping           bool
	recoveryActive     bool
	pendingBytes       int
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	done               chan struct{}
}

func New(cfg config.Config, exporter Exporter, metrics *Metrics, logger *slog.Logger) *Pipeline {
	return NewWithWAL(cfg, exporter, metrics, logger, nil, nil)
}

func NewWithWAL(cfg config.Config, exporter Exporter, metrics *Metrics, logger *slog.Logger, durable *wal.Log, replay []wal.Record) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{cfg: cfg, exporter: exporter, metrics: metrics, log: logger, wal: durable, queue: make(chan item, cfg.QueueCapacity), accepting: len(replay) == 0, recoveryActive: len(replay) > 0, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	go func() { p.wg.Wait(); close(p.done) }()
	if len(replay) > 0 {
		go func() {
			defer func() {
				p.mu.Lock()
				p.recoveryActive = false
				if !p.stopping {
					p.accepting = true
				}
				p.closeQueueIfDrainedLocked()
				p.mu.Unlock()
			}()
			for _, record := range replay {
				size := proto.Size(record.Request)
				count := logRecordCount(record.Request)
				p.mu.Lock()
				p.pendingBytes += size
				p.metrics.PendingBytes.Add(float64(size))
				p.metrics.QueueDepth.Inc()
				p.mu.Unlock()
				select {
				case p.queue <- item{request: record.Request, records: count, size: size, sequence: record.Sequence}:
				case <-p.ctx.Done():
					return
				}
			}
			p.log.Info("WAL recovery queued", "requests", len(replay))
		}()
	}
	return p
}

func logRecordCount(req *logs.ExportLogsServiceRequest) int {
	n := 0
	for _, resource := range req.ResourceLogs {
		if resource == nil {
			continue
		}
		for _, scope := range resource.ScopeLogs {
			if scope != nil {
				n += len(scope.LogRecords)
			}
		}
	}
	return n
}

func (p *Pipeline) Ready() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.accepting }

func (p *Pipeline) Export(ctx context.Context, req *logs.ExportLogsServiceRequest) (*logs.ExportLogsServiceResponse, error) {
	start := time.Now()
	defer func() { p.metrics.IngestDuration.Observe(time.Since(start).Seconds()) }()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		return nil, p.reject("invalid", codes.InvalidArgument, "request is required")
	}
	count := 0
	for _, resource := range req.ResourceLogs {
		if resource == nil {
			return nil, p.reject("invalid", codes.InvalidArgument, "nil resource logs")
		}
		for _, scope := range resource.ScopeLogs {
			if scope == nil {
				return nil, p.reject("invalid", codes.InvalidArgument, "nil scope logs")
			}
			for _, record := range scope.LogRecords {
				if record == nil {
					return nil, p.reject("invalid", codes.InvalidArgument, "nil log record")
				}
				if len(record.TraceId) != 0 && len(record.TraceId) != 16 {
					return nil, p.reject("invalid", codes.InvalidArgument, "trace_id must be 16 bytes")
				}
				if len(record.SpanId) != 0 && len(record.SpanId) != 8 {
					return nil, p.reject("invalid", codes.InvalidArgument, "span_id must be 8 bytes")
				}
				count++
			}
		}
	}
	if count == 0 {
		return &logs.ExportLogsServiceResponse{}, nil
	}
	if count > p.cfg.MaxRecords {
		return nil, p.reject("records", codes.InvalidArgument, "too many log records in request")
	}
	size := proto.Size(req)
	if size > p.cfg.MaxRequestBytes {
		return nil, p.reject("size", codes.InvalidArgument, "request exceeds configured byte limit")
	}
	p.mu.Lock()
	if !p.accepting {
		p.mu.Unlock()
		return nil, p.reject("shutdown", codes.Unavailable, "collector is draining")
	}
	if size > p.cfg.MaxPendingBytes-p.pendingBytes {
		p.mu.Unlock()
		return nil, p.saturated()
	}
	if len(p.queue)+p.admissionsInFlight >= cap(p.queue) {
		p.mu.Unlock()
		return nil, p.saturated()
	}
	// Reserve byte and queue capacity before releasing the state lock. The
	// reservation keeps concurrent appenders bounded while WAL syncs are shared.
	p.pendingBytes += size
	p.metrics.PendingBytes.Add(float64(size))
	p.metrics.QueueDepth.Inc()
	p.admissionsInFlight++
	p.admissions.Add(1)
	p.mu.Unlock()

	// Persist before making the request visible to workers or acknowledging it.
	var sequence uint64
	if p.wal != nil {
		var err error
		sequence, err = p.wal.Append(req)
		if err != nil {
			p.finishAdmission(size, false)
			p.admissions.Done()
			return nil, p.reject("wal", codes.Unavailable, "durable append failed")
		}
	}
	if err := ctx.Err(); err != nil {
		p.finishAdmission(size, false)
		p.admissions.Done()
		return nil, status.FromContextError(err).Err()
	}
	p.mu.Lock()
	select {
	case p.queue <- item{request: req, records: count, size: size, sequence: sequence}:
		p.metrics.Received.Add(float64(count))
		p.admissionsInFlight--
		p.closeQueueIfDrainedLocked()
		p.mu.Unlock()
		p.admissions.Done()
		return &logs.ExportLogsServiceResponse{}, nil
	default:
		p.releaseReservationLocked(size)
		p.admissionsInFlight--
		p.closeQueueIfDrainedLocked()
		p.mu.Unlock()
		p.admissions.Done()
		return nil, p.saturated()
	}
}

func (p *Pipeline) finishAdmission(size int, published bool) {
	p.mu.Lock()
	if !published {
		p.releaseReservationLocked(size)
	}
	p.admissionsInFlight--
	p.closeQueueIfDrainedLocked()
	p.mu.Unlock()
}

func (p *Pipeline) releaseReservationLocked(size int) {
	p.pendingBytes -= size
	p.metrics.PendingBytes.Sub(float64(size))
	p.metrics.QueueDepth.Dec()
}

func (p *Pipeline) closeQueueIfDrainedLocked() {
	if p.stopping && !p.recoveryActive && p.admissionsInFlight == 0 && !p.queueClosed {
		close(p.queue)
		p.queueClosed = true
	}
}

func (p *Pipeline) reject(reason string, code codes.Code, message string) error {
	p.metrics.Rejected.WithLabelValues(reason).Inc()
	return status.Error(code, message)
}
func (p *Pipeline) saturated() error {
	p.metrics.Rejected.WithLabelValues("capacity").Inc()
	s := status.New(codes.ResourceExhausted, "collector capacity exhausted; retry later")
	withDetails, err := s.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
	if err == nil {
		s = withDetails
	}
	return s.Err()
}

func (p *Pipeline) StopAdmission() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.accepting = false
	p.stopping = true
	p.closeQueueIfDrainedLocked()
}

// Shutdown drains accepted work until the deadline, then cancels exports and
// records every remaining accepted record as dropped.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.StopAdmission()
	// No new admission can start after StopAdmission. Let any in-progress WAL
	// group commit finish before draining workers or closing the queue.
	p.admissions.Wait()
	select {
	case <-p.done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		<-p.done
		for work := range p.queue {
			p.metrics.QueueDepth.Dec()
			p.release(work.size)
			p.metrics.Dropped.WithLabelValues("shutdown").Add(float64(work.records))
		}
		return ctx.Err()
	}
}

func (p *Pipeline) release(size int) {
	p.mu.Lock()
	p.pendingBytes -= size
	p.metrics.PendingBytes.Sub(float64(size))
	p.mu.Unlock()
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	for {
		if p.ctx.Err() != nil {
			return
		}
		var first item
		select {
		case <-p.ctx.Done():
			return
		case work, ok := <-p.queue:
			if !ok {
				return
			}
			first = work
		}
		p.metrics.QueueDepth.Dec()
		batch := &logs.ExportLogsServiceRequest{ResourceLogs: append(first.request.ResourceLogs[:0:0], first.request.ResourceLogs...)}
		records, size := first.records, first.size
		sequences := []uint64{first.sequence}
		p.metrics.InFlight.Add(float64(records))
		timer := time.NewTimer(p.cfg.BatchWait)
		collecting := true
		for n := 1; n < p.cfg.BatchSize && collecting; n++ {
			select {
			case <-p.ctx.Done():
				collecting = false
			case <-timer.C:
				collecting = false
			case work, ok := <-p.queue:
				if !ok {
					collecting = false
					break
				}
				p.metrics.QueueDepth.Dec()
				p.metrics.InFlight.Add(float64(work.records))
				batch.ResourceLogs = append(batch.ResourceLogs, work.request.ResourceLogs...)
				records += work.records
				size += work.size
				sequences = append(sequences, work.sequence)
			}
		}
		timer.Stop()
		start := time.Now()
		rejected, err := p.exporter.Export(p.ctx, batch)
		p.metrics.ExportDuration.Observe(time.Since(start).Seconds())
		if rejected > int64(records) {
			err = errors.New("downstream rejected count exceeds exported record count")
		}
		if err != nil {
			reason := "export"
			if p.ctx.Err() != nil {
				reason = "shutdown"
			}
			p.metrics.Dropped.WithLabelValues(reason).Add(float64(records))
			p.log.Error("batch delivery failed", "records", records, "error", err)
		} else {
			if p.wal != nil {
				if err := p.wal.AckBatch(sequences); err != nil {
					p.log.Error("WAL checkpoint failed; delivery may replay", "sequences", sequences, "error", err)
				}
			}
			p.metrics.Exported.Add(float64(int64(records) - rejected))
			if rejected > 0 {
				p.metrics.Dropped.WithLabelValues("downstream_rejected").Add(float64(rejected))
				p.log.Warn("downstream partially rejected batch", "rejected", rejected)
			}
		}
		p.metrics.InFlight.Sub(float64(records))
		p.release(size)
	}
}
