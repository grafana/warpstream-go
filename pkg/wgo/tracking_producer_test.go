package wgo

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type recordedTrack struct {
	nodeID  int32
	latency time.Duration
	err     error
}

type recordingTracker struct {
	mu    sync.Mutex
	calls []recordedTrack
	noopAgentStatsTracker
}

func (r *recordingTracker) TrackAgentRequest(_ time.Time, nodeID int32, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedTrack{nodeID: nodeID, latency: latency, err: err})
}

// noopAgentStatsTracker satisfies the read methods of AgentStatsTracker for
// tests that only care about TrackAgentRequest writes.
type noopAgentStatsTracker struct{}

func (noopAgentStatsTracker) AgentStats(time.Time, int32) (AgentStats, bool) {
	return AgentStats{}, false
}
func (noopAgentStatsTracker) ClusterStats(time.Time, float64, float64) (ClusterStats, bool) {
	return ClusterStats{}, false
}
func (noopAgentStatsTracker) PurgeAgents([]int32) {}

func TestTrackingProducer_Produce(t *testing.T) {
	t.Run("records latency and nil error on success", func(t *testing.T) {
		inner := newMockDirectProducer()
		inner.delays[7] = 5 * time.Millisecond
		tr := &recordingTracker{}
		tp := NewTrackingProducer(inner, tr)

		res := tp.ProduceSync(context.Background(), Agent{NodeID: 7}, nil)
		require.NoError(t, res.err)

		require.Len(t, tr.calls, 1)
		assert.Equal(t, int32(7), tr.calls[0].nodeID)
		assert.NoError(t, tr.calls[0].err)
		assert.GreaterOrEqual(t, tr.calls[0].latency, 5*time.Millisecond)
	})

	t.Run("records error from inner", func(t *testing.T) {
		inner := newMockDirectProducer()
		boom := errors.New("boom")
		inner.errs[3] = boom
		tr := &recordingTracker{}
		tp := NewTrackingProducer(inner, tr)

		res := tp.ProduceSync(context.Background(), Agent{NodeID: 3}, nil)
		require.ErrorIs(t, res.err, boom)

		require.Len(t, tr.calls, 1)
		assert.Equal(t, int32(3), tr.calls[0].nodeID)
		assert.ErrorIs(t, tr.calls[0].err, boom)
	})

	t.Run("records per-partition error code as agent error", func(t *testing.T) {
		inner := newMockDirectProducer()
		resp := kmsg.NewPtrProduceResponse()
		resp.Topics = []kmsg.ProduceResponseTopic{{
			Topic: "t",
			Partitions: []kmsg.ProduceResponseTopicPartition{
				{Partition: 0, ErrorCode: kerr.NotLeaderForPartition.Code},
			},
		}}
		inner.respFn = func(int32, []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) { return resp, nil }
		tr := &recordingTracker{}
		tp := NewTrackingProducer(inner, tr)

		res := tp.ProduceSync(context.Background(), Agent{NodeID: 5}, nil)
		require.NoError(t, res.err)

		require.Len(t, tr.calls, 1)
		assert.ErrorIs(t, tr.calls[0].err, kerr.NotLeaderForPartition)
	})

	t.Run("forwards inner response unchanged", func(t *testing.T) {
		inner := newMockDirectProducer()
		want := kmsg.NewPtrProduceResponse()
		inner.respFn = func(int32, []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) { return want, nil }
		tp := NewTrackingProducer(inner, &recordingTracker{})

		res := tp.ProduceSync(context.Background(), Agent{NodeID: 1}, nil)
		require.NoError(t, res.err)
		assert.Same(t, want, res.resp)
	})
}

func TestTrackingProducer_CanceledDrainHealth(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		cancel bool
		want   error
	}{
		{"canceled wire timeout", &wireError{context.DeadlineExceeded}, true, context.DeadlineExceeded},
		{"live wire timeout", &wireError{context.DeadlineExceeded}, false, context.DeadlineExceeded},
		{"other wire failure", &wireError{kgo.ErrRecordTimeout}, true, kgo.ErrRecordTimeout},
		{"canceled transport", &wireError{io.EOF}, true, context.Canceled},
		{"live transport", &wireError{io.EOF}, false, io.EOF},
		{"canceled broker error", kerr.NotLeaderForPartition, true, kerr.NotLeaderForPartition},
		{"canceled success", nil, true, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := newMockDirectProducer()
			inner.errs[3] = test.err
			tracker := &recordingTracker{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.cancel {
				cancel()
			}
			result := NewTrackingProducer(inner, tracker).ProduceSync(ctx, Agent{NodeID: 3}, nil)
			require.ErrorIs(t, result.error(), test.err)
			require.Len(t, tracker.calls, 1)
			require.ErrorIs(t, tracker.calls[0].err, test.want)
		})
	}
}

func TestTrackingProducer_CanceledSiblingFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer, _, parts, started, release := gatedKafkaDirectProducer(t)
		tracker := &recordingTracker{}
		tracking := NewTrackingProducer(producer, tracker)
		firstCtx, cancelFirst := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancelFirst()
		secondCtx, cancelSecond := context.WithCancel(t.Context())
		defer cancelSecond()
		first, second := make(chan ProduceResult, 1), make(chan ProduceResult, 1)
		go func() { first <- tracking.ProduceSync(firstCtx, Agent{NodeID: 0}, parts) }()
		<-started
		go func() { second <- tracking.ProduceSync(secondCtx, Agent{NodeID: 0}, parts) }()
		synctest.Wait()
		cancelFirst()
		cancelSecond()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		require.ErrorIs(t, (<-first).error(), context.DeadlineExceeded)
		select {
		case result := <-second:
			require.Error(t, result.error())
			require.NotErrorIs(t, result.error(), context.DeadlineExceeded)
		default:
			t.Fatal("sibling did not observe the connection failure")
		}
		require.Len(t, tracker.calls, 2)
		var canceled, deadlines int
		for _, call := range tracker.calls {
			if errors.Is(call.err, context.Canceled) {
				canceled++
			}
			if errors.Is(call.err, context.DeadlineExceeded) {
				deadlines++
			}
		}
		require.Equal(t, 1, canceled)
		require.Equal(t, 1, deadlines)
		release()
	})
}
