package wgo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// makeTopicPartitionRecords builds the encoded partition input the accumulator
// takes, encoding the payloads into a RecordBatch (empty payloads leave the
// batch unencoded — the accumulator only keys by topic/partition).
func makeTopicPartitionRecords(topic string, partition int32, payloads ...string) encodedTopicPartitionRecords {
	if len(payloads) == 0 {
		return encodedTopicPartitionRecords{topic: topic, partition: partition}
	}
	records := make([]*kgo.Record, len(payloads))
	for i, payload := range payloads {
		records[i] = &kgo.Record{Topic: topic, Partition: partition, Value: []byte(payload)}
	}
	return newEncodedTopicPartitionRecords(topic, partition, records)
}

func partitionEntries(resp *kmsg.ProduceResponse, topic string) map[int32]kmsg.ProduceResponseTopicPartition {
	out := map[int32]kmsg.ProduceResponseTopicPartition{}
	for _, t := range resp.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			out[p.Partition] = p
		}
	}
	return out
}

type testProduceResultAccumulator struct {
	*produceResultAccumulator
}

// mustNewProduceResultAccumulator builds an accumulator and fails the
// test if the input has duplicate (topic, partition) pairs.
func mustNewProduceResultAccumulator(t *testing.T, partitions []encodedTopicPartitionRecords) *testProduceResultAccumulator {
	t.Helper()
	a, err := newProduceResultAccumulator(partitions)
	require.NoError(t, err)
	return &testProduceResultAccumulator{produceResultAccumulator: a}
}

func scopeAllResponsePartitions(res ProduceResult) scopedProduceResult {
	own := make(map[topicPartition]struct{})
	if res.resp != nil {
		for _, t := range res.resp.Topics {
			for _, p := range t.Partitions {
				own[topicPartition{topic: t.Topic, partition: p.Partition}] = struct{}{}
			}
		}
	}
	return scopeProduceResultToPartitions(res, own)
}

func (a *testProduceResultAccumulator) accumulate(res ProduceResult) {
	a.produceResultAccumulator.accumulate(scopeAllResponsePartitions(res))
}

func TestProduceResult_Error(t *testing.T) {
	tests := map[string]struct {
		res  ProduceResult
		want error
	}{
		"fully successful resp returns nil": {
			res: ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
				makeProduceResponseTopicPartition(0, kerrNoError),
			))},
			want: nil,
		},
		"transport err wins over resp": {
			res: ProduceResult{
				err: kerr.LeaderNotAvailable,
				resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
					makeProduceResponseTopicPartition(0, kerrNoError),
				)),
			},
			want: kerr.LeaderNotAvailable,
		},
		"per-partition err: first non-zero ErrorCode": {
			res: ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
				makeProduceResponseTopicPartition(0, kerrNoError),
				makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
			))},
			want: kerr.NotLeaderForPartition,
		},
		"empty ProduceResult (both nil) returns the empty sentinel": {
			res:  ProduceResult{},
			want: errEmptyProduceResult,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.res.error()
			if tc.want == nil {
				assert.NoError(t, got)
			} else {
				require.Error(t, got)
				assert.ErrorIs(t, got, tc.want)
			}
		})
	}
}

func TestScopedProduceResult_Error(t *testing.T) {
	tp0 := topicPartition{topic: "t", partition: 0}
	tp1 := topicPartition{topic: "t", partition: 1}
	own := map[topicPartition]struct{}{tp0: {}}

	t.Run("ignores another caller's error merged into the same flush", func(t *testing.T) {
		res := ProduceResult{resp: makeProduceResponse(11, 0,
			makeProduceResponseTopic("t",
				makeProduceResponseTopicPartition(0, kerrNoError),
				makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
			),
		)}

		scoped := scopeProduceResultToPartitions(res, own)

		assert.NoError(t, scoped.error())
		assert.False(t, scoped.allOwned)
	})

	t.Run("keeps two of own's own partitions all-or-nothing with each other", func(t *testing.T) {
		res := ProduceResult{resp: makeProduceResponse(11, 0,
			makeProduceResponseTopic("t",
				makeProduceResponseTopicPartition(0, kerrNoError),
				makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
			),
		)}

		scoped := scopeProduceResultToPartitions(res, map[topicPartition]struct{}{tp0: {}, tp1: {}})

		assert.Error(t, scoped.error())
		assert.True(t, scoped.allOwned)
	})

	t.Run("none of own present stays a no-op, not an empty result", func(t *testing.T) {
		res := ProduceResult{resp: makeProduceResponse(11, 0,
			makeProduceResponseTopic("t", makeProduceResponseTopicPartition(1, kerrNoError)),
		)}

		scoped := scopeProduceResultToPartitions(res, own)

		assert.NoError(t, scoped.error())
		assert.NotErrorIs(t, scoped.error(), errEmptyProduceResult)
		assert.False(t, scoped.allOwned)
	})

	t.Run("non-nil empty response stays successful", func(t *testing.T) {
		scoped := scopeProduceResultToPartitions(ProduceResult{resp: &kmsg.ProduceResponse{}}, own)

		assert.NoError(t, scoped.error())
		assert.True(t, scoped.allOwned)
	})

	t.Run("a whole-flush transport error is kept for every partition", func(t *testing.T) {
		res := ProduceResult{err: kerr.KafkaStorageError}

		scoped := scopeProduceResultToPartitions(res, own)

		assert.ErrorIs(t, scoped.error(), kerr.KafkaStorageError)
	})
}

func TestGetProduceResultErr(t *testing.T) {
	cases := map[string]struct {
		err  error
		want produceResultErr
	}{
		"context canceled": {
			err:  context.Canceled,
			want: produceResultErr{reason: "canceled", retriable: false, code: kerr.RequestTimedOut.Code},
		},
		"context deadline exceeded": {
			err:  context.DeadlineExceeded,
			want: produceResultErr{reason: "timeout", retriable: true, code: kerr.RequestTimedOut.Code},
		},
		"wrapped context canceled": {
			err:  fmt.Errorf("upstream: %w", context.Canceled),
			want: produceResultErr{reason: "canceled", retriable: false, code: kerr.RequestTimedOut.Code},
		},
		"wrapped context deadline exceeded": {
			err:  fmt.Errorf("attempt expired: %w", context.DeadlineExceeded),
			want: produceResultErr{reason: "timeout", retriable: true, code: kerr.RequestTimedOut.Code},
		},
		"retriable kerr error (LeaderNotAvailable)": {
			err:  kerr.LeaderNotAvailable,
			want: produceResultErr{reason: "kafka_retriable_error", retriable: true, code: kerr.LeaderNotAvailable.Code},
		},
		"retriable kerr error (RequestTimedOut)": {
			err:  kerr.RequestTimedOut,
			want: produceResultErr{reason: "kafka_retriable_error", retriable: true, code: kerr.RequestTimedOut.Code},
		},
		"wrapped retriable kerr error": {
			err:  fmt.Errorf("from broker: %w", kerr.NotLeaderForPartition),
			want: produceResultErr{reason: "kafka_retriable_error", retriable: true, code: kerr.NotLeaderForPartition.Code},
		},
		"transport error (io.EOF)": {
			err:  io.EOF,
			want: produceResultErr{reason: "transport", retriable: true, code: kerr.UnknownServerError.Code},
		},
		"non-retriable kerr error (MessageTooLarge)": {
			err:  kerr.MessageTooLarge,
			want: produceResultErr{reason: "kafka_non_retriable_error", retriable: false, code: kerr.MessageTooLarge.Code},
		},
		"non-retriable kerr error (TopicAuthorizationFailed)": {
			err:  kerr.TopicAuthorizationFailed,
			want: produceResultErr{reason: "kafka_non_retriable_error", retriable: false, code: kerr.TopicAuthorizationFailed.Code},
		},
		"wrapped non-retriable kerr error": {
			err:  fmt.Errorf("rejected: %w", kerr.MessageTooLarge),
			want: produceResultErr{reason: "kafka_non_retriable_error", retriable: false, code: kerr.MessageTooLarge.Code},
		},
		"unknown error": {
			err:  errors.New("something we don't recognise"),
			want: produceResultErr{reason: "unknown", retriable: false, code: kerr.UnknownServerError.Code},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// getProduceResultErr stores the input err verbatim; fill in the
			// expectation here rather than repeating it in every case.
			tc.want.err = tc.err
			got := getProduceResultErr(tc.err)
			assert.Equal(t, tc.want, got)
			assert.NotZero(t, got.code)
		})
	}
}

func TestNewProduceResultAccumulator(t *testing.T) {
	t.Run("rejects duplicate topic-partition pairs", func(t *testing.T) {
		_, err := newProduceResultAccumulator([]encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
			makeTopicPartitionRecords("t", 0),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `topic="t"`)
		assert.Contains(t, err.Error(), "partition=0")
	})

	t.Run("same partition in different topics is not a duplicate", func(t *testing.T) {
		_, err := newProduceResultAccumulator([]encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("u", 0),
		})
		require.NoError(t, err)
	})

	t.Run("initial state: nothing resolved, all pending", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		assert.False(t, a.done())
		assert.NotNil(t, a.result().err)
		assert.Len(t, a.remaining(), 2)
	})

	t.Run("empty input: nothing pending, done immediately", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, nil)
		assert.True(t, a.done())
		r := a.result()
		assert.NoError(t, r.err)
		assert.Empty(t, r.resp.Topics)
	})

	t.Run("remaining preserves a pending partition's pre-encoded batch", func(t *testing.T) {
		enc := encodedTopicPartitionRecords{
			topic:        "t",
			partition:    0,
			encoded:      []byte("ENCODED-BATCH"),
			encodedStats: produceRequestStats{records: 2, batches: 1},
		}
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{enc})

		rem := a.remaining()
		require.Len(t, rem, 1)
		assert.Equal(t, []byte("ENCODED-BATCH"), rem[0].encoded)
		assert.Equal(t, produceRequestStats{records: 2, batches: 1}, rem[0].encodedStats)
	})
}

func TestProduceResultAccumulator_Accumulate(t *testing.T) {
	t.Run("full success: every partition resolved", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(ProduceResult{resp: makeProduceResponse(9, 50, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 200},
		))})

		assert.True(t, a.done())
		assert.Nil(t, a.result().err)
		assert.Empty(t, a.remaining())

		resp := a.result().resp
		assert.Equal(t, int16(9), resp.Version)
		assert.Equal(t, int32(50), resp.ThrottleMillis)
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, int64(100), entries[0].BaseOffset)
		assert.Equal(t, kerrNoError, entries[1].ErrorCode)
		assert.Equal(t, int64(200), entries[1].BaseOffset)
	})

	t.Run("transport retriable err: not aborted, pending unchanged", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(ProduceResult{err: kerr.LeaderNotAvailable})

		assert.False(t, a.done())
		assert.ErrorIs(t, a.result().err, kerr.LeaderNotAvailable)
		assert.Len(t, a.remaining(), 1)
	})

	t.Run("transport non-retriable err: aborted", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(ProduceResult{err: context.Canceled})

		assert.True(t, a.done())
		assert.ErrorIs(t, a.result().err, context.Canceled)

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		// context.Canceled isn't a kerr.Error, so we fall back to
		// REQUEST_TIMED_OUT for the synthesized entry.
		assert.Equal(t, kerr.RequestTimedOut.Code, entries[0].ErrorCode)
	})

	t.Run("per-partition mixed retriable: whole leg treated as failed, partial successes dropped", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
		))})

		assert.False(t, a.done())
		assert.ErrorIs(t, a.result().err, kerr.NotLeaderForPartition)
		assert.Len(t, a.remaining(), 2)
	})

	t.Run("per-partition mixed non-retriable: aborted and pending synthesized with the aborted kerr code", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			makeProduceResponseTopicPartition(1, kerr.MessageTooLarge.Code),
		))})

		assert.True(t, a.done())
		assert.ErrorIs(t, a.result().err, kerr.MessageTooLarge)

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerr.MessageTooLarge.Code, entries[0].ErrorCode)
		assert.Equal(t, kerr.MessageTooLarge.Code, entries[1].ErrorCode)
	})

	t.Run("partial response coverage: only the included partitions resolve", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
		))})

		assert.False(t, a.done())
		// One partition resolved but the other is still pending: result
		// surfaces bare ErrRecordTimeout (no failure was observed).
		assert.ErrorIs(t, a.result().err, kgo.ErrRecordTimeout)
		rem := a.remaining()
		require.Len(t, rem, 1)
		assert.Equal(t, int32(1), rem[0].partition)
	})

	t.Run("attempt ownership ignores another agent's still-pending partition", func(t *testing.T) {
		tp0 := topicPartition{topic: "t", partition: 0}
		tp1 := topicPartition{topic: "t", partition: 1}
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		res := ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 200},
		))}

		a.produceResultAccumulator.accumulate(scopeProduceResultToPartitions(res, map[topicPartition]struct{}{tp1: {}}))

		assert.False(t, a.done())
		assert.Contains(t, a.pending, tp0)
		assert.NotContains(t, a.pending, tp1)
		assert.NotContains(t, a.resolved, tp0)
		assert.Contains(t, a.resolved, tp1)
	})

	t.Run("attempt ownership ignores another agent's error", func(t *testing.T) {
		tp0 := topicPartition{topic: "t", partition: 0}
		tp1 := topicPartition{topic: "t", partition: 1}
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		res := ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			makeProduceResponseTopicPartition(0, kerr.NotLeaderForPartition.Code),
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 200},
		))}

		a.produceResultAccumulator.accumulate(scopeProduceResultToPartitions(res, map[topicPartition]struct{}{tp1: {}}))

		assert.False(t, a.done())
		assert.Contains(t, a.pending, tp0)
		assert.NotContains(t, a.pending, tp1)
		assert.Empty(t, a.failed)
		assert.Nil(t, a.lastErr.err)
	})

	t.Run("multiple successful calls together resolve everything and capture metadata", func(t *testing.T) {
		tid := [16]byte{1, 2, 3}
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		first := makeProduceResponse(9, 100, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
		))
		first.Topics[0].TopicID = tid

		second := makeProduceResponse(11, 50, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 200},
		))

		a.accumulate(ProduceResult{resp: first})
		assert.False(t, a.done())
		// Intermediate state: one partition resolved but one still
		// pending and no error observed → bare ErrRecordTimeout.
		assert.ErrorIs(t, a.result().err, kgo.ErrRecordTimeout)

		a.accumulate(ProduceResult{resp: second})
		assert.True(t, a.done())
		assert.Nil(t, a.result().err)

		resp := a.result().resp
		assert.Equal(t, int16(9), resp.Version, "version is from the first response")
		assert.Equal(t, int32(100), resp.ThrottleMillis, "throttle is the max across responses")
		require.Len(t, resp.Topics, 1)
		assert.Equal(t, tid, resp.Topics[0].TopicID)
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, int64(100), entries[0].BaseOffset)
		assert.Equal(t, int64(200), entries[1].BaseOffset)
	})

	t.Run("overwrites already-resolved entry on later accumulate", func(t *testing.T) {
		// Real callers don't re-submit a resolved partition (claimNextWave
		// skips it), so this case shouldn't fire in practice. If it ever
		// does — e.g. a buggy agent echoing a partition we didn't ask
		// about, or a future caller mistake — the last accumulate wins.
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 1},
		))})
		require.True(t, a.done())

		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 999},
		))})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		assert.Equal(t, int64(999), entries[0].BaseOffset, "last accumulate wins for an already-resolved partition")
	})
}

func TestProduceResultAccumulator_Response(t *testing.T) {
	t.Run("exhausted retriable: pending entries inherit the leg's kerr code", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("u", 7),
		})

		a.accumulate(ProduceResult{err: kerr.LeaderNotAvailable})

		resp := a.result().resp
		require.Len(t, resp.Topics, 2)
		for _, topic := range resp.Topics {
			require.Len(t, topic.Partitions, 1)
			assert.Equal(t, kerr.LeaderNotAvailable.Code, topic.Partitions[0].ErrorCode)
		}
	})

	t.Run("aborted on non-kerr err: pending entries fall back to UNKNOWN_SERVER_ERROR", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(ProduceResult{err: errors.New("some non-retriable transport boom")})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		assert.Equal(t, kerr.UnknownServerError.Code, entries[0].ErrorCode)
	})

	t.Run("pending partition with recorded per-partition failure surfaces that entry", func(t *testing.T) {
		// One mixed-failure leg; the partition that failed retriably
		// has its actual entry recorded so response() can surface it
		// instead of a synthesized REQUEST_TIMED_OUT.
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
		))})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		// Partition 0 had no recorded failure entry (its success was
		// dropped under all-or-nothing), so it inherits the leg's
		// kerr code from lastErr.
		assert.Equal(t, kerr.NotLeaderForPartition.Code, entries[0].ErrorCode)
		// Partition 1 surfaces the actual error reported by the agent.
		assert.Equal(t, kerr.NotLeaderForPartition.Code, entries[1].ErrorCode)
	})

	t.Run("failed entry is cleared once the partition later resolves", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		// First leg: partition fails retriably (recorded in failed map).
		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			makeProduceResponseTopicPartition(0, kerr.NotLeaderForPartition.Code),
		))})
		// Second leg: partition succeeds — should drop the failed entry.
		a.accumulate(ProduceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, int64(42), entries[0].BaseOffset)
	})

	t.Run("partial resolved plus pending: resolved entry preserved, pending synthesized", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(ProduceResult{resp: makeProduceResponse(9, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})
		a.accumulate(ProduceResult{err: kerr.LeaderNotAvailable})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, int64(42), entries[0].BaseOffset)
		// The unresolved partition inherits the most recent leg's kerr
		// code so perPartitionDone can surface the specific failure.
		assert.Equal(t, kerr.LeaderNotAvailable.Code, entries[1].ErrorCode)
	})
}

func TestProduceResultAccumulator_Result(t *testing.T) {
	t.Run("all partitions resolved: no err, response carries every entry", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		a.accumulate(ProduceResult{resp: makeProduceResponse(9, 100, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 43},
		))})

		r := a.result()
		require.NoError(t, r.err)
		require.NotNil(t, r.resp)
		entries := partitionEntries(r.resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, int64(42), entries[0].BaseOffset)
		assert.Equal(t, int64(43), entries[1].BaseOffset)
	})

	t.Run("partially resolved, no err observed: bare ErrRecordTimeout, pending entries synthesized in response", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		a.accumulate(ProduceResult{resp: makeProduceResponse(9, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		entries := partitionEntries(r.resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, kerr.RequestTimedOut.Code, entries[1].ErrorCode)
	})

	t.Run("partially resolved with failed leg: lastErr wins, wrapped in ErrRecordTimeout", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		// First leg resolves partition 0; second leg's transport error
		// leaves partition 1 pending and is recorded as lastErr.
		a.accumulate(ProduceResult{resp: makeProduceResponse(9, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})
		a.accumulate(ProduceResult{err: kerr.LeaderNotAvailable})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, r.err, kerr.LeaderNotAvailable)
	})

	t.Run("nothing resolved, no err observed: bare kgo.ErrRecordTimeout", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.NotErrorIs(t, r.err, kerr.LeaderNotAvailable)
	})

	t.Run("nothing resolved, retriable err observed: kgo.ErrRecordTimeout wraps the kerr", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})
		a.accumulate(ProduceResult{err: kerr.LeaderNotAvailable})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, r.err, kerr.LeaderNotAvailable)
	})

	t.Run("nothing resolved, non-retriable err observed: kgo.ErrRecordTimeout wraps the kerr", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []encodedTopicPartitionRecords{makeTopicPartitionRecords("t", 0)})
		a.accumulate(ProduceResult{err: kerr.MessageTooLarge})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, r.err, kerr.MessageTooLarge)
	})

	t.Run("empty accumulator: no err (vacuously resolved), empty response", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, nil)

		r := a.result()
		assert.NoError(t, r.err)
		require.NotNil(t, r.resp)
		assert.Empty(t, r.resp.Topics)
	})
}

func TestSelectProduceResult(t *testing.T) {
	successResp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
		Topic:      "t",
		Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: 0, BaseOffset: 42}},
	}}}
	primaryCodesResp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
		Topic:      "t",
		Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: kerr.NotEnoughReplicas.Code}},
	}}}
	fallbackCodesResp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
		Topic:      "t",
		Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: kerr.NotLeaderForPartition.Code}},
	}}}
	primaryErr := kerr.LeaderNotAvailable
	fallbackErr := fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, kerr.NotLeaderForPartition)

	tests := map[string]struct {
		primary  ProduceResult
		fallback ProduceResult
		want     ProduceResult
	}{
		"primary succeeded: return primary, ignore fallback": {
			primary:  ProduceResult{resp: successResp},
			fallback: ProduceResult{resp: nil, err: fallbackErr},
			want:     ProduceResult{resp: successResp},
		},
		"primary errored, fallback succeeded: return fallback": {
			primary:  ProduceResult{err: primaryErr},
			fallback: ProduceResult{resp: successResp},
			want:     ProduceResult{resp: successResp},
		},
		"primary errored, fallback errored: fallback.resp (merged view) + chained primary.err: fallback.err": {
			primary:  ProduceResult{err: primaryErr},
			fallback: ProduceResult{resp: fallbackCodesResp, err: fallbackErr},
			want: ProduceResult{
				resp: fallbackCodesResp,
				err:  fmt.Errorf("%w: %w", primaryErr, fallbackErr),
			},
		},
		"primary had per-partition codes only (no transport err), fallback errored: return fallback (merged view)": {
			primary:  ProduceResult{resp: primaryCodesResp},
			fallback: ProduceResult{resp: fallbackCodesResp, err: fallbackErr},
			want:     ProduceResult{resp: fallbackCodesResp, err: fallbackErr},
		},
		"primary transport-errored, fallback had per-partition codes only (partial wins): return fallback": {
			primary:  ProduceResult{err: primaryErr},
			fallback: ProduceResult{resp: fallbackCodesResp},
			want:     ProduceResult{resp: fallbackCodesResp},
		},
		"both had per-partition codes only (no transport errs): return fallback (its view is the latest)": {
			primary:  ProduceResult{resp: primaryCodesResp},
			fallback: ProduceResult{resp: fallbackCodesResp},
			want:     ProduceResult{resp: fallbackCodesResp},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := selectProduceResult(tc.primary, tc.fallback)
			assert.Same(t, tc.want.resp, got.resp)
			if tc.want.err == nil {
				assert.NoError(t, got.err)
			} else {
				require.Error(t, got.err)
				assert.Equal(t, tc.want.err.Error(), got.err.Error())
			}
		})
	}
}
