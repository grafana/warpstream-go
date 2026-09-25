package wgo

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// secondaryOf is a test-only helper: returns the second candidate's
// NodeID (the "secondary" in the historical sense) for the given
// strategy, plus a bool indicating whether one is available. Lets the
// existing test scenarios continue to assert "secondary selection"
// invariants now that the strategy interface only exposes Candidates.
func secondaryOf(s PartitionAssignmentStrategy, topic string, partition int32) (int32, bool) {
	c := s.Candidates(topic, partition, 2)
	if len(c) < 2 {
		return 0, false
	}
	return c[1].NodeID, true
}

func TestDefaultPartitionAssignmentStrategy_SecondaryAvailability(t *testing.T) {
	tests := map[string]struct {
		topic     string
		partition int32
		primary   int32
		all       []int32
		wantOk    bool
	}{
		"single agent: no secondary available": {
			topic: "t", partition: 0, primary: 1, all: []int32{1},
			wantOk: false,
		},
		"two agents: returns the non-primary": {
			topic: "t", partition: 0, primary: 1, all: []int32{1, 2},
			wantOk: true,
		},
		"two agents: primary=2 returns the non-primary": {
			topic: "t", partition: 0, primary: 2, all: []int32{1, 2},
			wantOk: true,
		},
		"three agents: result is deterministic": {
			topic: "t", partition: 0, primary: 1, all: []int32{1, 2, 3},
			wantOk: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := newDefaultPartitionAssignmentStrategy(tc.all, map[topicPartition]int32{
				{topic: tc.topic, partition: tc.partition}: tc.primary,
			})
			nodeID, ok := secondaryOf(s, tc.topic, tc.partition)
			assert.Equal(t, tc.wantOk, ok)
			if ok {
				assert.NotEqual(t, tc.primary, nodeID)
				nodeID2, _ := secondaryOf(s, tc.topic, tc.partition)
				assert.Equal(t, nodeID, nodeID2)
			}
		})
	}
}

func TestDefaultPartitionAssignmentStrategy_SecondaryNeverReturnsPrimary(t *testing.T) {
	agents := []int32{1, 2, 3, 4, 5}
	for _, primary := range agents {
		for part := int32(0); part < 50; part++ {
			t.Run(fmt.Sprintf("primary=%d partition=%d", primary, part), func(t *testing.T) {
				s := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
					{topic: "t", partition: part}: primary,
				})
				nodeID, ok := secondaryOf(s, "t", part)
				require.True(t, ok)
				assert.NotEqual(t, primary, nodeID)
			})
		}
	}
}

func TestDefaultPartitionAssignmentStrategy_NodeIDZeroIsValid(t *testing.T) {
	t.Run("NodeID 0 can be the secondary", func(t *testing.T) {
		s := newDefaultPartitionAssignmentStrategy([]int32{0, 1}, map[topicPartition]int32{
			{topic: "t", partition: 0}: 1,
		})
		nodeID, ok := secondaryOf(s, "t", 0)
		require.True(t, ok)
		assert.Equal(t, int32(0), nodeID)
	})

	t.Run("NodeID 0 can be the primary without aliasing no-secondary sentinel", func(t *testing.T) {
		s := newDefaultPartitionAssignmentStrategy([]int32{0, 1}, map[topicPartition]int32{
			{topic: "t", partition: 0}: 0,
		})
		nodeID, ok := secondaryOf(s, "t", 0)
		require.True(t, ok)
		assert.Equal(t, int32(1), nodeID)
	})
}

func TestDefaultPartitionAssignmentStrategy_Candidates(t *testing.T) {
	agents := []int32{1, 2, 3, 4, 5}
	leaders := map[topicPartition]int32{
		{topic: "t", partition: 0}: 1,
	}
	s := newDefaultPartitionAssignmentStrategy(agents, leaders)

	t.Run("returns up to maxCandidates entries", func(t *testing.T) {
		c := s.Candidates("t", 0, 3)
		assert.Len(t, c, 3)
		assert.Equal(t, int32(1), c[0].NodeID, "primary at [0]")
	})

	t.Run("falls back to a live agent for an unknown partition", func(t *testing.T) {
		c := s.Candidates("t", 99, 3)
		require.Len(t, c, 3)
		for _, cand := range c {
			assert.Contains(t, agents, cand.NodeID)
		}
	})

	t.Run("returns nil for unknown partition when no agents are live", func(t *testing.T) {
		empty := newDefaultPartitionAssignmentStrategy(nil, leaders)
		assert.Nil(t, empty.Candidates("t", 99, 3))
	})

	t.Run("returns nil for maxCandidates<=0", func(t *testing.T) {
		assert.Nil(t, s.Candidates("t", 0, 0))
	})

	t.Run("never repeats an agent in the result", func(t *testing.T) {
		c := s.Candidates("t", 0, len(agents))
		seen := make(map[int32]struct{})
		for _, cand := range c {
			_, dup := seen[cand.NodeID]
			require.False(t, dup, "duplicate NodeID %d in candidates", cand.NodeID)
			seen[cand.NodeID] = struct{}{}
		}
	})

	t.Run("all candidates are healthy in the default strategy", func(t *testing.T) {
		c := s.Candidates("t", 0, len(agents))
		for _, cand := range c {
			assert.Equal(t, AgentStateHealthy, cand.State)
		}
	})

	t.Run("ordering is deterministic across calls", func(t *testing.T) {
		c1 := s.Candidates("t", 0, len(agents))
		c2 := s.Candidates("t", 0, len(agents))
		assert.Equal(t, c1, c2)
	})

	t.Run("single-agent cluster has no secondary candidate", func(t *testing.T) {
		s1 := newDefaultPartitionAssignmentStrategy([]int32{1}, map[topicPartition]int32{
			{topic: "t", partition: 0}: 1,
		})
		c := s1.Candidates("t", 0, 5)
		assert.Len(t, c, 1)
		assert.Equal(t, int32(1), c[0].NodeID)
	})
}

func TestDefaultPartitionAssignmentStrategy_FallbackOnUnknownLeader(t *testing.T) {
	agents := []int32{1, 2, 3, 4, 5}
	// This one entry marks topic "t" as known, so every other partition
	// looked up below takes the fallback path, not "topic unknown".
	s := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
		{topic: "t", partition: -1}: agents[0],
	})

	t.Run("fallback pick is deterministic across calls and instances", func(t *testing.T) {
		c1 := s.Candidates("t", 7, 1)
		c2 := s.Candidates("t", 7, 1)
		require.Len(t, c1, 1)
		assert.Equal(t, c1, c2)

		s2 := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
			{topic: "t", partition: -1}: agents[0],
		})
		c3 := s2.Candidates("t", 7, 1)
		assert.Equal(t, c1, c3, "two independently constructed strategies over the same agent set must agree")
	})

	t.Run("fallback pick is exactly agents[hash(topic,partition) % len(agents)]", func(t *testing.T) {
		for p := int32(0); p < 20; p++ {
			c := s.Candidates("t", p, 1)
			require.Len(t, c, 1)
			want := agents[hashTopicPartition("t", p)%uint64(len(agents))]
			assert.Equal(t, want, c[0].NodeID, "partition %d", p)
		}
	})

	t.Run("fallback pick differs across partitions (not always agent[0])", func(t *testing.T) {
		seen := map[int32]struct{}{}
		for p := int32(0); p < 20; p++ {
			c := s.Candidates("t", p, 1)
			require.Len(t, c, 1)
			seen[c[0].NodeID] = struct{}{}
		}
		assert.Greater(t, len(seen), 1, "20 partitions should not all hash to the same fallback agent")
	})

	t.Run("fallback pick is reported healthy, same as a known leader", func(t *testing.T) {
		c := s.Candidates("t", 0, 1)
		require.Len(t, c, 1)
		assert.Equal(t, AgentStateHealthy, c[0].State)
	})

	t.Run("secondary candidates exclude the fallback pick, same as a known leader", func(t *testing.T) {
		c := s.Candidates("t", 0, len(agents))
		require.Len(t, c, len(agents))
		seen := make(map[int32]struct{}, len(c))
		for _, cand := range c {
			_, dup := seen[cand.NodeID]
			require.False(t, dup, "duplicate NodeID %d", cand.NodeID)
			seen[cand.NodeID] = struct{}{}
		}
		for _, id := range agents {
			assert.Contains(t, seen, id)
		}
	})

	t.Run("single live agent: fallback returns it with no secondary", func(t *testing.T) {
		single := newDefaultPartitionAssignmentStrategy([]int32{1}, map[topicPartition]int32{
			{topic: "t", partition: -1}: 1,
		})
		c := single.Candidates("t", 0, 5)
		require.Len(t, c, 1)
		assert.Equal(t, int32(1), c[0].NodeID)
	})

	t.Run("no live agents at all: still returns nil, not a zero-value agent", func(t *testing.T) {
		empty := newDefaultPartitionAssignmentStrategy(nil, map[topicPartition]int32{
			{topic: "t", partition: -1}: 1,
		})
		assert.Nil(t, empty.Candidates("t", 0, 3))
	})

	t.Run("topic entirely unknown to Metadata: no fallback, stays nil", func(t *testing.T) {
		// Unlike above, this topic has zero entries in leaders, so it's
		// unknown, not just missing one leader. No fallback here.
		unknownTopic := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
			{topic: "other-topic", partition: 0}: agents[0],
		})
		assert.Nil(t, unknownTopic.Candidates("t", 0, 3))
	})

	t.Run("mixed: some partitions have a known leader, others fall back, in the same strategy", func(t *testing.T) {
		mixed := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
			{topic: "t", partition: 0}: 3,
		})
		known := mixed.Candidates("t", 0, 1)
		require.Len(t, known, 1)
		assert.Equal(t, int32(3), known[0].NodeID)

		fallback := mixed.Candidates("t", 1, 1)
		require.Len(t, fallback, 1)
		assert.Contains(t, agents, fallback[0].NodeID)
	})
}

func TestDefaultPartitionAssignmentStrategy_PrimaryAndSecondaryViaCandidates(t *testing.T) {
	agents := []int32{1, 2, 3}
	leaders := map[topicPartition]int32{
		{topic: "t", partition: 0}: 1,
		{topic: "t", partition: 1}: 2,
	}
	s := newDefaultPartitionAssignmentStrategy(agents, leaders)

	t.Run("Candidates[0] is the leader", func(t *testing.T) {
		c := s.Candidates("t", 0, 1)
		require.Len(t, c, 1)
		assert.Equal(t, int32(1), c[0].NodeID)
		c = s.Candidates("t", 1, 1)
		require.Len(t, c, 1)
		assert.Equal(t, int32(2), c[0].NodeID)
	})

	t.Run("Candidates falls back to a live agent for unknown partition", func(t *testing.T) {
		c := s.Candidates("t", 99, 1)
		require.Len(t, c, 1)
		assert.Contains(t, agents, c[0].NodeID)
	})

	t.Run("Candidates[1] (secondary) differs from Candidates[0] (primary)", func(t *testing.T) {
		c := s.Candidates("t", 0, 2)
		require.Len(t, c, 2)
		assert.NotEqual(t, c[0].NodeID, c[1].NodeID)
	})

	t.Run("Candidates ordering is deterministic across repeated calls", func(t *testing.T) {
		c1 := s.Candidates("t", 0, 2)
		c2 := s.Candidates("t", 0, 2)
		assert.Equal(t, c1, c2)
	})

	t.Run("each Refresh creates a fresh strategy with its own cache", func(t *testing.T) {
		s1 := newDefaultPartitionAssignmentStrategy(agents, leaders)
		s2 := newDefaultPartitionAssignmentStrategy(agents, leaders)
		// Both strategies should produce the same deterministic result independently.
		c1 := s1.Candidates("t", 0, 2)
		c2 := s2.Candidates("t", 0, 2)
		assert.Equal(t, c1, c2)
	})
}

func BenchmarkDefaultPartitionAssignmentStrategy_Candidates(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		all := makeNodeIDs(n)
		s := newDefaultPartitionAssignmentStrategy(all, map[topicPartition]int32{
			{topic: "ingest", partition: 7}: all[0],
		})
		b.Run(fmt.Sprintf("agents=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = s.Candidates("ingest", 7, 3)
			}
		})
		// The real routeRecords call shape: one candidate, leader known.
		// Must never pay for the fallback hash or the non-leader walk.
		b.Run(fmt.Sprintf("agents=%d/maxCandidates=1", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = s.Candidates("ingest", 7, 1)
			}
		})
	}
}

// BenchmarkNewDefaultPartitionAssignmentStrategy measures the constructor
// cost, which now scans leaders once to build the known-topics set.
// Built once per AgentPool.Refresh, not per record, so this is off the
// hot path — Candidates (benchmarked above) is what runs per record.
//
// In all configurations partitions >= 2 * agents: an agent without partitions
// assigned to it is idle, so deployments always have more partitions than agents.
func BenchmarkNewDefaultPartitionAssignmentStrategy(b *testing.B) {
	configs := []struct{ agents, partitions int }{
		{agents: 10, partitions: 32},
		{agents: 10, partitions: 128},
		{agents: 100, partitions: 256},
		{agents: 100, partitions: 1024},
		{agents: 1000, partitions: 2048},
		{agents: 1000, partitions: 8192},
	}
	for _, cfg := range configs {
		b.Run(fmt.Sprintf("agents=%d/partitions=%d", cfg.agents, cfg.partitions), func(b *testing.B) {
			agents := makeNodeIDs(cfg.agents)
			leaders := make(map[topicPartition]int32, cfg.partitions)
			for p := int32(0); p < int32(cfg.partitions); p++ {
				leaders[topicPartition{topic: "ingest", partition: p}] = agents[int(p)%len(agents)]
			}
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				_ = newDefaultPartitionAssignmentStrategy(agents, leaders)
			}
		})
	}
}

func makeNodeIDs(n int) []int32 {
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i + 1)
	}
	return ids
}

// partitionKey identifies a (topic, partition) pair. Used as a map key by
// the test mocks below.
type partitionKey struct {
	topic     string
	partition int32
}

// mockPartitionAssignmentStrategy returns a deterministic candidate list per
// (topic, partition). The first entry in candidates is the primary, the
// rest are fallbacks in order. Every Candidates call is recorded so tests
// can assert caching / call counts.
type mockPartitionAssignmentStrategy struct {
	candidates map[partitionKey][]Agent

	mu      sync.Mutex
	counts  map[partitionKey]int
	lastMax map[partitionKey]int
}

func (s *mockPartitionAssignmentStrategy) Candidates(topic string, partition int32, maxCandidates int) []Agent {
	k := partitionKey{topic, partition}
	s.mu.Lock()
	if s.counts == nil {
		s.counts = map[partitionKey]int{}
		s.lastMax = map[partitionKey]int{}
	}
	s.counts[k]++
	s.lastMax[k] = maxCandidates
	s.mu.Unlock()

	if maxCandidates <= 0 {
		return nil
	}
	all := s.candidates[k]
	if len(all) == 0 {
		return nil
	}
	if len(all) > maxCandidates {
		return all[:maxCandidates]
	}
	return all
}

// healthyAgents builds an []Agent of the given NodeIDs, all marked
// AgentStateHealthy. Convenience for the common case in tests.
func healthyAgents(nodeIDs ...int32) []Agent {
	out := make([]Agent, len(nodeIDs))
	for i, id := range nodeIDs {
		out[i] = Agent{NodeID: id, State: AgentStateHealthy}
	}
	return out
}

// primaryOf returns a lookup function that resolves (topic, partition) to
// its primary NodeID via the strategy. Convenience for tests that build a
// strategy and need to thread routing into their helpers.
func primaryOf(s *mockPartitionAssignmentStrategy) func(string, int32) int32 {
	return func(topic string, partition int32) int32 {
		cs := s.Candidates(topic, partition, 1)
		if len(cs) == 0 {
			return 0
		}
		return cs[0].NodeID
	}
}

// candidatesCalls returns how many times Candidates() was invoked for
// (topic, partition).
func (s *mockPartitionAssignmentStrategy) candidatesCalls(topic string, partition int32) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[partitionKey{topic, partition}]
}

// lastMaxCandidates returns the maxCandidates value of the most recent
// Candidates() call for (topic, partition).
func (s *mockPartitionAssignmentStrategy) lastMaxCandidates(topic string, partition int32) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastMax[partitionKey{topic, partition}]
}

func TestHashTopicPartition(t *testing.T) {
	assert.Equal(t, hashTopicPartition("t", 3), hashTopicPartition("t", 3))
	// Both topic and partition are part of the hashed key.
	assert.NotEqual(t, hashTopicPartition("t", 0), hashTopicPartition("t", 1))
	assert.NotEqual(t, hashTopicPartition("a", 0), hashTopicPartition("b", 0))
}

func TestHashTopicPartition_NoHeapAllocation(t *testing.T) {
	allocs := testing.AllocsPerRun(1000, func() { _ = hashTopicPartition("some-topic-name", 7) })
	assert.Zero(t, allocs)
}
