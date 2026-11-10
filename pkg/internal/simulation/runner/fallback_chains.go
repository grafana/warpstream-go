package runner

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

// A partition's candidates are its leader followed by consecutive agents from a
// hash of (topic, partition). fallbackChain reproduces that walk for the
// simulation's layout, where agent p leads partition p, so scenarios can place
// slow agents knowing which fallbacks a hedge will race. The golden table in
// fallback_chains_test.go and a test in pkg/wgo pin it to wgo's own output.
const candidateChainLength = wgo.DefaultHedgerMaxHedgeAgents

func fallbackChain(topic string, partition, agents int32) []int32 {
	var d xxhash.Digest
	_, _ = d.WriteString(topic)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(partition))
	_, _ = d.Write(b[:])
	nonLeaders := int(agents) - 1
	start := int(d.Sum64() % uint64(nonLeaders))
	chain := []int32{partition}
	for offset := 0; offset < nonLeaders && len(chain) < candidateChainLength; offset++ {
		id := int32((start + offset) % nonLeaders)
		if id >= partition {
			id++ // the leader is skipped
		}
		chain = append(chain, id)
	}
	return chain
}

// stalledRule says where the fast agents have to sit in the chain of every
// partition that a stalled agent leads.
type stalledRule int

const (
	// fastFirstFallback leaves every stalled partition a fast first fallback, which
	// is the only one a hedge races unless a leg fails with an error.
	fastFirstFallback stalledRule = iota
	// fastSecondFallback leaves no chain entirely stalled and makes at least
	// minNeedingSecondFallback partitions rely on the second fallback, which only a
	// client that races another candidate when one stalls reaches in time.
	fastSecondFallback
)

const (
	minNeedingSecondFallback = 3
	maxStalledSearch         = 1_000_000
)

// chainExposure counts, among the partitions led by a stalled agent, those whose
// first fallback is stalled too and those whose whole chain is stalled.
func chainExposure(stalled []int32) (firstFallbackStalled, wholeChainStalled int) {
	isStalled := make(map[int32]bool, len(stalled))
	for _, id := range stalled {
		isStalled[id] = true
	}
	for _, p := range stalled {
		chain := fallbackChain(topicName, p, clusterSize)
		if isStalled[chain[1]] {
			firstFallbackStalled++
			if slices.ContainsFunc(chain, func(id int32) bool { return !isStalled[id] }) {
				continue
			}
			wholeChainStalled++
		}
	}
	return firstFallbackStalled, wholeChainStalled
}

func (r stalledRule) allows(stalled []int32) bool {
	first, whole := chainExposure(stalled)
	if r == fastFirstFallback {
		return first == 0
	}
	return whole == 0 && first >= minNeedingSecondFallback
}

type stalledKey struct {
	count int32
	rule  stalledRule
	runID int
}

var (
	stalledCacheMu sync.Mutex
	stalledCache   = map[stalledKey][]int32{}
)

// defaultRunID is the run id of a plain run. Scenarios that vary their setup with
// the run id behave as they did before it existed for this id.
const defaultRunID = 1

// findStalledAgents returns the runID-th set of count agent ids that satisfies
// rule, taking candidate sets in order of seed. The same id gives the same agents
// on every build and run, and each id gives a different set that meets the same
// rule. It fails when the search finds fewer than runID such sets.
func findStalledAgents(count int32, rule stalledRule, runID int) ([]int32, error) {
	key := stalledKey{count, rule, runID}
	stalledCacheMu.Lock()
	defer stalledCacheMu.Unlock()
	if ids, ok := stalledCache[key]; ok {
		return slices.Clone(ids), nil
	}
	seen := map[string]bool{}
	found := 0
	for seed := uint64(1); seed <= maxStalledSearch; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed))
		ids := make([]int32, 0, count)
		for _, id := range rng.Perm(int(clusterSize))[:count] {
			ids = append(ids, int32(id))
		}
		slices.SortFunc(ids, cmp.Compare[int32])
		if !rule.allows(ids) || seen[fmt.Sprint(ids)] {
			continue
		}
		seen[fmt.Sprint(ids)] = true
		if found++; found == runID {
			stalledCache[key] = ids
			return slices.Clone(ids), nil
		}
	}
	return nil, fmt.Errorf("found %d of the %d sets of %d stalled agents out of %d that satisfy rule %d within %d seeds", found, runID, count, clusterSize, rule, maxStalledSearch)
}

func pickStalledAgents(count int32, rule stalledRule, runID int) []int32 {
	ids, err := findStalledAgents(count, rule, runID)
	if err != nil {
		panic("simulation: " + err.Error())
	}
	return ids
}
