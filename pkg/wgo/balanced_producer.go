package wgo

import (
	"context"
	"sync"
)

// Prefer an idle connection so a stalled response cannot queue every fallback behind it.
type balancedProducer struct {
	producers []DirectProducer
	mu        sync.Mutex
	active    map[int32][]int
}

func newBalancedProducer(producers []DirectProducer) *balancedProducer {
	return &balancedProducer{producers: producers, active: map[int32][]int{}}
}

func (p *balancedProducer) ProduceSync(ctx context.Context, target Agent, partitions []encodedTopicPartitionRecords) ProduceResult {
	if err := ctx.Err(); err != nil {
		return ProduceResult{err: err}
	}
	p.mu.Lock()
	counts := p.active[target.NodeID]
	if counts == nil {
		counts = make([]int, len(p.producers))
		p.active[target.NodeID] = counts
	}
	selected := 0
	for i := range counts {
		if counts[i] < counts[selected] {
			selected = i
		}
	}
	counts[selected]++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		counts[selected]--
		for _, count := range counts {
			if count > 0 {
				return
			}
		}
		delete(p.active, target.NodeID)
	}()
	return p.producers[selected].ProduceSync(ctx, target, partitions)
}
