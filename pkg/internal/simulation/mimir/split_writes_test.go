package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

func TestEventRecords_SplitWritesGivesOneRecordPerSeries(t *testing.T) {
	t.Parallel()
	split := &distributorClient{config: runner.WorkloadConfig{Partitions: 50, SplitWrites: true}, series: 50}
	first := split.EventRecords("t", 50, 0)
	assert.Len(t, first, 50)
	next := split.EventRecords("t", 50, 500*time.Millisecond)
	assert.NotEqual(t, first[0].Value, next[0].Value, "series names move with the event so the ring is covered over time")

	wide := &distributorClient{config: runner.WorkloadConfig{Partitions: 50}, series: 1600}
	assert.Len(t, wide.EventRecords("t", 50, 0), 1)
}
