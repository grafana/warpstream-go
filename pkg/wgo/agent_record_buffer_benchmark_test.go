package wgo

import (
	"bytes"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func BenchmarkAgentBuffer_MultiAdd_Fitting(b *testing.B) {
	items := make([]promised[routedTopicPartitionRecords], 4)
	for i := range items {
		items[i] = promised[routedTopicPartitionRecords]{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{
					topic:     "topic",
					partition: int32(i),
					records:   []*kgo.Record{{Value: []byte("value")}},
				},
				nodeID: 1,
			},
			done: func(ProduceResult) {},
		}
	}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	a := &AgentBuffer[routedTopicPartitionRecords]{
		batchMaxBytes:         1 << 20,
		nextProduceFlushTimer: timer,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		a.MultiAdd(items)
		a.nextProduceItems = nil
		a.nextProduceRecords = 0
		a.nextProduceWireBytes = 0
	}
}

func BenchmarkAgentBuffer_MultiAdd_Mixed(b *testing.B) {
	const batchMaxBytes = 512
	items := []promised[routedTopicPartitionRecords]{
		{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{
					topic:     "topic",
					partition: 0,
					records:   []*kgo.Record{{Value: []byte("prefix")}},
				},
				nodeID: 1,
			},
			done: func(ProduceResult) {},
		},
		{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{
					topic:     "topic",
					partition: 1,
					records: []*kgo.Record{
						{Value: bytes.Repeat([]byte("x"), 200)},
						{Value: bytes.Repeat([]byte("x"), 200)},
						{Value: bytes.Repeat([]byte("x"), 200)},
						{Value: bytes.Repeat([]byte("x"), 200)},
						{Value: bytes.Repeat([]byte("x"), 200)},
					},
				},
				nodeID: 1,
			},
			done: func(ProduceResult) {},
		},
		{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{
					topic:     "topic",
					partition: 2,
					records:   []*kgo.Record{{Value: []byte("suffix")}},
				},
				nodeID: 1,
			},
			done: func(ProduceResult) {},
		},
	}
	a := &AgentBuffer[routedTopicPartitionRecords]{
		batchMaxBytes: batchMaxBytes,
		closed:        true,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		a.MultiAdd(items)
	}
}
