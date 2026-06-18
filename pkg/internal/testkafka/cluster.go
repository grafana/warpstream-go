package testkafka

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

type Opt func() []kfake.Opt

// WithNumBrokers overrides the default number of brokers (1) in the fake cluster.
func WithNumBrokers(n int) Opt {
	return func() []kfake.Opt {
		return []kfake.Opt{kfake.NumBrokers(n)}
	}
}

// NewCluster creates a fake Kafka cluster and returns it plus the address of its
// first broker. The caller owns the cluster and must Close it.
//
// When multiple brokers are configured (via WithNumBrokers), partition leaders are
// assigned round-robin: partition 0 → broker 0, partition 1 → broker 1, etc. So if
// brokers >= partitions, each partition is guaranteed to be on a different broker.
func NewCluster(numPartitions int32, topicName string, opts ...Opt) (*kfake.Cluster, string, error) {
	cfg := []kfake.Opt{
		kfake.NumBrokers(1),
		kfake.SeedTopics(numPartitions, topicName),
	}
	for _, opt := range opts {
		cfg = append(cfg, opt()...)
	}

	cluster, err := kfake.NewCluster(cfg...)
	if err != nil {
		return nil, "", err
	}

	addrs := cluster.ListenAddrs()
	if len(addrs) == 0 {
		cluster.Close()
		return nil, "", errors.New("testkafka: fake cluster has no listen addresses")
	}

	// kfake assigns leaders randomly by default; override to the round-robin
	// layout the doc comment promises.
	if numBrokers := int32(len(addrs)); numBrokers > 1 {
		for i := int32(0); i < numPartitions; i++ {
			if err := cluster.MoveTopicPartition(topicName, i, i%numBrokers); err != nil {
				cluster.Close()
				return nil, "", err
			}
		}
	}

	return cluster, addrs[0], nil
}

// CreateCluster returns a fake Kafka cluster for unit testing, registering its
// Close with t.Cleanup.
func CreateCluster(t testing.TB, numPartitions int32, topicName string, opts ...Opt) (*kfake.Cluster, string) {
	t.Helper()
	cluster, addr, err := NewCluster(numPartitions, topicName, opts...)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster, addr
}
