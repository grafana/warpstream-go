package testkafka

import (
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

// CreateCluster returns a fake Kafka cluster for unit testing.
//
// When multiple brokers are configured (via WithNumBrokers), partition leaders are assigned
// in a round-robin fashion: partition 0 → broker 0, partition 1 → broker 1, etc.
// This means that if the number of brokers is >= the number of partitions, each partition
// is guaranteed to be on a different broker.
func CreateCluster(t testing.TB, numPartitions int32, topicName string, opts ...Opt) (*kfake.Cluster, string) {
	cfg := []kfake.Opt{
		kfake.NumBrokers(1),
		kfake.SeedTopics(numPartitions, topicName),
	}

	// Apply options.
	for _, opt := range opts {
		cfg = append(cfg, opt()...)
	}

	cluster, err := kfake.NewCluster(cfg...)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.NotEmpty(t, addrs)

	// Assign partition leaders in a round-robin fashion across brokers.
	// kfake assigns leaders randomly by default, so we override it here.
	if numBrokers := int32(len(addrs)); numBrokers > 1 {
		for i := int32(0); i < numPartitions; i++ {
			require.NoError(t, cluster.MoveTopicPartition(topicName, i, i%numBrokers))
		}
	}

	return cluster, addrs[0]
}
