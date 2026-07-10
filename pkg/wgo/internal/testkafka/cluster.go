package testkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

// virtualBasePort is the first virtual port assigned to a cluster's brokers
// when running on an in-memory network. Ports only need to be distinct within a
// single VirtualNetwork, and every test uses its own, so a fixed base keeps the
// assignment deterministic.
const virtualBasePort = 9092

type options struct {
	numBrokers int
	vnet       *kfake.VirtualNetwork
}

type Opt func(*options)

// WithNumBrokers overrides the default number of brokers (1) in the fake cluster.
func WithNumBrokers(n int) Opt {
	return func(o *options) { o.numBrokers = n }
}

// WithVirtualNetwork routes the cluster's listeners through an in-memory network
// so it can run inside a testing/synctest bubble. Pair the same VirtualNetwork's
// DialContext with every client (wgo.WithDialer / kgo.Dialer) so no real sockets
// are opened.
func WithVirtualNetwork(vnet *kfake.VirtualNetwork) Opt {
	return func(o *options) { o.vnet = vnet }
}

// CreateCluster returns a fake Kafka cluster for unit testing.
//
// When multiple brokers are configured (via WithNumBrokers), partition leaders are assigned
// in a round-robin fashion: partition 0 → broker 0, partition 1 → broker 1, etc.
// This means that if the number of brokers is >= the number of partitions, each partition
// is guaranteed to be on a different broker.
func CreateCluster(t testing.TB, numPartitions int32, topicName string, opts ...Opt) (*kfake.Cluster, string) {
	o := options{numBrokers: 1}
	for _, opt := range opts {
		opt(&o)
	}

	cfg := []kfake.Opt{
		kfake.NumBrokers(o.numBrokers),
		kfake.SeedTopics(numPartitions, topicName),
	}

	if o.vnet != nil {
		// The virtual network can't auto-assign a port, so give every broker a distinct virtual port.
		ports := make([]int, o.numBrokers)
		for i := range ports {
			ports[i] = virtualBasePort + i
		}
		cfg = append(cfg, kfake.ListenFn(o.vnet.Listen), kfake.Ports(ports...))
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
