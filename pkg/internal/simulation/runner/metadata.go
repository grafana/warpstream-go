package runner

import (
	"cmp"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func missingBrokerBehaviours(nodeIDs ...int32) brokersBehaviour {
	b := healthyBehaviours()
	b.missingBrokers = slices.Clone(nodeIDs)
	return b
}

type metadataView struct {
	current                atomic.Pointer[kmsg.MetadataResponse]
	template               *kmsg.MetadataResponse
	mu                     sync.Mutex
	missingLeaderResponses map[int32]int64
}

func installMetadataControl(cluster *kfake.Cluster, template *kmsg.MetadataResponse, behaviours *brokersBehaviourProvider) *metadataView {
	return installMetadataPlacementControl(cluster, template, behaviours, false, false, -1)
}

func installMetadataPlacementControl(cluster *kfake.Cluster, template *kmsg.MetadataResponse, behaviours *brokersBehaviourProvider, singleAgent, dynamic bool, retainedFallback int32) *metadataView {
	view := &metadataView{template: template}
	view.current.Store(template)
	cluster.ControlKey(int16(kmsg.Metadata), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		// kfake reshuffles leaders during membership changes. Publish a complete
		// scenario view so unrelated partitions never inherit those random leaders.
		response := metadataWithoutBrokers(view.current.Load(), req.GetVersion(), behaviours.get().missingBrokers)
		if singleAgent {
			preferred := int32(0)
			if dynamic {
				preferred = cluster.CurrentNode()
			}
			response = metadataPreferredPlacement(response, preferred, 3, retainedFallback)
		}
		view.observeMissingLeaders(response)
		return response, nil, true
	})
	return view
}

func (view *metadataView) withdrawBroker(nodeID int32) {
	previous := view.current.Load()
	next := *previous
	// Discovery can remove a pod before partition assignments converge.
	next.Brokers = slices.DeleteFunc(slices.Clone(previous.Brokers), func(b kmsg.MetadataResponseBroker) bool { return b.NodeID == nodeID })
	view.current.Store(&next)
}

func (view *metadataView) observeMissingLeaders(response *kmsg.MetadataResponse) {
	live := make(map[int32]bool, len(response.Brokers))
	for _, broker := range response.Brokers {
		live[broker.NodeID] = true
	}
	missing := make(map[int32]bool)
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if partition.Leader >= 0 && !live[partition.Leader] {
				missing[partition.Leader] = true
			}
		}
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	if view.missingLeaderResponses == nil {
		view.missingLeaderResponses = make(map[int32]int64)
	}
	for nodeID := range missing {
		view.missingLeaderResponses[nodeID]++
	}
}

func (view *metadataView) missingLeaders() map[int32]int64 {
	view.mu.Lock()
	defer view.mu.Unlock()
	return maps.Clone(view.missingLeaderResponses)
}

func metadataWithoutBrokers(template *kmsg.MetadataResponse, version int16, missing []int32) *kmsg.MetadataResponse {
	// Preserve partition leaders and topic IDs: the inconsistency between the
	// broker list and assignments is what makes AgentPool drop a leader.
	response := *template
	response.Version = version
	response.Brokers = slices.DeleteFunc(slices.Clone(template.Brokers), func(b kmsg.MetadataResponseBroker) bool {
		return slices.Contains(missing, b.NodeID)
	})
	return &response
}

func (view *metadataView) updateBroker(nodeID int32, broker *kmsg.MetadataResponseBroker) {
	previous := view.current.Load()
	next := *previous
	next.Brokers = slices.DeleteFunc(slices.Clone(previous.Brokers), func(candidate kmsg.MetadataResponseBroker) bool { return candidate.NodeID == nodeID })
	if broker != nil {
		next.Brokers = append(next.Brokers, *broker)
	}
	slices.SortFunc(next.Brokers, func(a, b kmsg.MetadataResponseBroker) int { return cmp.Compare(a.NodeID, b.NodeID) })

	live := make(map[int32]bool, len(next.Brokers))
	for _, member := range next.Brokers {
		live[member.NodeID] = true
	}
	liveReplicas := func(replicas []int32) []int32 {
		return slices.DeleteFunc(slices.Clone(replicas), func(replica int32) bool { return !live[replica] })
	}
	// Rebuild from the captured layout so returning brokers regain their replica
	// assignments without retaining departed members in any published snapshot.
	next.Topics = slices.Clone(view.template.Topics)
	for topicIndex := range next.Topics {
		topic := &next.Topics[topicIndex]
		topic.Partitions = slices.Clone(topic.Partitions)
		for partitionIndex := range topic.Partitions {
			partition := &topic.Partitions[partitionIndex]
			partition.Replicas = liveReplicas(partition.Replicas)
			partition.ISR = liveReplicas(partition.ISR)
			partition.OfflineReplicas = liveReplicas(partition.OfflineReplicas)
			if !live[partition.Leader] {
				partition.Leader = -1
				if len(partition.ISR) > 0 {
					partition.Leader = partition.ISR[0]
				} else {
					partition.ErrorCode = kerr.LeaderNotAvailable.Code
				}
			}
			oldPartition := previous.Topics[topicIndex].Partitions[partitionIndex]
			partition.LeaderEpoch = oldPartition.LeaderEpoch
			if partition.Leader != oldPartition.Leader {
				partition.LeaderEpoch++
			}
		}
	}

	view.current.Store(&next)
}

func metadataPreferredPlacement(template *kmsg.MetadataResponse, preferred int32, advertised int, retainedFallback int32) *kmsg.MetadataResponse {
	response := *template
	response.Brokers = nil
	index := slices.IndexFunc(template.Brokers, func(b kmsg.MetadataResponseBroker) bool { return b.NodeID == preferred })
	if index < 0 {
		return template
	}
	for i := 0; i < min(advertised, len(template.Brokers)); i++ {
		response.Brokers = append(response.Brokers, template.Brokers[(index+i)%len(template.Brokers)])
	}
	if len(response.Brokers) > 1 && !slices.ContainsFunc(response.Brokers, func(b kmsg.MetadataResponseBroker) bool { return b.NodeID == retainedFallback }) {
		for _, broker := range template.Brokers {
			if broker.NodeID == retainedFallback {
				response.Brokers[len(response.Brokers)-1] = broker
				break
			}
		}
	}
	response.Topics = slices.Clone(template.Topics)
	for i := range response.Topics {
		topic := &response.Topics[i]
		topic.Partitions = slices.Clone(topic.Partitions)
		for j := range topic.Partitions {
			partition := &topic.Partitions[j]
			partition.Leader = preferred
			partition.Replicas = []int32{preferred}
			partition.ISR = []int32{preferred}
		}
	}
	return &response
}
