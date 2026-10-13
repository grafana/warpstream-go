package main

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func missingBrokerBehaviours(nodeIDs ...int32) brokersBehaviour {
	b := healthyBehaviours()
	b.missingBrokers = slices.Clone(nodeIDs)
	return b
}

func installMetadataControl(cluster *kfake.Cluster, template *kmsg.MetadataResponse, behaviours *brokersBehaviourProvider) {
	cluster.ControlKey(int16(kmsg.Metadata), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		missing := behaviours.get().missingBrokers
		if len(missing) == 0 {
			return nil, nil, false
		}
		return metadataWithoutBrokers(template, req.GetVersion(), missing), nil, true
	})
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
