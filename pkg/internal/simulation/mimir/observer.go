package main

import "github.com/twmb/franz-go/pkg/kmsg"

func observeProduceResponse(frame []byte, version int16, partition func(int32)) bool {
	response := kmsg.NewPtrProduceResponse()
	response.SetVersion(version)
	offset, err := responseHeaderLength(frame, response.IsFlexible())
	if err != nil || response.ReadFrom(frame[offset:]) != nil {
		return false
	}
	succeeded := false
	for _, topic := range response.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode == 0 {
				succeeded = true
				if partition != nil {
					partition(part.Partition)
				}
			}
		}
	}
	return succeeded
}
