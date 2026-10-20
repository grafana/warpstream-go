package main

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestObserver_ProduceOutcome(t *testing.T) {
	for _, version := range []int16{8, 11} {
		for _, errorCode := range []int16{0, kerr.RequestTimedOut.Code} {
			t.Run(fmt.Sprintf("version %d error %d", version, errorCode), func(t *testing.T) {
				response := kmsg.NewPtrProduceResponse()
				response.SetVersion(version)
				response.Topics = []kmsg.ProduceResponseTopic{{Topic: "simulation", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 1, ErrorCode: errorCode}}}}
				header := binary.BigEndian.AppendUint32(nil, 42)
				if response.IsFlexible() {
					header = append(header, 0)
				}
				var partitions []int32
				success := observeProduceResponse(response.AppendTo(header), version, func(id int32) { partitions = append(partitions, id) })
				require.Equal(t, errorCode == 0, success)
				if success {
					require.Equal(t, []int32{1}, partitions)
				} else {
					require.Empty(t, partitions)
				}
			})
		}
	}
	for _, frame := range [][]byte{nil, {0, 0, 0, 1, 128}} {
		require.False(t, observeProduceResponse(frame, 11, func(int32) { t.Fatal("invalid response counted") }))
	}
}
