package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func kafkaFrame(body []byte) []byte {
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	return append(frame, body...)
}

func TestObservedConn_ProduceOutcome(t *testing.T) {
	for _, version := range []int16{8, 11} {
		for _, errorCode := range []int16{0, 6} {
			t.Run(fmt.Sprintf("version %d error %d", version, errorCode), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client, server := net.Pipe()
					defer client.Close()
					defer server.Close()
					var successes int
					observer := &observedConn{Conn: client, versions: make(map[int32]int16), success: func() { successes++ }}
					request := binary.BigEndian.AppendUint16(nil, 0)
					request = binary.BigEndian.AppendUint16(request, uint16(version))
					request = binary.BigEndian.AppendUint32(request, 42)
					request = kafkaFrame(request)
					response := kmsg.NewPtrProduceResponse()
					response.SetVersion(version)
					response.Topics = []kmsg.ProduceResponseTopic{{Topic: "simulation", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 1, ErrorCode: errorCode}}}}
					reply := binary.BigEndian.AppendUint32(nil, 42)
					if response.IsFlexible() {
						reply = append(reply, 0)
					}
					reply = kafkaFrame(append(reply, response.AppendTo(nil)...))
					done := make(chan struct{})
					go func() {
						defer close(done)
						_, _ = io.ReadFull(server, make([]byte, len(request)))
						_, _ = server.Write(reply[:2])
						_, _ = server.Write(reply[2:])
					}()
					_, err := observer.Write(request[:2])
					require.NoError(t, err)
					_, err = observer.Write(request[2:])
					require.NoError(t, err)
					for read := 0; read < len(reply); {
						n, err := observer.Read(make([]byte, 3))
						require.NoError(t, err)
						read += n
					}
					<-done
					if errorCode == 0 {
						assert.Equal(t, 1, successes)
					} else {
						assert.Zero(t, successes)
					}
					assert.Empty(t, observer.versions)
				})
			})
		}
	}
}
