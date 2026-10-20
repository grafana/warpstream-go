package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBrokerProxy_Metadata(t *testing.T) {
	for _, version := range []int16{8, 12} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			dialed := make(chan string, 2)
			p := newBrokerProxy(func(_ context.Context, _, address string) (net.Conn, error) {
				dialed <- address
				client, server := net.Pipe()
				go func() {
					defer server.Close()
					for {
						frame, err := readKafkaFrame(server)
						if err != nil || writeKafkaFrame(server, frame) != nil {
							return
						}
					}
				}()
				return client, nil
			})
			defer p.Close()
			response := kmsg.NewPtrMetadataResponse()
			response.SetVersion(version)
			response.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "backend", Port: 9092}}
			topic := "simulation"
			response.Topics = []kmsg.MetadataResponseTopic{{Topic: &topic, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 2, Replicas: []int32{1, 2}, ISR: []int32{1}}}}}
			header := binary.BigEndian.AppendUint32(nil, 42)
			if response.IsFlexible() {
				header = append(header, 1, 7, 2, 99, 98)
			}
			frame, err := p.rewriteMetadata(response.AppendTo(header), version)
			require.NoError(t, err)
			offset, err := responseHeaderLength(frame, response.IsFlexible())
			require.NoError(t, err)
			require.Equal(t, header, frame[:offset])
			decoded := kmsg.NewPtrMetadataResponse()
			decoded.SetVersion(version)
			require.NoError(t, decoded.ReadFrom(frame[offset:]))
			require.Equal(t, response.Topics, decoded.Topics)
			require.Equal(t, int32(1), decoded.Brokers[0].NodeID)
			broker := decoded.Brokers[0]
			address := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
			require.NotEqual(t, "backend:9092", address)
			conn, err := net.DialTimeout("tcp", address, time.Second)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
			require.NoError(t, writeKafkaFrame(conn, []byte("payload")))
			echoed, err := readKafkaFrame(conn)
			require.NoError(t, err)
			require.Equal(t, []byte("payload"), echoed)
			require.Equal(t, "backend:9092", <-dialed)
			response.Brokers[0].Port = 9093
			replaced, err := p.rewriteMetadata(response.AppendTo(header), version)
			require.NoError(t, err)
			require.NoError(t, decoded.ReadFrom(replaced[offset:]))
			require.NotEqual(t, broker.Port, decoded.Brokers[0].Port)
			require.Equal(t, response.Topics, decoded.Topics)
		})
	}
}

func TestBrokerProxy_MalformedHeader(t *testing.T) {
	for _, frame := range [][]byte{nil, {0, 0, 0, 1, 128}, {0, 0, 0, 1, 1, 2, 50}} {
		_, err := responseHeaderLength(frame, true)
		require.Error(t, err)
	}
}

func TestBrokerProxy_ResponseCorrelation(t *testing.T) {
	p := newBrokerProxy(func(_ context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			if _, err := readKafkaFrame(server); err != nil {
				return
			}
			if _, err := readKafkaFrame(server); err != nil {
				return
			}
			if writeKafkaFrame(server, []byte{0, 0, 0, 44, 99}) != nil {
				return
			}
			response := kmsg.NewPtrMetadataResponse()
			response.SetVersion(12)
			response.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "backend", Port: 9092}}
			_ = writeKafkaFrame(server, response.AppendTo([]byte{0, 0, 0, 42, 0}))
		}()
		return client, nil
	})
	defer p.Close()
	address, err := p.address("bootstrap:9092")
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
	require.NoError(t, writeKafkaFrame(conn, []byte{0, 3, 0, 12, 0, 0, 0, 42}))
	require.NoError(t, writeKafkaFrame(conn, []byte{0, 0, 0, 11, 0, 0, 0, 44}))
	opaque, err := readKafkaFrame(conn)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 44, 99}, opaque)
	frame, err := readKafkaFrame(conn)
	require.NoError(t, err)
	response := kmsg.NewPtrMetadataResponse()
	response.SetVersion(12)
	require.NoError(t, response.ReadFrom(frame[5:]))
	require.Equal(t, "127.0.0.1", response.Brokers[0].Host)
	require.NotEqual(t, int32(9092), response.Brokers[0].Port)
}

func TestBrokerProxy_ConcurrentResponseDelays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend, server := net.Pipe()
		defer server.Close()
		p := newBrokerProxy(func(context.Context, string, string) (net.Conn, error) { return backend, nil })
		p.latency = func(string) time.Duration { return time.Second }
		defer p.Close()
		app, client := net.Pipe()
		defer app.Close()
		p.wg.Add(1)
		go p.forward(client, "broker")
		go func() {
			for range 2 {
				if _, err := readKafkaFrame(server); err != nil {
					return
				}
			}
			for _, correlation := range []uint32{1, 2} {
				if writeKafkaFrame(server, binary.BigEndian.AppendUint32(nil, correlation)) != nil {
					return
				}
			}
		}()
		start := time.Now()
		for _, correlation := range []uint32{1, 2} {
			request := binary.BigEndian.AppendUint32([]byte{0, 0, 0, 11}, correlation)
			require.NoError(t, writeKafkaFrame(app, request))
		}
		for _, correlation := range []uint32{1, 2} {
			frame, err := readKafkaFrame(app)
			require.NoError(t, err)
			require.Equal(t, correlation, binary.BigEndian.Uint32(frame))
			require.Equal(t, time.Second, time.Since(start))
		}
	})
}

func TestBrokerProxy_CloseDuringDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend, server := net.Pipe()
		defer server.Close()
		p := newBrokerProxy(func(context.Context, string, string) (net.Conn, error) { return backend, nil })
		p.latency = func(string) time.Duration { return time.Hour }
		app, client := net.Pipe()
		defer app.Close()
		p.wg.Add(1)
		go p.forward(client, "broker")
		go func() {
			if _, err := readKafkaFrame(server); err == nil {
				_ = writeKafkaFrame(server, []byte{0, 0, 0, 1})
			}
		}()
		require.NoError(t, writeKafkaFrame(app, []byte{0, 0, 0, 11, 0, 0, 0, 1}))
		synctest.Wait()
		start := time.Now()
		p.Close()
		require.Zero(t, time.Since(start))
	})
}

func TestBrokerProxy_StorageTimeout(t *testing.T) {
	for _, version := range []int16{8, 11} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newBrokerProxy(func(_ context.Context, _, _ string) (net.Conn, error) {
					client, server := net.Pipe()
					go func() {
						defer server.Close()
						for {
							frame, err := readKafkaFrame(server)
							if err != nil {
								return
							}
							response := kmsg.NewPtrProduceResponse()
							response.SetVersion(version)
							response.Topics = []kmsg.ProduceResponseTopic{{Topic: "topic", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 3}}}}
							header := append([]byte(nil), frame[4:8]...)
							if response.IsFlexible() {
								header = append(header, 0)
							}
							if writeKafkaFrame(server, response.AppendTo(header)) != nil {
								return
							}
						}
					}()
					return client, nil
				})
				p.latency = func(string) time.Duration { return 3 * time.Second }
				p.enforceProduceTimeout = true
				var delivered atomic.Int64
				p.observe = func(_ string, version int16, frame []byte) {
					if observeProduceResponse(frame, version, nil) {
						delivered.Add(1)
					}
				}
				client, server := net.Pipe()
				p.wg.Add(1)
				go p.forward(server, "agent")
				defer p.Close()
				defer client.Close()
				for _, budget := range []int32{2000, 4000} {
					request := kmsg.NewPtrProduceRequest()
					request.SetVersion(version)
					request.TimeoutMillis = budget
					header := binary.BigEndian.AppendUint16(nil, uint16(kmsg.Produce))
					header = binary.BigEndian.AppendUint16(header, uint16(version))
					header = binary.BigEndian.AppendUint32(header, uint32(budget))
					header = append(header, 255, 255)
					if request.IsFlexible() {
						header = append(header, 0)
					}
					frame := request.AppendTo(header)
					parsed, err := produceTimeout(frame, version)
					require.NoError(t, err)
					require.Equal(t, time.Duration(budget)*time.Millisecond, parsed)
					start := time.Now()
					require.NoError(t, writeKafkaFrame(client, frame))
					frame, err = readKafkaFrame(client)
					require.NoError(t, err)
					require.Equal(t, min(3*time.Second, parsed), time.Since(start))
					response := kmsg.NewPtrProduceResponse()
					response.SetVersion(version)
					offset, err := responseHeaderLength(frame, response.IsFlexible())
					require.NoError(t, err)
					require.NoError(t, response.ReadFrom(frame[offset:]))
					require.Equal(t, uint32(budget), binary.BigEndian.Uint32(frame[:4]))
					partition := response.Topics[0].Partitions[0]
					require.EqualValues(t, 3, partition.Partition)
					synctest.Wait()
					if budget == 2000 {
						require.Zero(t, delivered.Load())
						require.Equal(t, kerr.RequestTimedOut.Code, partition.ErrorCode)
					} else {
						require.Zero(t, partition.ErrorCode)
						require.EqualValues(t, 1, delivered.Load())
					}
				}
			})
		})
	}
}
