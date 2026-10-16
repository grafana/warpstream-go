package main

import (
	"context"
	"encoding/binary"
	"net"
	"sync"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type observedConn struct {
	net.Conn
	mu                  sync.Mutex
	requests, responses []byte
	versions            map[int32]int16
	success             func()
	partition           func(int32)
}

func observedDialer(dial func(context.Context, string, string) (net.Conn, error), observe func(string), partition func(int32)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &observedConn{Conn: connection, versions: make(map[int32]int16), success: func() { observe(address) }, partition: partition}, nil
	}
}
func (c *observedConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	c.requests = append(c.requests, data...)
	c.requests = consumeFrames(c.requests, func(frame []byte) {
		if len(frame) >= 8 && binary.BigEndian.Uint16(frame[:2]) == 0 {
			c.versions[int32(binary.BigEndian.Uint32(frame[4:8]))] = int16(binary.BigEndian.Uint16(frame[2:4]))
		}
	})
	c.mu.Unlock()
	return c.Conn.Write(data)
}
func (c *observedConn) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses = append(c.responses, data[:n]...)
	c.responses = consumeFrames(c.responses, func(frame []byte) {
		if len(frame) < 4 {
			return
		}
		correlation := int32(binary.BigEndian.Uint32(frame[:4]))
		version, ok := c.versions[correlation]
		if !ok {
			return
		}
		delete(c.versions, correlation)
		body := frame[4:]
		response := kmsg.NewPtrProduceResponse()
		response.SetVersion(version)
		if response.IsFlexible() {
			count, size := binary.Uvarint(body)
			if size <= 0 {
				return
			}
			body = body[size:]
			for range count {
				_, size = binary.Uvarint(body)
				if size <= 0 {
					return
				}
				body = body[size:]
				length, used := binary.Uvarint(body)
				if used <= 0 || length > uint64(len(body)-used) {
					return
				}
				body = body[used+int(length):]
			}
		}
		if response.ReadFrom(body) != nil {
			return
		}
		succeeded := false
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode == 0 {
					succeeded = true
					if c.partition != nil {
						c.partition(partition.Partition)
					}
				}
			}
		}
		if succeeded {
			c.success()
		}
	})
	return n, err
}
func consumeFrames(data []byte, consume func([]byte)) []byte {
	for len(data) >= 4 {
		size := uint64(binary.BigEndian.Uint32(data[:4]))
		if size > 32<<20 {
			return nil
		}
		if size+4 > uint64(len(data)) {
			break
		}
		consume(data[4 : 4+int(size)])
		data = data[4+int(size):]
	}
	return data
}
