package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type brokerProxy struct {
	dial                  func(context.Context, string, string) (net.Conn, error)
	latency               func(string) time.Duration
	enforceProduceTimeout bool
	observe               func(string, int16, []byte)
	ctx                   context.Context
	cancel                context.CancelFunc
	mu                    sync.Mutex
	closed                bool
	listeners             map[string]net.Listener
	connections           map[net.Conn]struct{}
	wg                    sync.WaitGroup
}

func newBrokerProxy(dial func(context.Context, string, string) (net.Conn, error)) *brokerProxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &brokerProxy{dial: dial, ctx: ctx, cancel: cancel, listeners: make(map[string]net.Listener), connections: make(map[net.Conn]struct{})}
}

func (p *brokerProxy) address(target string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return "", net.ErrClosed
	}
	if listener := p.listeners[target]; listener != nil {
		return listener.Addr().String(), nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	p.listeners[target] = listener
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				_ = client.Close()
				return
			}
			p.connections[client] = struct{}{}
			p.wg.Add(1)
			p.mu.Unlock()
			go p.forward(client, target)
		}
	}()
	return listener.Addr().String(), nil
}

func (p *brokerProxy) forward(client net.Conn, target string) {
	defer p.wg.Done()
	defer func() { _ = client.Close(); p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
	backend, err := p.dial(p.ctx, "tcp", target)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = backend.Close()
		return
	}
	p.connections[backend] = struct{}{}
	p.mu.Unlock()
	defer func() { _ = backend.Close(); p.mu.Lock(); delete(p.connections, backend); p.mu.Unlock() }()
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	var mu sync.Mutex
	type requestKind struct {
		api, version int16
		timeout      time.Duration
	}
	requests := map[int32]requestKind{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		defer backend.Close()
		defer client.Close()
		for {
			frame, err := readKafkaFrame(client)
			if err != nil {
				return
			}
			if len(frame) >= 8 {
				api := int16(binary.BigEndian.Uint16(frame[:2]))
				if api == int16(kmsg.Metadata) || api == int16(kmsg.Produce) {
					kind := requestKind{api: api, version: int16(binary.BigEndian.Uint16(frame[2:4]))}
					if api == int16(kmsg.Produce) && p.enforceProduceTimeout {
						kind.timeout, err = produceTimeout(frame, kind.version)
						if err != nil {
							return
						}
					}
					mu.Lock()
					requests[int32(binary.BigEndian.Uint32(frame[4:8]))] = kind
					mu.Unlock()
				}
			}
			if writeKafkaFrame(backend, frame) != nil {
				return
			}
		}
	}()
	type response struct {
		frame   []byte
		due     time.Time
		produce bool
		version int16
	}
	responses := make(chan response, 64)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer close(responses)
		for {
			frame, err := readKafkaFrame(backend)
			if err != nil {
				return
			}
			due := time.Now()
			var produce bool
			var version int16
			if len(frame) >= 4 {
				correlation := int32(binary.BigEndian.Uint32(frame[:4]))
				mu.Lock()
				kind, known := requests[correlation]
				delete(requests, correlation)
				mu.Unlock()
				if known && kind.api == int16(kmsg.Metadata) {
					frame, err = p.rewriteMetadata(frame, kind.version)
					if err != nil {
						return
					}
				}
				if known && kind.api == int16(kmsg.Produce) {
					produce, version = true, kind.version
				}
				if produce && p.latency != nil {
					delay := p.latency(target)
					if p.enforceProduceTimeout && delay > kind.timeout {
						delay = kind.timeout
						frame, err = timeoutProduceResponse(frame, kind.version)
						if err != nil {
							return
						}
					}
					due = due.Add(delay)
				}
			}
			select {
			case responses <- response{frame: frame, due: due, produce: produce, version: version}:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); _ = client.Close(); _ = backend.Close(); <-done; <-readDone }()
	// Keep Kafka response order without adding each delay to the next request's service time.
	for response := range responses {
		if delay := time.Until(response.due); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		if writeKafkaFrame(client, response.frame) != nil {
			return
		}
		if response.produce && p.observe != nil {
			p.observe(target, response.version, response.frame)
		}
	}
}

func (p *brokerProxy) rewriteMetadata(frame []byte, version int16) ([]byte, error) {
	response := kmsg.NewPtrMetadataResponse()
	response.SetVersion(version)
	offset, err := responseHeaderLength(frame, response.IsFlexible())
	if err != nil {
		return nil, err
	}
	if err := response.ReadFrom(frame[offset:]); err != nil {
		return nil, err
	}
	for i := range response.Brokers {
		broker := &response.Brokers[i]
		address, err := p.address(net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port))))
		if err != nil {
			return nil, err
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		number, err := strconv.Atoi(port)
		if err != nil {
			return nil, err
		}
		broker.Host, broker.Port = host, int32(number)
	}
	return response.AppendTo(append([]byte(nil), frame[:offset]...)), nil
}

func responseHeaderLength(frame []byte, flexible bool) (int, error) {
	if len(frame) < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	offset := 4
	if !flexible {
		return offset, nil
	}
	read := func() (uint64, error) {
		value, n := binary.Uvarint(frame[offset:])
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		offset += n
		return value, nil
	}
	count, err := read()
	if err != nil {
		return 0, err
	}
	for range count {
		if _, err := read(); err != nil {
			return 0, err
		}
		length, err := read()
		if err != nil || length > uint64(len(frame)-offset) {
			return 0, io.ErrUnexpectedEOF
		}
		offset += int(length)
	}
	return offset, nil
}

func readKafkaFrame(reader io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size > 32<<20 {
		return nil, fmt.Errorf("kafka frame exceeds 32MiB")
	}
	frame := make([]byte, int(size))
	_, err := io.ReadFull(reader, frame)
	return frame, err
}

func writeKafkaFrame(writer io.Writer, frame []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(frame)))
	for _, part := range [][]byte{header[:], frame} {
		for len(part) > 0 {
			n, err := writer.Write(part)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}

func (p *brokerProxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.cancel()
	for _, listener := range p.listeners {
		_ = listener.Close()
	}
	for connection := range p.connections {
		_ = connection.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func produceTimeout(frame []byte, version int16) (time.Duration, error) {
	if len(frame) < 10 {
		return 0, io.ErrUnexpectedEOF
	}
	clientLength := int(int16(binary.BigEndian.Uint16(frame[8:10])))
	if clientLength < -1 {
		return 0, fmt.Errorf("invalid Kafka client ID length")
	}
	offset := 10 + max(0, clientLength)
	if offset > len(frame) {
		return 0, io.ErrUnexpectedEOF
	}
	request := kmsg.NewPtrProduceRequest()
	request.SetVersion(version)
	if request.IsFlexible() {
		// Request headers place tagged fields after the nullable client ID.
		header, err := responseHeaderLength(frame[offset-4:], true)
		if err != nil {
			return 0, err
		}
		offset += header - 4
	}
	if err := request.ReadFrom(frame[offset:]); err != nil {
		return 0, err
	}
	return time.Duration(request.TimeoutMillis) * time.Millisecond, nil
}

func timeoutProduceResponse(frame []byte, version int16) ([]byte, error) {
	response := kmsg.NewPtrProduceResponse()
	response.SetVersion(version)
	offset, err := responseHeaderLength(frame, response.IsFlexible())
	if err != nil {
		return nil, err
	}
	if err := response.ReadFrom(frame[offset:]); err != nil {
		return nil, err
	}
	for i := range response.Topics {
		for j := range response.Topics[i].Partitions {
			partition := &response.Topics[i].Partitions[j]
			if partition.ErrorCode == 0 {
				partition.ErrorCode = kerr.RequestTimedOut.Code
			}
		}
	}
	return response.AppendTo(append([]byte(nil), frame[:offset]...)), nil
}
