package runner

import (
	"context"
	"maps"
	"net"
	"sync"
	"syscall"
	"time"
)

type transportFault int

const (
	transportHealthy transportFault = iota
	transportKilled
	transportBlackhole
)

type transportNetwork struct {
	dial               func(context.Context, string, string) (net.Conn, error)
	mu                 sync.Mutex
	closed             bool
	brokers            map[string]int32
	faults             map[int32]transportFault
	connections        map[*transportConnection]struct{}
	droppedBytes       int64
	rejectedDials      int64
	closedConnections  int64
	byBroker           map[int32]brokerTransportStats
	restored           map[int32]bool
	recoveredResponses map[int32]int64
	recoveryStart      time.Time
}

func newTransportNetwork(dial func(context.Context, string, string) (net.Conn, error)) *transportNetwork {
	return &transportNetwork{dial: dial, brokers: make(map[string]int32), faults: make(map[int32]transportFault), connections: make(map[*transportConnection]struct{}), byBroker: make(map[int32]brokerTransportStats), restored: make(map[int32]bool), recoveredResponses: make(map[int32]int64)}
}

func (n *transportNetwork) register(address string, nodeID int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.brokers[address] = nodeID
}

func (n *transportNetwork) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, net.ErrClosed
	}
	nodeID, known := n.brokers[address]
	if !known || n.faults[nodeID] == transportKilled {
		n.rejectedDials++
		n.mu.Unlock()
		return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
	}
	n.mu.Unlock()
	raw, err := n.dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	app, proxy := net.Pipe()
	connection := &transportConnection{Conn: app, raw: raw, proxy: proxy, network: n, nodeID: nodeID}
	n.mu.Lock()
	// A dial already in progress must not escape shutdown or a pod kill.
	if n.closed {
		n.mu.Unlock()
		_ = connection.Close()
		return nil, net.ErrClosed
	}
	if known && n.faults[nodeID] == transportKilled {
		n.rejectedDials++
		n.mu.Unlock()
		_ = connection.Close()
		return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
	}
	n.connections[connection] = struct{}{}
	n.mu.Unlock()
	go connection.forward(raw, proxy)
	go connection.forward(proxy, raw)
	return connection, nil
}

func (n *transportNetwork) setFault(nodeID int32, fault transportFault) {
	n.mu.Lock()
	previous := n.faults[nodeID]
	n.faults[nodeID] = fault
	if fault == transportHealthy && previous != transportHealthy {
		n.restored[nodeID] = true
	}
	var closeConnections []*transportConnection
	if fault == transportKilled || previous == transportBlackhole && fault == transportHealthy {
		for connection := range n.connections {
			if connection.nodeID == nodeID {
				closeConnections = append(closeConnections, connection)
			}
		}
		n.closedConnections += int64(len(closeConnections))
		stats := n.byBroker[nodeID]
		stats.closedConnections += int64(len(closeConnections))
		n.byBroker[nodeID] = stats
	}
	n.mu.Unlock()
	// Dropped stream bytes cannot be replayed safely after connectivity returns.
	for _, connection := range closeConnections {
		_ = connection.Close()
	}
}

func (n *transportNetwork) drop(nodeID int32, size int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.faults[nodeID] != transportBlackhole {
		return false
	}
	n.droppedBytes += int64(size)
	stats := n.byBroker[nodeID]
	stats.droppedBytes += int64(size)
	n.byBroker[nodeID] = stats
	return true
}

func (n *transportNetwork) Close() {
	n.mu.Lock()
	n.closed = true
	connections := make([]*transportConnection, 0, len(n.connections))
	for connection := range n.connections {
		connections = append(connections, connection)
	}
	n.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

type transportConnection struct {
	net.Conn
	raw, proxy net.Conn
	network    *transportNetwork
	nodeID     int32
	once       sync.Once
}

func (c *transportConnection) Close() error {
	c.once.Do(func() {
		_ = c.Conn.Close()
		_ = c.proxy.Close()
		_ = c.raw.Close()
		c.network.mu.Lock()
		delete(c.network.connections, c)
		c.network.mu.Unlock()
	})
	return nil
}

func (c *transportConnection) forward(destination, source net.Conn) {
	defer c.Close() // A disconnected peer must also release the opposite copy loop.
	buffer := make([]byte, 32*1024)
	for {
		size, err := source.Read(buffer)
		if size > 0 && !c.network.drop(c.nodeID, size) {
			remaining := buffer[:size]
			for len(remaining) > 0 {
				written, writeErr := destination.Write(remaining)
				if writeErr != nil || written == 0 {
					return
				}
				remaining = remaining[written:]
			}
		}
		if err != nil {
			return
		}
	}
}

type brokerTransportStats struct {
	droppedBytes, closedConnections int64
}

type transportStats struct {
	byBroker                                       map[int32]brokerTransportStats
	droppedBytes, rejectedDials, closedConnections int64
	recoveredResponses                             map[int32]int64
}

func (n *transportNetwork) observeProduce(nodeID int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restored[nodeID] && n.faults[nodeID] == transportHealthy && !time.Now().Before(n.recoveryStart) {
		n.recoveredResponses[nodeID]++
	}
}

func (n *transportNetwork) snapshot() transportStats {
	if n == nil {
		return transportStats{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return transportStats{
		droppedBytes: n.droppedBytes, rejectedDials: n.rejectedDials, closedConnections: n.closedConnections,
		byBroker: maps.Clone(n.byBroker), recoveredResponses: maps.Clone(n.recoveredResponses),
	}
}
