package main

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func echoTransport(t *testing.T) *transportNetwork {
	t.Helper()
	network := newTransportNetwork(func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		go func() { _, _ = io.Copy(server, server) }()
		return client, nil
	})
	network.register("broker", 1)
	t.Cleanup(network.Close)
	return network
}

func assertEcho(t *testing.T, connection net.Conn) {
	t.Helper()
	_, err := connection.Write([]byte("hello"))
	require.NoError(t, err)
	buffer := make([]byte, 5)
	_, err = io.ReadFull(connection, buffer)
	require.NoError(t, err)
	require.Equal(t, "hello", string(buffer))
}

func TestTransportNetwork_PodKill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := echoTransport(t)
		connection, err := network.DialContext(t.Context(), "tcp", "broker")
		require.NoError(t, err)
		assertEcho(t, connection)
		network.setFault(1, transportKilled)
		_, err = connection.Write([]byte("lost"))
		require.Error(t, err)
		_, err = network.DialContext(t.Context(), "tcp", "broker")
		require.ErrorContains(t, err, "connection refused")
		network.setFault(1, transportHealthy)
		replacement, err := network.DialContext(t.Context(), "tcp", "broker")
		require.NoError(t, err)
		assertEcho(t, replacement)
		network.mu.Lock()
		assert.Equal(t, int64(1), network.rejectedDials)
		assert.Equal(t, int64(1), network.closedConnections)
		network.mu.Unlock()
	})
}

func TestTransportNetwork_BlackholeDeadlinesAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := echoTransport(t)
		connection, err := network.DialContext(t.Context(), "tcp", "broker")
		require.NoError(t, err)
		assertEcho(t, connection)
		network.setFault(1, transportBlackhole)
		require.NoError(t, connection.SetReadDeadline(time.Now().Add(time.Second)))
		_, err = connection.Write([]byte("dropped"))
		require.NoError(t, err)
		_, err = connection.Read(make([]byte, 7))
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		network.mu.Lock()
		assert.Equal(t, int64(7), network.droppedBytes)
		assert.Zero(t, network.closedConnections)
		network.mu.Unlock()
		network.setFault(1, transportHealthy)
		replacement, err := network.DialContext(t.Context(), "tcp", "broker")
		require.NoError(t, err)
		assertEcho(t, replacement)
	})
}

func TestTransportNetwork_DropsResponsesOnEstablishedConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()
		network := newTransportNetwork(func(context.Context, string, string) (net.Conn, error) { return client, nil })
		defer network.Close()
		network.register("broker", 1)
		connection, err := network.DialContext(t.Context(), "tcp", "broker")
		require.NoError(t, err)
		readDone := make(chan struct{})
		go func() { _, _ = io.ReadFull(server, make([]byte, 1)); close(readDone) }()
		_, err = connection.Write([]byte("x"))
		require.NoError(t, err)
		<-readDone
		network.setFault(1, transportBlackhole)
		_, err = server.Write([]byte("response"))
		require.NoError(t, err)
		synctest.Wait()
		require.NoError(t, connection.SetReadDeadline(time.Now().Add(time.Second)))
		_, err = connection.Read(make([]byte, 8))
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		network.mu.Lock()
		assert.Equal(t, int64(8), network.droppedBytes)
		network.mu.Unlock()
	})
}

func TestTransportNetwork_BrokerEvidenceSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := echoTransport(t)
		network.register("other", 2)
		connection, err := network.DialContext(t.Context(), "tcp", "broker")
		require.NoError(t, err)
		assertEcho(t, connection)
		network.setFault(1, transportKilled)
		before := network.snapshot()
		assert.Equal(t, int64(1), before.byBroker[1].closedConnections)
		assert.Zero(t, before.byBroker[2].closedConnections)
		network.setFault(2, transportBlackhole)
		assert.True(t, network.drop(2, 10))
		after := network.snapshot()
		assert.Equal(t, int64(10), after.byBroker[2].droppedBytes)
		assert.Zero(t, after.byBroker[1].droppedBytes)
		assert.Zero(t, before.byBroker[2].droppedBytes)
	})
}
