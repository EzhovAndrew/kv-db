package network

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Initialize logging once before all tests
	logging.Init(&configuration.LoggingConfig{})

	// Run all tests
	code := m.Run()

	// Exit with the test result code
	os.Exit(code)
}

func TestNewTCPServer(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *configuration.NetworkConfig
		wantErr bool
	}{
		{
			name: "valid configuration",
			cfg: &configuration.NetworkConfig{
				Ip:                      "127.0.0.1",
				Port:                    "3223",
				MaxConnections:          10,
				MaxMessageSize:          1024,
				IdleTimeout:             30,
				GracefulShutdownTimeout: 5,
			},
			wantErr: false,
		},
		{
			name: "invalid port",
			cfg: &configuration.NetworkConfig{
				Ip:                      "127.0.0.1",
				Port:                    "99999",
				MaxConnections:          10,
				MaxMessageSize:          1024,
				IdleTimeout:             30,
				GracefulShutdownTimeout: 5,
			},
			wantErr: true,
		},
		{
			name: "invalid IP",
			cfg: &configuration.NetworkConfig{
				Ip:                      "invalid-ip",
				Port:                    "8080",
				MaxConnections:          10,
				MaxMessageSize:          1024,
				IdleTimeout:             30,
				GracefulShutdownTimeout: 5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewTCPServer(tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, server)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, server)
				assert.NotNil(t, server.listener)
				assert.NotNil(t, server.semaphore)
				assert.Equal(t, tt.cfg, server.cfg)

				// Clean up
				server.listener.Close()
			}
		})
	}
}

func TestTCPServer_BufferPool(t *testing.T) {
	cfg := &configuration.NetworkConfig{
		Ip:             "127.0.0.1",
		Port:           "3223",
		MaxConnections: 10,
		MaxMessageSize: 1024,
	}

	server, err := NewTCPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)
	defer server.listener.Close()

	// Test buffer pool functionality
	buffer1 := server.bufferPool.Get().(*[]byte)
	buffer2 := server.bufferPool.Get().(*[]byte)

	assert.Equal(t, cfg.MaxMessageSize+1, len(*buffer1))
	assert.Equal(t, cfg.MaxMessageSize+1, len(*buffer2))

	// Put buffers back
	server.bufferPool.Put(buffer1)
	server.bufferPool.Put(buffer2)
}

func TestTCPServer_HandleRequests_ContextCancellation(t *testing.T) {
	logging.Init(&configuration.LoggingConfig{})
	cfg := &configuration.NetworkConfig{
		Ip:                      "127.0.0.1",
		Port:                    "3223",
		MaxConnections:          5,
		MaxMessageSize:          1024,
		IdleTimeout:             1,
		GracefulShutdownTimeout: 1,
	}

	server, err := NewTCPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	ctx, cancel := context.WithCancel(context.Background())

	handler := func(ctx context.Context, data []byte) []byte {
		return []byte("response")
	}

	done := make(chan struct{})
	go func() {
		server.HandleRequests(ctx, handler)
		close(done)
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for graceful shutdown
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not shut down gracefully")
	}
}

func TestTCPServer_ConnectionLimit(t *testing.T) {
	cfg := &configuration.NetworkConfig{
		Ip:                      "127.0.0.1",
		Port:                    "0",
		MaxConnections:          2,
		MaxMessageSize:          1024,
		IdleTimeout:             5,
		GracefulShutdownTimeout: 1,
	}

	server, err := NewTCPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(ctx context.Context, data []byte) []byte {
		time.Sleep(100 * time.Millisecond) // Simulate processing time
		return []byte("response")
	}

	go server.HandleRequests(ctx, handler)

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Get the actual listening address
	addr := server.listener.Addr().String()

	// Create connections up to the limit
	var connections []net.Conn
	for i := 0; i < cfg.MaxConnections; i++ {
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		connections = append(connections, conn)
	}

	// Try to create one more connection (should be rejected)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Read the rejection message
	buffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	n, err := conn.Read(buffer)
	require.NoError(t, err)
	assert.Contains(t, string(buffer[:n]), "Connection limit exceeded")

	// Clean up connections
	for _, c := range connections {
		c.Close()
	}
}

func TestTCPServer_MessageSizeLimit(t *testing.T) {
	cfg := &configuration.NetworkConfig{
		Ip:                      "127.0.0.1",
		Port:                    "3223",
		MaxConnections:          5,
		MaxMessageSize:          10, // Small limit for testing
		IdleTimeout:             5,
		GracefulShutdownTimeout: 1,
	}

	server, err := NewTCPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(ctx context.Context, data []byte) []byte {
		return []byte("response")
	}

	go server.HandleRequests(ctx, handler)

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	addr := server.listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Send message within limit
	smallMessage := []byte("small")
	_, err = conn.Write(smallMessage)
	require.NoError(t, err)

	// Read response
	buffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	n, err := conn.Read(buffer)
	require.NoError(t, err)
	assert.Equal(t, "response", string(buffer[:n]))

	// Send message exceeding limit
	largeMessage := make([]byte, cfg.MaxMessageSize+1)
	for i := range largeMessage {
		largeMessage[i] = 'A'
	}

	_, err = conn.Write(largeMessage)
	require.NoError(t, err)

	// Connection should be closed due to size limit
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = conn.Read(buffer)
	assert.Error(t, err) // Should get EOF or connection closed error
}

func TestTCPServer_ConcurrentConnections(t *testing.T) {
	cfg := &configuration.NetworkConfig{
		Ip:                      "127.0.0.1",
		Port:                    "0",
		MaxConnections:          10,
		MaxMessageSize:          1024,
		IdleTimeout:             5,
		GracefulShutdownTimeout: 2,
	}

	server, err := NewTCPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(ctx context.Context, data []byte) []byte {
		return []byte("response")
	}

	go server.HandleRequests(ctx, handler)

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	addr := server.listener.Addr().String()

	// Test concurrent connections
	var wg sync.WaitGroup
	numConnections := 5

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("Failed to connect: %v", err)
				return
			}
			defer conn.Close()

			message := []byte("test message")
			_, err = conn.Write(message)
			if err != nil {
				t.Errorf("Failed to write: %v", err)
				return
			}

			buffer := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, err := conn.Read(buffer)
			if err != nil {
				t.Errorf("Failed to read: %v", err)
				return
			}

			if string(buffer[:n]) != "response" {
				t.Errorf("Unexpected response: %s", string(buffer[:n]))
			}
		}(i)
	}

	wg.Wait()
}
func TestTCPServer_GracefulShutdown(t *testing.T) {
	cfg := &configuration.NetworkConfig{
		Ip:                      "127.0.0.1",
		Port:                    "0",
		MaxConnections:          5,
		MaxMessageSize:          1024,
		IdleTimeout:             5,
		GracefulShutdownTimeout: 1,
	}

	server, err := NewTCPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	ctx, cancel := context.WithCancel(context.Background())

	handler := func(ctx context.Context, data []byte) []byte {
		return []byte("response")
	}

	shutdownComplete := make(chan struct{})
	go func() {
		server.HandleRequests(ctx, handler)
		close(shutdownComplete)
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Create a connection
	addr := server.listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	// Send a message
	_, err = conn.Write([]byte("test"))
	require.NoError(t, err)

	// Read response
	buffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = conn.Read(buffer)
	require.NoError(t, err)

	// Trigger shutdown
	cancel()

	// Wait for graceful shutdown
	select {
	case <-shutdownComplete:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Graceful shutdown took too long")
	}

	conn.Close()
}
