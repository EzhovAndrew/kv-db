package api

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetryIntegrationWithActualOperations tests retry behavior with real client operations
// This doesn't require a real server, but simulates the network layer for testing

func TestRetryIntegrationWithMockedNetwork(t *testing.T) {
	// Create a client with fast retries for testing
	config := DefaultConfig().
		WithRetryAttempts(3).
		WithRetryBaseDelay(10 * time.Millisecond).
		WithRetryMaxDelay(50 * time.Millisecond).
		WithRetryJitter(false)

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test context with reasonable timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// These operations will fail because there's no server, but we can verify
	// that they respect the retry configuration by measuring timing

	tests := []struct {
		name      string
		operation func() error
	}{
		{
			name: "Get operation with retries",
			operation: func() error {
				_, err := client.Get(ctx, "test-key")
				return err
			},
		},
		{
			name: "Set operation with retries",
			operation: func() error {
				return client.Set(ctx, "test-key", "test-value")
			},
		},
		{
			name: "Delete operation with retries",
			operation: func() error {
				return client.Delete(ctx, "test-key")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			err := tt.operation()
			duration := time.Since(start)

			// All operations should fail (no server)
			if err == nil {
				t.Error("Expected operation to fail without server")
			}

			// Should have taken time for retries (rough estimate)
			// 3 attempts = 1 immediate + 2 retries with 10ms, 20ms delays = ~30ms minimum
			minExpectedDuration := 20 * time.Millisecond
			if duration < minExpectedDuration {
				t.Errorf("Operation completed too quickly (%v), expected at least %v (indicating retries)",
					duration, minExpectedDuration)
			}

			// But shouldn't take too long (max 4 attempts * 50ms = 200ms buffer)
			maxExpectedDuration := 500 * time.Millisecond
			if duration > maxExpectedDuration {
				t.Errorf("Operation took too long (%v), expected less than %v",
					duration, maxExpectedDuration)
			}
		})
	}
}

func TestRetryWithContextTimeout(t *testing.T) {
	// Client with longer retries
	config := DefaultConfig().
		WithRetryAttempts(10).
		WithRetryBaseDelay(100 * time.Millisecond).
		WithRetryMaxDelay(200 * time.Millisecond).
		WithRetryJitter(false)

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Short context timeout should interrupt retries
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = client.Set(ctx, "test-key", "test-value")
	duration := time.Since(start)

	// Should fail with context deadline exceeded
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}

	// Should respect context timeout (allow some buffer for processing)
	maxExpectedDuration := 150 * time.Millisecond
	if duration > maxExpectedDuration {
		t.Errorf("Operation took %v, should have been interrupted by context timeout", duration)
	}
}

func TestRetryDisabled(t *testing.T) {
	// Client with retries disabled
	config := DefaultConfig().
		WithRetryAttempts(0)

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	start := time.Now()
	_, err = client.Get(ctx, "test-key")
	duration := time.Since(start)

	// Should fail quickly without retries
	if err == nil {
		t.Error("Expected operation to fail without server")
	}

	// Should complete quickly (no retries)
	maxExpectedDuration := 100 * time.Millisecond
	if duration > maxExpectedDuration {
		t.Errorf("Operation took %v, expected quick failure with no retries", duration)
	}
}

// TestRetryBehaviorConsistency ensures retry behavior is consistent across operations
func TestRetryBehaviorConsistency(t *testing.T) {
	config := DefaultConfig().
		WithRetryAttempts(2).
		WithRetryBaseDelay(20 * time.Millisecond).
		WithRetryMaxDelay(100 * time.Millisecond).
		WithRetryJitter(false)

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	operations := []struct {
		name string
		fn   func() error
	}{
		{"Get", func() error { _, err := client.Get(ctx, "key"); return err }},
		{"GetBytes", func() error { _, err := client.GetBytes(ctx, "key"); return err }},
		{"Set", func() error { return client.Set(ctx, "key", "value") }},
		{"SetBytes", func() error { return client.SetBytes(ctx, "key", []byte("value")) }},
		{"Delete", func() error { return client.Delete(ctx, "key") }},
	}

	var durations []time.Duration

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			start := time.Now()
			err := op.fn()
			duration := time.Since(start)
			durations = append(durations, duration)

			if err == nil {
				t.Error("Expected operation to fail without server")
			}

			// All operations should take roughly the same time due to consistent retry logic
			// Expected: immediate + 20ms + 40ms = ~60ms minimum
			minExpected := 50 * time.Millisecond
			maxExpected := 200 * time.Millisecond

			if duration < minExpected || duration > maxExpected {
				t.Errorf("Operation %s took %v, expected between %v and %v",
					op.name, duration, minExpected, maxExpected)
			}
		})
	}

	// Verify all operations took roughly similar time (within 50ms of each other)
	if len(durations) > 1 {
		min, max := durations[0], durations[0]
		for _, d := range durations {
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}

		tolerance := 50 * time.Millisecond
		if max-min > tolerance {
			t.Errorf("Operations have inconsistent timing: min=%v, max=%v, diff=%v (tolerance=%v)",
				min, max, max-min, tolerance)
		}
	}
}

// Mock server for controlled testing
type MockServer struct {
	listener     net.Listener
	responseFunc func([]byte) []byte
	closed       atomic.Bool
}

func NewMockServer(responseFunc func([]byte) []byte) (*MockServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0") // Random available port
	if err != nil {
		return nil, err
	}

	server := &MockServer{
		listener:     listener,
		responseFunc: responseFunc,
	}

	go server.serve()
	return server, nil
}

func (s *MockServer) serve() {
	for {
		if s.closed.Load() {
			return
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *MockServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	response := s.responseFunc(buffer[:n])
	conn.Write(response)
}

func (s *MockServer) Close() error {
	s.closed.Store(true)
	return s.listener.Close()
}

func (s *MockServer) Port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func TestRetrySuccessAfterFailures(t *testing.T) {
	var attemptCount atomic.Int32

	// Server that fails first 2 attempts, then succeeds
	server, err := NewMockServer(func(request []byte) []byte {
		attempt := attemptCount.Add(1)
		if attempt <= 2 {
			// Simulate network error by closing connection
			return nil // Will cause read/write error
		}
		return []byte("test-value") // Success on 3rd attempt
	})
	if err != nil {
		t.Fatalf("Failed to create mock server: %v", err)
	}
	defer server.Close()

	config := DefaultConfig().
		WithHost("127.0.0.1").
		WithPort(server.Port()).
		WithRetryAttempts(3).
		WithRetryBaseDelay(10 * time.Millisecond).
		WithRetryMaxDelay(50 * time.Millisecond).
		WithRetryJitter(false)

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This should succeed on the 3rd attempt
	start := time.Now()
	value, err := client.Get(ctx, "test-key")
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Expected success after retries, got error: %v", err)
	}

	if value != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", value)
	}

	// Should have made exactly 3 attempts
	if attempts := attemptCount.Load(); attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	// Should have taken time for 2 retries
	minExpected := 20 * time.Millisecond // 10ms + 20ms delays
	if duration < minExpected {
		t.Errorf("Operation completed too quickly (%v), expected at least %v for retries",
			duration, minExpected)
	}
}
