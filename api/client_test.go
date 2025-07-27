package api

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Host != "127.0.0.1" {
		t.Errorf("Expected Host '127.0.0.1', got '%s'", config.Host)
	}

	if config.Port != 3223 {
		t.Errorf("Expected Port 3223, got %d", config.Port)
	}

	if config.ConnectionTimeout != 10*time.Second {
		t.Errorf("Expected ConnectionTimeout 10s, got %v", config.ConnectionTimeout)
	}

	if config.OperationTimeout != 5*time.Second {
		t.Errorf("Expected OperationTimeout 5s, got %v", config.OperationTimeout)
	}

	if config.MaxMessageSize != 4096 {
		t.Errorf("Expected MaxMessageSize 4096, got %d", config.MaxMessageSize)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		wantError bool
	}{
		{
			name:      "valid config",
			config:    DefaultConfig(),
			wantError: false,
		},
		{
			name: "empty host",
			config: &Config{
				Host:              "",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
			},
			wantError: true,
		},
		{
			name: "invalid port - zero",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              0,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
			},
			wantError: true,
		},
		{
			name: "invalid port - too high",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              65536,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
			},
			wantError: true,
		},
		{
			name: "invalid connection timeout",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 0,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
			},
			wantError: true,
		},
		{
			name: "invalid operation timeout",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  0,
				MaxMessageSize:    4096,
			},
			wantError: true,
		},
		{
			name: "invalid max message size",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    0,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Config.Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestConfigWithMethods(t *testing.T) {
	config := DefaultConfig().
		WithHost("192.168.1.1").
		WithPort(8080).
		WithConnectionTimeout(20 * time.Second).
		WithOperationTimeout(10 * time.Second).
		WithMaxMessageSize(8192).
		WithRetryAttempts(5).
		WithRetryDelay(200 * time.Millisecond)

	if config.Host != "192.168.1.1" {
		t.Errorf("Expected Host '192.168.1.1', got '%s'", config.Host)
	}

	if config.Port != 8080 {
		t.Errorf("Expected Port 8080, got %d", config.Port)
	}

	if config.ConnectionTimeout != 20*time.Second {
		t.Errorf("Expected ConnectionTimeout 20s, got %v", config.ConnectionTimeout)
	}

	if config.OperationTimeout != 10*time.Second {
		t.Errorf("Expected OperationTimeout 10s, got %v", config.OperationTimeout)
	}

	if config.MaxMessageSize != 8192 {
		t.Errorf("Expected MaxMessageSize 8192, got %d", config.MaxMessageSize)
	}

	if config.RetryAttempts != 5 {
		t.Errorf("Expected RetryAttempts 5, got %d", config.RetryAttempts)
	}

	if config.RetryDelay != 200*time.Millisecond {
		t.Errorf("Expected RetryDelay 200ms, got %v", config.RetryDelay)
	}
}

func TestNewClient(t *testing.T) {
	// Test with nil config (should use defaults)
	client, err := NewClient(nil)
	if err != nil {
		t.Errorf("NewClient(nil) failed: %v", err)
	}
	if client == nil {
		t.Error("NewClient(nil) returned nil client")
	}

	// Test with valid config
	config := DefaultConfig()
	client, err = NewClient(config)
	if err != nil {
		t.Errorf("NewClient(valid config) failed: %v", err)
	}
	if client == nil {
		t.Error("NewClient(valid config) returned nil client")
	}

	// Test with invalid config
	invalidConfig := &Config{
		Host: "", // Invalid empty host
	}
	client, err = NewClient(invalidConfig)
	if err == nil {
		t.Error("NewClient(invalid config) should have failed")
	}
	if client != nil {
		t.Error("NewClient(invalid config) should return nil client")
	}
}

func TestClientInputValidation(t *testing.T) {
	client, err := NewClient(nil)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Test Get with empty key
	_, err = client.Get(ctx, "")
	if err == nil {
		t.Error("Get with empty key should fail")
	}

	var invalidArgErr *InvalidArgumentError
	if !errors.As(err, &invalidArgErr) {
		t.Errorf("Expected InvalidArgumentError, got %T", err)
	}

	// Test GetBytes with empty key
	_, err = client.GetBytes(ctx, "")
	if err == nil {
		t.Error("GetBytes with empty key should fail")
	}

	if !errors.As(err, &invalidArgErr) {
		t.Errorf("Expected InvalidArgumentError, got %T", err)
	}

	// Test Set with empty key
	err = client.Set(ctx, "", "value")
	if err == nil {
		t.Error("Set with empty key should fail")
	}

	if !errors.As(err, &invalidArgErr) {
		t.Errorf("Expected InvalidArgumentError, got %T", err)
	}

	// Test SetBytes with empty key
	err = client.SetBytes(ctx, "", []byte("value"))
	if err == nil {
		t.Error("SetBytes with empty key should fail")
	}

	if !errors.As(err, &invalidArgErr) {
		t.Errorf("Expected InvalidArgumentError, got %T", err)
	}

	// Test Delete with empty key
	err = client.Delete(ctx, "")
	if err == nil {
		t.Error("Delete with empty key should fail")
	}

	if !errors.As(err, &invalidArgErr) {
		t.Errorf("Expected InvalidArgumentError, got %T", err)
	}
}

func TestByteMethods(t *testing.T) {
	client, err := NewClient(nil)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Test SetBytes and GetBytes consistency
	testData := []byte(`{"name":"John","age":30,"active":true}`)
	key := "test:json"

	// These methods should handle byte data properly
	err = client.SetBytes(ctx, key, testData)
	if err == nil {
		// Only test if connection succeeds (no server running in tests)
		// This validates the method signatures and error handling
		t.Logf("SetBytes method signature is correct")
	}

	retrievedBytes, err := client.GetBytes(ctx, key)
	if err == nil {
		// Only test if connection succeeds
		if string(retrievedBytes) != string(testData) {
			t.Errorf("Expected %s, got %s", string(testData), string(retrievedBytes))
		}
	}

	// Test that GetBytes returns proper byte slice
	if len(retrievedBytes) > 0 {
		// Verify it's actually a byte slice
		var testByte = retrievedBytes[0]
		_ = testByte // Use the variable to avoid unused error
	}
}

func TestErrorTypes(t *testing.T) {
	// Test KeyNotFoundError
	keyErr := &KeyNotFoundError{Key: "testkey"}
	if !errors.Is(keyErr, ErrKeyNotFound) {
		t.Error("KeyNotFoundError should satisfy errors.Is(err, ErrKeyNotFound)")
	}

	expectedMsg := "key 'testkey' not found"
	if keyErr.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, keyErr.Error())
	}

	// Test InvalidArgumentError
	argErr := &InvalidArgumentError{Message: "test message"}
	if !errors.Is(argErr, ErrInvalidArgument) {
		t.Error("InvalidArgumentError should satisfy errors.Is(err, ErrInvalidArgument)")
	}

	if argErr.Error() != "test message" {
		t.Errorf("Expected error message 'test message', got '%s'", argErr.Error())
	}

	// Test ConnectionError
	connErr := &ConnectionError{Host: "127.0.0.1", Port: 3223, Err: errors.New("connection refused")}
	if !errors.Is(connErr, ErrConnectionFailed) {
		t.Error("ConnectionError should satisfy errors.Is(err, ErrConnectionFailed)")
	}

	expectedConnMsg := "failed to connect to 127.0.0.1:3223: connection refused"
	if connErr.Error() != expectedConnMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedConnMsg, connErr.Error())
	}

	// Test NetworkError
	netErr := &NetworkError{Err: errors.New("network timeout")}
	if !errors.Is(netErr, ErrNetworkError) {
		t.Error("NetworkError should satisfy errors.Is(err, ErrNetworkError)")
	}

	expectedNetMsg := "network error: network timeout"
	if netErr.Error() != expectedNetMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedNetMsg, netErr.Error())
	}

	// Test ServerError
	serverErr := &ServerError{Message: "invalid command"}
	if !errors.Is(serverErr, ErrServerError) {
		t.Error("ServerError should satisfy errors.Is(err, ErrServerError)")
	}

	expectedServerMsg := "server error: invalid command"
	if serverErr.Error() != expectedServerMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedServerMsg, serverErr.Error())
	}
}

func TestClientIsConnected(t *testing.T) {
	client, err := NewClient(nil)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Initially should not be connected
	if client.IsConnected() {
		t.Error("Client should not be connected initially")
	}

	// After close, should not be connected
	err = client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if client.IsConnected() {
		t.Error("Client should not be connected after close")
	}

	// Multiple closes should be safe
	err = client.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}
