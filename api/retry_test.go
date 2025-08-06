package api

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// ===============================================
// RETRY FUNCTIONALITY TESTS
// ===============================================

func TestDefaultConfigRetrySettings(t *testing.T) {
	config := DefaultConfig()

	// Test retry configuration defaults
	if config.RetryAttempts != 3 {
		t.Errorf("Expected RetryAttempts 3, got %d", config.RetryAttempts)
	}

	if config.RetryBaseDelay != 100*time.Millisecond {
		t.Errorf("Expected RetryBaseDelay 100ms, got %v", config.RetryBaseDelay)
	}

	if config.RetryMaxDelay != 5*time.Second {
		t.Errorf("Expected RetryMaxDelay 5s, got %v", config.RetryMaxDelay)
	}

	if !config.RetryJitter {
		t.Errorf("Expected RetryJitter true, got %v", config.RetryJitter)
	}
}

func TestConfigValidationRetrySettings(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		wantError bool
	}{
		{
			name: "valid retry config",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
				RetryAttempts:     3,
				RetryBaseDelay:    100 * time.Millisecond,
				RetryMaxDelay:     5 * time.Second,
				RetryJitter:       true,
			},
			wantError: false,
		},
		{
			name: "negative retry base delay",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
				RetryAttempts:     3,
				RetryBaseDelay:    -100 * time.Millisecond,
				RetryMaxDelay:     5 * time.Second,
				RetryJitter:       true,
			},
			wantError: true,
		},
		{
			name: "negative retry max delay",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
				RetryAttempts:     3,
				RetryBaseDelay:    100 * time.Millisecond,
				RetryMaxDelay:     -5 * time.Second,
				RetryJitter:       true,
			},
			wantError: true,
		},
		{
			name: "base delay greater than max delay",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
				RetryAttempts:     3,
				RetryBaseDelay:    10 * time.Second,
				RetryMaxDelay:     5 * time.Second,
				RetryJitter:       true,
			},
			wantError: true,
		},
		{
			name: "negative retry attempts",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
				RetryAttempts:     -1,
				RetryBaseDelay:    100 * time.Millisecond,
				RetryMaxDelay:     5 * time.Second,
				RetryJitter:       true,
			},
			wantError: true,
		},
		{
			name: "zero retry attempts (disabled retries)",
			config: &Config{
				Host:              "127.0.0.1",
				Port:              3223,
				ConnectionTimeout: 10 * time.Second,
				OperationTimeout:  5 * time.Second,
				MaxMessageSize:    4096,
				RetryAttempts:     0,
				RetryBaseDelay:    100 * time.Millisecond,
				RetryMaxDelay:     5 * time.Second,
				RetryJitter:       true,
			},
			wantError: false,
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

func TestConfigFluentRetryMethods(t *testing.T) {
	config := DefaultConfig().
		WithRetryAttempts(5).
		WithRetryBaseDelay(200 * time.Millisecond).
		WithRetryMaxDelay(10 * time.Second).
		WithRetryJitter(false)

	if config.RetryAttempts != 5 {
		t.Errorf("Expected RetryAttempts 5, got %d", config.RetryAttempts)
	}

	if config.RetryBaseDelay != 200*time.Millisecond {
		t.Errorf("Expected RetryBaseDelay 200ms, got %v", config.RetryBaseDelay)
	}

	if config.RetryMaxDelay != 10*time.Second {
		t.Errorf("Expected RetryMaxDelay 10s, got %v", config.RetryMaxDelay)
	}

	if config.RetryJitter {
		t.Errorf("Expected RetryJitter false, got %v", config.RetryJitter)
	}
}

func TestIsRetryableError(t *testing.T) {
	client, err := NewClient(DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		{
			name:      "network error",
			err:       &NetworkError{Err: errors.New("connection refused")},
			retryable: true,
		},
		{
			name:      "connection error",
			err:       &ConnectionError{Host: "localhost", Port: 3223, Err: errors.New("timeout")},
			retryable: true,
		},
		{
			name:      "server error",
			err:       &ServerError{Message: "internal server error"},
			retryable: false,
		},
		{
			name:      "key not found error",
			err:       &KeyNotFoundError{Key: "test"},
			retryable: false,
		},
		{
			name:      "invalid argument error",
			err:       &InvalidArgumentError{Message: "empty key"},
			retryable: false,
		},
		{
			name:      "context deadline exceeded",
			err:       context.DeadlineExceeded,
			retryable: false,
		},
		{
			name:      "context canceled",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "wrapped context deadline exceeded",
			err:       errors.Join(errors.New("operation failed"), context.DeadlineExceeded),
			retryable: false,
		},
		{
			name:      "wrapped network error",
			err:       errors.Join(errors.New("middleware error"), &NetworkError{Err: errors.New("connection failed")}),
			retryable: true,
		},
		{
			name:      "unknown error",
			err:       errors.New("some unknown error"),
			retryable: true, // Conservative approach - retry unknown errors
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.isRetryableError(tt.err)
			if result != tt.retryable {
				t.Errorf("isRetryableError(%v) = %v, want %v", tt.err, result, tt.retryable)
			}
		})
	}
}

func TestCalculateRetryDelay(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		attempt     int
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name: "attempt 0 with 100ms base",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 100 * time.Millisecond,
				RetryMaxDelay:  5 * time.Second,
				RetryJitter:    false,
			},
			attempt:     0,
			expectedMin: 100 * time.Millisecond,
			expectedMax: 100 * time.Millisecond,
		},
		{
			name: "attempt 1 with 100ms base",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 100 * time.Millisecond,
				RetryMaxDelay:  5 * time.Second,
				RetryJitter:    false,
			},
			attempt:     1,
			expectedMin: 200 * time.Millisecond,
			expectedMax: 200 * time.Millisecond,
		},
		{
			name: "attempt 2 with 100ms base",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 100 * time.Millisecond,
				RetryMaxDelay:  5 * time.Second,
				RetryJitter:    false,
			},
			attempt:     2,
			expectedMin: 400 * time.Millisecond,
			expectedMax: 400 * time.Millisecond,
		},
		{
			name: "attempt 3 with max delay cap",
			config: &Config{
				RetryAttempts:  5,
				RetryBaseDelay: 100 * time.Millisecond,
				RetryMaxDelay:  500 * time.Millisecond,
				RetryJitter:    false,
			},
			attempt:     3,
			expectedMin: 500 * time.Millisecond, // Capped at max
			expectedMax: 500 * time.Millisecond,
		},
		{
			name: "with jitter enabled",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 100 * time.Millisecond,
				RetryMaxDelay:  5 * time.Second,
				RetryJitter:    true,
			},
			attempt:     1,
			expectedMin: 150 * time.Millisecond, // 200ms - 25% = 150ms
			expectedMax: 250 * time.Millisecond, // 200ms + 25% = 250ms
		},
		{
			name: "retries disabled",
			config: &Config{
				RetryAttempts:  0,
				RetryBaseDelay: 100 * time.Millisecond,
				RetryMaxDelay:  5 * time.Second,
				RetryJitter:    false,
			},
			attempt:     1,
			expectedMin: 0,
			expectedMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{config: tt.config}
			delay := client.calculateRetryDelay(tt.attempt)

			if delay < tt.expectedMin || delay > tt.expectedMax {
				t.Errorf("calculateRetryDelay(attempt=%d) = %v, want between %v and %v",
					tt.attempt, delay, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestCalculateJitter(t *testing.T) {
	client := &Client{config: DefaultConfig()}

	tests := []struct {
		name        string
		delay       time.Duration
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "zero delay",
			delay:       0,
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:        "100ms delay",
			delay:       100 * time.Millisecond,
			expectedMin: -25 * time.Millisecond, // -25%
			expectedMax: 25 * time.Millisecond,  // +25%
		},
		{
			name:        "1 second delay",
			delay:       1 * time.Second,
			expectedMin: -250 * time.Millisecond, // -25%
			expectedMax: 250 * time.Millisecond,  // +25%
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test multiple times since jitter is random
			for range 10 {
				jitter := client.calculateJitter(tt.delay)
				if jitter < tt.expectedMin || jitter > tt.expectedMax {
					t.Errorf("calculateJitter(%v) = %v, want between %v and %v",
						tt.delay, jitter, tt.expectedMin, tt.expectedMax)
				}
			}
		})
	}
}

// Mock implementations for testing retry behavior with controlled failures
type MockConnection struct {
	maxFailures    int
	errorToReturn  error
	operationCount int
}

func (m *MockConnection) SimulateOperation() error {
	m.operationCount++
	if m.operationCount <= m.maxFailures {
		return m.errorToReturn
	}
	return nil // Success after max failures
}

func TestRetryOperationBehavior(t *testing.T) {
	tests := []struct {
		name             string
		config           *Config
		mockError        error
		maxFailures      int
		expectSuccess    bool
		expectedAttempts int
	}{
		{
			name: "succeed on first attempt",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 1 * time.Millisecond, // Fast for testing
				RetryMaxDelay:  5 * time.Millisecond,
				RetryJitter:    false,
			},
			mockError:        &NetworkError{Err: errors.New("network error")},
			maxFailures:      0, // Succeed immediately
			expectSuccess:    true,
			expectedAttempts: 1,
		},
		{
			name: "succeed on second attempt",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 1 * time.Millisecond,
				RetryMaxDelay:  5 * time.Millisecond,
				RetryJitter:    false,
			},
			mockError:        &NetworkError{Err: errors.New("network error")},
			maxFailures:      1, // Fail first, succeed second
			expectSuccess:    true,
			expectedAttempts: 2,
		},
		{
			name: "exhaust all retries",
			config: &Config{
				RetryAttempts:  2,
				RetryBaseDelay: 1 * time.Millisecond,
				RetryMaxDelay:  5 * time.Millisecond,
				RetryJitter:    false,
			},
			mockError:        &NetworkError{Err: errors.New("network error")},
			maxFailures:      10, // Always fail
			expectSuccess:    false,
			expectedAttempts: 3, // 1 initial + 2 retries
		},
		{
			name: "don't retry non-retryable error",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 1 * time.Millisecond,
				RetryMaxDelay:  5 * time.Millisecond,
				RetryJitter:    false,
			},
			mockError:        &KeyNotFoundError{Key: "test"},
			maxFailures:      10, // Always fail
			expectSuccess:    false,
			expectedAttempts: 1, // No retries for non-retryable errors
		},
		{
			name: "don't retry context deadline exceeded",
			config: &Config{
				RetryAttempts:  3,
				RetryBaseDelay: 1 * time.Millisecond,
				RetryMaxDelay:  5 * time.Millisecond,
				RetryJitter:    false,
			},
			mockError:        context.DeadlineExceeded,
			maxFailures:      10, // Always fail
			expectSuccess:    false,
			expectedAttempts: 1, // No retries for context errors
		},
		{
			name: "retries disabled",
			config: &Config{
				RetryAttempts:  0, // Disabled
				RetryBaseDelay: 1 * time.Millisecond,
				RetryMaxDelay:  5 * time.Millisecond,
				RetryJitter:    false,
			},
			mockError:        &NetworkError{Err: errors.New("network error")},
			maxFailures:      10, // Always fail
			expectSuccess:    false,
			expectedAttempts: 1, // No retries when disabled
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{config: tt.config}
			mock := &MockConnection{
				errorToReturn: tt.mockError,
				maxFailures:   tt.maxFailures,
			}

			// Test the retry operation wrapper
			err := client.retryOperation(context.Background(), func() error {
				return mock.SimulateOperation()
			})

			// Check if success/failure matches expectation
			success := (err == nil)
			if success != tt.expectSuccess {
				t.Errorf("Expected success=%v, got success=%v (err=%v)",
					tt.expectSuccess, success, err)
			}

			// Check attempt count
			if mock.operationCount != tt.expectedAttempts {
				t.Errorf("Expected %d attempts, got %d attempts",
					tt.expectedAttempts, mock.operationCount)
			}
		})
	}
}

func TestRetryOperationContextCancellation(t *testing.T) {
	config := &Config{
		RetryAttempts:  5,
		RetryBaseDelay: 50 * time.Millisecond,
		RetryMaxDelay:  200 * time.Millisecond,
		RetryJitter:    false,
	}

	client := &Client{config: config}

	// Create a context that will be canceled after a short time
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	start := time.Now()
	attemptCount := 0

	err := client.retryOperation(ctx, func() error {
		attemptCount++
		return &NetworkError{Err: errors.New("network error")} // Always fail
	})

	duration := time.Since(start)

	// Should return context.DeadlineExceeded
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}

	// Should not take much longer than the context timeout
	if duration > 100*time.Millisecond {
		t.Errorf("Operation took too long: %v", duration)
	}

	// Should have attempted at least once, but not all retries
	if attemptCount == 0 {
		t.Error("Expected at least one attempt")
	}
	if attemptCount > 3 {
		t.Errorf("Too many attempts for cancelled context: %d", attemptCount)
	}
}

func TestExponentialBackoffMath(t *testing.T) {
	client := &Client{
		config: &Config{
			RetryAttempts:  5,
			RetryBaseDelay: 100 * time.Millisecond,
			RetryMaxDelay:  0, // No max to test pure exponential
			RetryJitter:    false,
		},
	}

	expectedDelays := []time.Duration{
		100 * time.Millisecond, // 100ms << 0 = 100ms
		200 * time.Millisecond, // 100ms << 1 = 200ms
		400 * time.Millisecond, // 100ms << 2 = 400ms
		800 * time.Millisecond, // 100ms << 3 = 800ms
	}

	for attempt, expected := range expectedDelays {
		actual := client.calculateRetryDelay(attempt)
		if actual != expected {
			t.Errorf("Attempt %d: expected %v, got %v", attempt, expected, actual)
		}
	}
}

func TestIsConnectionBroken(t *testing.T) {
	client := &Client{config: DefaultConfig()}

	tests := []struct {
		name             string
		err              error
		shouldDisconnect bool
	}{
		{
			name:             "nil error",
			err:              nil,
			shouldDisconnect: false,
		},
		{
			name:             "EOF - connection closed",
			err:              io.EOF,
			shouldDisconnect: true,
		},
		{
			name:             "unexpected EOF",
			err:              io.ErrUnexpectedEOF,
			shouldDisconnect: true,
		},
		{
			name:             "connection reset by peer",
			err:              errors.New("read tcp 127.0.0.1:3223: connection reset by peer"),
			shouldDisconnect: true,
		},
		{
			name:             "broken pipe",
			err:              errors.New("write tcp 127.0.0.1:3223: broken pipe"),
			shouldDisconnect: true,
		},
		{
			name:             "connection refused",
			err:              errors.New("dial tcp 127.0.0.1:3223: connection refused"),
			shouldDisconnect: true,
		},
		{
			name:             "network unreachable",
			err:              errors.New("dial tcp 192.168.1.100:3223: network is unreachable"),
			shouldDisconnect: true,
		},
		{
			name:             "context deadline exceeded",
			err:              context.DeadlineExceeded,
			shouldDisconnect: false,
		},
		{
			name:             "context canceled",
			err:              context.Canceled,
			shouldDisconnect: false,
		},
		{
			name:             "timeout error",
			err:              errors.New("read tcp 127.0.0.1:3223: i/o timeout"),
			shouldDisconnect: false,
		},
		{
			name:             "deadline exceeded in error message",
			err:              errors.New("operation deadline exceeded"),
			shouldDisconnect: false,
		},
		{
			name:             "unknown error",
			err:              errors.New("some random network error"),
			shouldDisconnect: false, // Conservative approach
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.isConnectionBroken(tt.err)
			if result != tt.shouldDisconnect {
				t.Errorf("isConnectionBroken(%v) = %v, want %v", tt.err, result, tt.shouldDisconnect)
			}
		})
	}
}
