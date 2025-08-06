// Package api provides a simple and intuitive client library for accessing kv-db.
// It handles connection management, command formatting, and response parsing automatically.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"
)

// Client represents a connection to the kv-db database.
// It provides a simple interface for performing GET, SET, and DELETE operations.
type Client struct {
	config *Config
	conn   net.Conn
	mutex  sync.RWMutex
}

// NewClient creates a new kv-db client with the provided configuration.
// If config is nil, default configuration will be used.
//
// Example:
//
//	client, err := api.NewClient(nil) // Use defaults
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	client := &Client{
		config: config,
	}

	return client, nil
}

// Connect establishes a connection to the kv-db server.
// This method is called automatically by Get, Set, and Delete if not already connected.
func (c *Client) Connect(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		return nil // Already connected
	}

	address := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)

	dialer := &net.Dialer{
		Timeout: c.config.ConnectionTimeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return &ConnectionError{
			Host: c.config.Host,
			Port: c.config.Port,
			Err:  err,
		}
	}

	c.conn = conn
	return nil
}

// Get retrieves the value associated with the given key as a string.
// This method converts the result from GetBytes to a string.
// For better performance with binary data or JSON, use GetBytes instead.
//
// The operation automatically retries on transient failures (network errors, timeouts)
// using exponential backoff with jitter. Retry behavior is controlled by the client configuration.
//
// Returns the value and nil error if successful.
// Returns empty string and KeyNotFoundError if the key doesn't exist.
// Returns empty string and other error types for connection or server errors.
//
// Example:
//
//	value, err := client.Get(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, api.ErrKeyNotFound) {
//	        fmt.Println("Key not found")
//	    } else {
//	        log.Printf("Error: %v", err)
//	    }
//	    return
//	}
//	fmt.Printf("Value: %s\n", value)
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	data, err := c.GetBytes(ctx, key)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// GetBytes retrieves the value associated with the given key as a byte slice.
// This is the core implementation that works directly with bytes to avoid conversions.
// More efficient than Get when working with binary data or when you plan to unmarshal JSON.
//
// The operation automatically retries on transient failures (network errors)
// using exponential backoff with jitter. Retry behavior is controlled by the client configuration.
//
// Returns the value as bytes and nil error if successful.
// Returns nil and KeyNotFoundError if the key doesn't exist.
// Returns nil and other error types for connection or server errors.
//
// Example:
//
//	data, err := client.GetBytes(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, api.ErrKeyNotFound) {
//	        fmt.Println("Key not found")
//	    } else {
//	        log.Printf("Error: %v", err)
//	    }
//	    return
//	}
//
//	var user User
//	err = json.Unmarshal(data, &user)
//	if err != nil {
//	    log.Printf("Failed to unmarshal: %v", err)
//	    return
//	}
func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, &InvalidArgumentError{Message: "key cannot be empty"}
	}

	var result []byte
	err := c.retryOperation(ctx, func() error {
		var err error
		result, err = c.performGet(ctx, key)
		return err
	})

	return result, err
}

// Set stores a key-value pair in the database using a string value.
// This method converts the string value to bytes and calls SetBytes.
// For better performance with binary data or JSON, use SetBytes instead.
//
// The operation automatically retries on transient failures (network errors)
// using exponential backoff with jitter. Retry behavior is controlled by the client configuration.
//
// Returns nil on success, error on failure.
//
// Example:
//
//	err := client.Set(ctx, "user:123", "John Doe")
//	if err != nil {
//	    log.Printf("Failed to set value: %v", err)
//	    return
//	}
//	fmt.Println("Value set successfully")
func (c *Client) Set(ctx context.Context, key, value string) error {
	return c.SetBytes(ctx, key, []byte(value))
}

// SetBytes stores a key-value pair in the database using a byte slice as the value.
// This is the core implementation that works directly with bytes to avoid conversions.
// More efficient than Set when working with binary data or marshaled JSON.
//
// The operation automatically retries on transient failures (network errors)
// using exponential backoff with jitter. Retry behavior is controlled by the client configuration.
//
// Returns nil on success, error on failure.
//
// Example:
//
//	user := User{Name: "John", Age: 30}
//	data, err := json.Marshal(user)
//	if err != nil {
//	    log.Printf("Failed to marshal: %v", err)
//	    return
//	}
//
//	err = client.SetBytes(ctx, "user:123", data)
//	if err != nil {
//	    log.Printf("Failed to set value: %v", err)
//	    return
//	}
//	fmt.Println("Value set successfully")
func (c *Client) SetBytes(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return &InvalidArgumentError{Message: "key cannot be empty"}
	}

	return c.retryOperation(ctx, func() error {
		return c.performSet(ctx, key, value)
	})
}

// Delete removes a key-value pair from the database.
//
// The operation automatically retries on transient failures (network errors)
// using exponential backoff with jitter. Retry behavior is controlled by the client configuration.
//
// Returns nil on success, error on failure.
// Note: Deleting a non-existent key is considered successful and returns nil.
//
// Example:
//
//	err := client.Delete(ctx, "user:123")
//	if err != nil {
//	    log.Printf("Failed to delete key: %v", err)
//	    return
//	}
//	fmt.Println("Key deleted successfully")
func (c *Client) Delete(ctx context.Context, key string) error {
	if key == "" {
		return &InvalidArgumentError{Message: "key cannot be empty"}
	}

	return c.retryOperation(ctx, func() error {
		return c.performDelete(ctx, key)
	})
}

// Ping checks if the connection to the server is alive.
// This is a convenience method that attempts a GET on a non-existent key.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.GetBytes(ctx, "__ping__")
	if err != nil {
		// KeyNotFoundError is expected and means the connection is working
		if _, ok := err.(*KeyNotFoundError); ok {
			return nil
		}
		return err
	}
	return nil
}

// Close closes the connection to the database.
// It's safe to call Close multiple times.
func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	return err
}

// IsConnected returns true if the client is currently connected to the server.
func (c *Client) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.conn != nil
}

// ensureConnected makes sure we have an active connection
func (c *Client) ensureConnected(ctx context.Context) error {
	if !c.IsConnected() {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}
	return nil
}

// ===============================================
// CORE OPERATION IMPLEMENTATIONS
// ===============================================
// These functions contain the actual database operation logic,
// separated from retry handling for better readability.

// performGet executes a single GET operation without retry logic.
func (c *Client) performGet(ctx context.Context, key string) ([]byte, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	// Build binary protocol command
	cmd := EncodeBinaryGet([]byte(key))

	response, err := c.sendCommandBytes(ctx, cmd)
	if err != nil {
		return nil, err
	}

	// Handle key not found
	if bytes.Contains(response, []byte("key not found")) || bytes.Contains(response, []byte("not found")) {
		return nil, &KeyNotFoundError{Key: key}
	}

	return response, nil
}

// performSet executes a single SET operation without retry logic.
func (c *Client) performSet(ctx context.Context, key string, value []byte) error {
	if err := c.ensureConnected(ctx); err != nil {
		return err
	}

	// Build binary protocol command - binary protocol supports all values including whitespace
	cmd := EncodeBinarySet([]byte(key), value)

	response, err := c.sendCommandBytes(ctx, cmd)
	if err != nil {
		return err
	}

	// Check response - convert only for comparison
	responseStr := string(response)
	if responseStr != "OK" {
		return &ServerError{Message: responseStr}
	}

	return nil
}

// performDelete executes a single DELETE operation without retry logic.
func (c *Client) performDelete(ctx context.Context, key string) error {
	if err := c.ensureConnected(ctx); err != nil {
		return err
	}

	// Build binary protocol command
	cmd := EncodeBinaryDel([]byte(key))

	response, err := c.sendCommandBytes(ctx, cmd)
	if err != nil {
		return err
	}

	// Check response - convert only for comparison
	responseStr := string(response)
	if responseStr != "OK" {
		return &ServerError{Message: responseStr}
	}

	return nil
}

// sendCommandBytes sends a command as bytes to the server and returns the response as bytes.
// This is the core method that works entirely with bytes to avoid string conversions.
func (c *Client) sendCommandBytes(ctx context.Context, command []byte) ([]byte, error) {
	c.mutex.RLock()
	conn := c.conn
	c.mutex.RUnlock()

	if conn == nil {
		return nil, &ConnectionError{
			Host: c.config.Host,
			Port: c.config.Port,
			Err:  fmt.Errorf("not connected"),
		}
	}

	// Set deadline for the operation
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("failed to set deadline: %w", err)
		}
	} else {
		// Use config timeout as fallback
		if err := conn.SetDeadline(time.Now().Add(c.config.OperationTimeout)); err != nil {
			return nil, fmt.Errorf("failed to set deadline: %w", err)
		}
	}

	_, err := conn.Write(command)
	if err != nil {
		if c.isConnectionBroken(err) {
			c.disconnect()
		}
		return nil, &NetworkError{Err: err}
	}

	// Read response as bytes
	buffer := make([]byte, c.config.MaxMessageSize)
	n, err := conn.Read(buffer)
	if err != nil {
		if c.isConnectionBroken(err) {
			c.disconnect()
		}
		return nil, &NetworkError{Err: err}
	}

	// Return raw bytes, trimming trailing whitespace
	response := bytes.TrimSpace(buffer[:n])
	return response, nil
}

// isConnectionBroken determines if an error indicates the connection is actually broken
// and should be closed, versus a transient error that might resolve on retry.
func (c *Client) isConnectionBroken(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific errors that indicate broken connection
	errorStr := err.Error()

	// Connection definitively broken
	if errors.Is(err, io.EOF) || // Server closed connection
		errors.Is(err, io.ErrUnexpectedEOF) || // Connection abruptly terminated
		strings.Contains(errorStr, "connection reset by peer") ||
		strings.Contains(errorStr, "broken pipe") ||
		strings.Contains(errorStr, "connection refused") ||
		strings.Contains(errorStr, "network is unreachable") ||
		strings.Contains(errorStr, "no route to host") {
		return true
	}

	// Context errors - don't disconnect, user choice
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}

	// Timeout errors - could be transient network congestion
	if strings.Contains(errorStr, "timeout") || strings.Contains(errorStr, "deadline exceeded") {
		return false // Keep connection, might recover
	}

	// For unknown errors, be conservative and keep the connection
	// Let the retry logic handle it, only disconnect on clear indicators
	return false
}

// disconnect safely closes the connection
func (c *Client) disconnect() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// ===============================================
// RETRY LOGIC IMPLEMENTATION
// ===============================================
// These functions handle automatic retry behavior with exponential backoff.

// isRetryableError determines if an error should trigger a retry attempt.
// Returns true for network errors, connection failures.
// Returns false for application-level errors like invalid arguments or key not found.
func (c *Client) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Retryable errors (transient infrastructure issues)
	if errors.Is(err, ErrNetworkError) || errors.Is(err, ErrConnectionFailed) {
		return true
	}

	// Non-retryable errors (permanent failures or user intent)
	if errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrKeyNotFound) ||
		errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return false
	}

	// Conservative approach: retry unknown errors
	return true
}

// calculateRetryDelay computes the delay for a retry attempt using exponential backoff with jitter.
func (c *Client) calculateRetryDelay(attempt int) time.Duration {
	if c.config.RetryAttempts <= 0 {
		return 0
	}

	// Use RetryBaseDelay from configuration
	baseDelay := c.config.RetryBaseDelay
	if baseDelay == 0 {
		baseDelay = 100 * time.Millisecond // Fallback if somehow not set
	}

	// Calculate exponential backoff: baseDelay * 2^attempt
	delay := baseDelay << attempt

	// Cap at maximum delay
	if c.config.RetryMaxDelay > 0 && delay > c.config.RetryMaxDelay {
		delay = c.config.RetryMaxDelay
	}

	// Add jitter if enabled
	if c.config.RetryJitter {
		jitter := c.calculateJitter(delay)
		delay = delay + jitter
	}

	return delay
}

// calculateJitter adds random variation to prevent thundering herd.
// Returns a value between -25% and +25% of the input delay.
func (c *Client) calculateJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}

	// Calculate 25% of delay for jitter range
	jitterRange := delay / 4

	// Generate cryptographically secure random number
	randomBig, err := rand.Int(rand.Reader, big.NewInt(int64(jitterRange*2)+1))
	if err != nil {
		// Fallback to no jitter if crypto/rand fails
		return 0
	}

	// Convert to jitter value: -25% to +25%
	jitter := time.Duration(randomBig.Int64()) - jitterRange
	return jitter
}

// retryOperation wraps an operation with retry logic using exponential backoff and jitter.
func (c *Client) retryOperation(ctx context.Context, operation func() error) error {
	if c.config.RetryAttempts <= 0 {
		// Retries disabled, execute once
		return operation()
	}

	var lastErr error
	for attempt := 0; attempt <= c.config.RetryAttempts; attempt++ {
		// Execute the operation
		err := operation()
		if err == nil {
			return nil // Success!
		}

		lastErr = err

		// Check if this is the last attempt
		if attempt == c.config.RetryAttempts {
			break // Don't delay after the final attempt
		}

		// Check if error is retryable
		if !c.isRetryableError(err) {
			break // Don't retry non-retryable errors
		}

		// Calculate delay for this attempt
		delay := c.calculateRetryDelay(attempt)

		// Wait for the delay, respecting context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return lastErr
}
