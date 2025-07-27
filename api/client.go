// Package api provides a simple and intuitive client library for accessing kv-db.
// It handles connection management, command formatting, and response parsing automatically.
package api

import (
	"bytes"
	"context"
	"fmt"
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

	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	// Build command using bytes.Buffer - add newline for protocol compatibility
	var cmd bytes.Buffer
	cmd.WriteString("GET ")
	cmd.WriteString(key)
	cmd.WriteString("\n") // Add newline as original client does

	response, err := c.sendCommandBytes(ctx, cmd.Bytes())
	if err != nil {
		return nil, err
	}

	// Handle key not found - need to check the response as string for this
	responseStr := string(response)
	if strings.Contains(responseStr, "key not found") || strings.Contains(responseStr, "not found") {
		return nil, &KeyNotFoundError{Key: key}
	}

	return response, nil
}

// Set stores a key-value pair in the database using a string value.
// This method converts the string value to bytes and calls SetBytes.
// For better performance with binary data or JSON, use SetBytes instead.
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
// Returns nil on success, error on failure.
//
// Note: Values containing whitespace (spaces, tabs, newlines) are not supported
// by the current protocol and will return an error.
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

	// Check for whitespace in value - the protocol doesn't support it
	if bytes.ContainsAny(value, " \t\n\r") {
		return &InvalidArgumentError{
			Message: "values containing whitespace (spaces, tabs, newlines) " +
				"are not supported by the protocol and will be improved " +
				"in future releases.",
		}
	}

	if err := c.ensureConnected(ctx); err != nil {
		return err
	}

	// Build command using bytes.Buffer - add newline for protocol compatibility
	var cmd bytes.Buffer
	cmd.WriteString("SET ")
	cmd.WriteString(key)
	cmd.WriteString(" ")
	cmd.Write(value)
	cmd.WriteString("\n") // Add newline as original client does

	response, err := c.sendCommandBytes(ctx, cmd.Bytes())
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

// Delete removes a key-value pair from the database.
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

	if err := c.ensureConnected(ctx); err != nil {
		return err
	}

	// Build command using bytes.Buffer - add newline for protocol compatibility
	var cmd bytes.Buffer
	cmd.WriteString("DEL ")
	cmd.WriteString(key)
	cmd.WriteString("\n") // Add newline as original client does

	response, err := c.sendCommandBytes(ctx, cmd.Bytes())
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

	// Send command as bytes
	_, err := conn.Write(command)
	if err != nil {
		c.disconnect()
		return nil, &NetworkError{Err: err}
	}

	// Read response as bytes
	buffer := make([]byte, c.config.MaxMessageSize)
	n, err := conn.Read(buffer)
	if err != nil {
		c.disconnect()
		return nil, &NetworkError{Err: err}
	}

	// Return raw bytes, trimming trailing whitespace
	response := bytes.TrimSpace(buffer[:n])
	return response, nil
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
