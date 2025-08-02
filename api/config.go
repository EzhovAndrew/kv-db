package api

import (
	"errors"
	"time"
)

// Config holds the configuration for the kv-db client.
type Config struct {
	// Host is the hostname or IP address of the kv-db server.
	// Default: "127.0.0.1"
	Host string

	// Port is the port number of the kv-db server.
	// Default: 3223
	Port int

	// ConnectionTimeout is the timeout for establishing a connection.
	// Default: 10 seconds
	ConnectionTimeout time.Duration

	// OperationTimeout is the timeout for individual operations (GET, SET, DELETE).
	// Default: 5 seconds
	OperationTimeout time.Duration

	// MaxMessageSize is the maximum size of messages sent/received.
	// This should match the server's max_message_size configuration.
	// Default: 4096 bytes
	MaxMessageSize int

	// RetryAttempts is the number of times to retry failed operations.
	// Default: 3
	RetryAttempts int

	// RetryDelay is the delay between retry attempts.
	// Default: 100 milliseconds
	RetryDelay time.Duration
}

// DefaultConfig returns a Config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		Host:              "127.0.0.1",
		Port:              3223,
		ConnectionTimeout: 10 * time.Second,
		OperationTimeout:  5 * time.Second,
		MaxMessageSize:    4096,
		RetryAttempts:     3,
		RetryDelay:        100 * time.Millisecond,
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}

	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	if c.ConnectionTimeout <= 0 {
		return errors.New("connection timeout must be positive")
	}

	if c.OperationTimeout <= 0 {
		return errors.New("operation timeout must be positive")
	}

	if c.MaxMessageSize <= 0 {
		return errors.New("max message size must be positive")
	}

	if c.RetryAttempts < 0 {
		return errors.New("retry attempts cannot be negative")
	}

	if c.RetryDelay < 0 {
		return errors.New("retry delay cannot be negative")
	}

	return nil
}

// WithHost sets the host and returns the config for method chaining.
func (c *Config) WithHost(host string) *Config {
	c.Host = host
	return c
}

// WithPort sets the port and returns the config for method chaining.
func (c *Config) WithPort(port int) *Config {
	c.Port = port
	return c
}

// WithConnectionTimeout sets the connection timeout and returns the config for method chaining.
func (c *Config) WithConnectionTimeout(timeout time.Duration) *Config {
	c.ConnectionTimeout = timeout
	return c
}

// WithOperationTimeout sets the operation timeout and returns the config for method chaining.
func (c *Config) WithOperationTimeout(timeout time.Duration) *Config {
	c.OperationTimeout = timeout
	return c
}

// WithMaxMessageSize sets the max message size and returns the config for method chaining.
func (c *Config) WithMaxMessageSize(size int) *Config {
	c.MaxMessageSize = size
	return c
}

// WithRetryAttempts sets the retry attempts and returns the config for method chaining.
func (c *Config) WithRetryAttempts(attempts int) *Config {
	c.RetryAttempts = attempts
	return c
}

// WithRetryDelay sets the retry delay and returns the config for method chaining.
func (c *Config) WithRetryDelay(delay time.Duration) *Config {
	c.RetryDelay = delay
	return c
}
