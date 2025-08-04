# kv-db SDK

A simple and intuitive Go client library for accessing [kv-db](https://github.com/EzhovAndrew/kv-db), an in-memory key-value database. This SDK handles connection management, command formatting, and response parsing automatically, making it easy to integrate kv-db into any Go project.

## ✨ Features

- **Simple API**: Easy-to-use methods for GET, SET, and DELETE operations
- **Automatic Connection Management**: Handles connections, reconnections, and timeouts
- **Type Safety**: Proper Go error types and structured responses
- **Context Support**: Full context support for timeouts and cancellation
- **Configurable**: Flexible configuration with sensible defaults
- **Error Handling**: Detailed error types for different failure scenarios
- **Thread Safe**: Safe for concurrent use across goroutines

## 📦 Installation

```bash
go get github.com/EzhovAndrew/kv-db/api
```

## 🚀 Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/EzhovAndrew/kv-db/api"
)

func main() {
    // Create a client with default configuration
    client, err := api.NewClient(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Set a value
    err = client.Set(ctx, "user:123", "John Doe")
    if err != nil {
        log.Printf("Failed to set value: %v", err)
        return
    }

    // Get the value
    value, err := client.Get(ctx, "user:123")
    if err != nil {
        log.Printf("Failed to get value: %v", err)
        return
    }

    fmt.Printf("Value: %s\n", value)

    // Delete the key
    err = client.Delete(ctx, "user:123")
    if err != nil {
        log.Printf("Failed to delete key: %v", err)
        return
    }

    fmt.Println("Key deleted successfully")
}
```

## ⚙️ Configuration

### Default Configuration

The SDK comes with sensible defaults that work with a standard kv-db installation:

```go
client, err := api.NewClient(nil) // Uses defaults
```

Default values:
- **Host**: `127.0.0.1`
- **Port**: `3223`
- **Connection Timeout**: `10 seconds`
- **Operation Timeout**: `5 seconds`
- **Max Message Size**: `4096 bytes`
- **Retry Attempts**: `3`
- **Retry Base Delay**: `100 milliseconds`
- **Retry Max Delay**: `5 seconds`
- **Retry Jitter**: `enabled`

### Custom Configuration

```go
config := api.DefaultConfig().
    WithHost("192.168.1.100").
    WithPort(8080).
    WithConnectionTimeout(20 * time.Second).
    WithOperationTimeout(10 * time.Second)

client, err := api.NewClient(config)
```

### Configuration Options

```go
type Config struct {
    Host              string        // Database server host
    Port              int           // Database server port
    ConnectionTimeout time.Duration // Timeout for establishing connections
    OperationTimeout  time.Duration // Timeout for individual operations
    MaxMessageSize    int           // Maximum message size (should match server config)
    RetryAttempts     int           // Number of retry attempts
    RetryBaseDelay    time.Duration // Base delay for exponential backoff
    RetryMaxDelay     time.Duration // Maximum delay cap for retries
    RetryJitter       bool          // Enable random jitter
}
```

## 🔧 API Reference

### Client Methods

#### `NewClient(config *Config) (*Client, error)`
Creates a new kv-db client. If config is nil, default configuration is used.

#### `Connect(ctx context.Context) error`
Explicitly establishes a connection to the server. Called automatically by Get/Set/Delete if not already connected.

#### `Get(ctx context.Context, key string) (string, error)`
Retrieves the value associated with the given key as a string.

```go
value, err := client.Get(ctx, "user:123")
if err != nil {
    if errors.Is(err, api.ErrKeyNotFound) {
        fmt.Println("Key not found")
    } else {
        log.Printf("Error: %v", err)
    }
    return
}
fmt.Printf("Value: %s\n", value)
```

#### `GetBytes(ctx context.Context, key string) ([]byte, error)`
Retrieves the value associated with the given key as a byte slice. More efficient for binary data and JSON unmarshaling.

```go
data, err := client.GetBytes(ctx, "user:123")
if err != nil {
    log.Printf("Error: %v", err)
    return
}

var user User
err = json.Unmarshal(data, &user)
if err != nil {
    log.Printf("Failed to unmarshal: %v", err)
}
```

#### `Set(ctx context.Context, key, value string) error`
Stores a key-value pair in the database using a string value.

```go
err := client.Set(ctx, "user:123", "John Doe")
if err != nil {
    log.Printf("Failed to set value: %v", err)
}
```

#### `SetBytes(ctx context.Context, key string, value []byte) error`
Stores a key-value pair in the database using a byte slice. More efficient for binary data and JSON marshaling.

```go
user := User{Name: "John", Age: 30}
data, err := json.Marshal(user)
if err != nil {
    log.Printf("Failed to marshal: %v", err)
    return
}

err = client.SetBytes(ctx, "user:123", data)
if err != nil {
    log.Printf("Failed to set value: %v", err)
}
```

#### `Delete(ctx context.Context, key string) error`
Removes a key-value pair from the database.

```go
err := client.Delete(ctx, "user:123")
if err != nil {
    log.Printf("Failed to delete key: %v", err)
}
```

#### `Ping(ctx context.Context) error`
Checks if the connection to the server is alive.

```go
err := client.Ping(ctx)
if err != nil {
    log.Printf("Server is not reachable: %v", err)
}
```

#### `Close() error`
Closes the connection to the database. Safe to call multiple times.

#### `IsConnected() bool`
Returns true if the client is currently connected to the server.

## 🚨 Error Handling

The SDK provides detailed error types to help you handle different failure scenarios:

### Error Types

- **`ErrKeyNotFound`**: Key doesn't exist in the database
- **`ErrInvalidArgument`**: Invalid arguments provided
- **`ErrConnectionFailed`**: Failed to connect to server
- **`ErrNetworkError`**: Network-related error
- **`ErrServerError`**: Server returned an error

### Error Checking Examples

```go
value, err := client.Get(ctx, "nonexistent-key")
if err != nil {
    switch {
    case errors.Is(err, api.ErrKeyNotFound):
        fmt.Println("Key not found")
    case errors.Is(err, api.ErrConnectionFailed):
        fmt.Println("Connection failed")
    case errors.Is(err, api.ErrNetworkError):
        fmt.Println("Network error")
    case errors.Is(err, api.ErrInvalidArgument):
        fmt.Println("Invalid argument")
    default:
        fmt.Printf("Unexpected error: %v\n", err)
    }
}
```

## 📚 Examples

### Basic Usage

See our comprehensive examples:

- **[examples/basic_usage/main.go](examples/basic_usage/main.go)** - Basic operations and configuration
- **[examples/json_usage/main.go](examples/json_usage/main.go)** - Efficient JSON handling with GetBytes/SetBytes
- **[examples/quick_demo/main.go](examples/quick_demo/main.go)** - Quick demonstration

Examples demonstrate:
- Creating clients with default and custom configurations
- Performing GET, SET, DELETE operations (both string and bytes)
- Error handling for different scenarios
- Working with complex data structures
- Connection management

### Context with Timeout

```go
// Set a timeout for the entire operation
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := client.Set(ctx, "key", "value")
if err != nil {
    log.Printf("Operation timed out or failed: %v", err)
}
```

### Storing JSON Data (Efficient Method)

```go
user := map[string]interface{}{
    "name":  "Alice",
    "age":   30,
    "email": "alice@example.com",
}

// Efficient: Use SetBytes to avoid string conversion
userJSON, _ := json.Marshal(user)
err := client.SetBytes(ctx, "user:alice", userJSON)

// Efficient: Use GetBytes for unmarshaling
data, err := client.GetBytes(ctx, "user:alice")
if err != nil {
    log.Fatal(err)
}

var retrievedUser map[string]interface{}
err = json.Unmarshal(data, &retrievedUser)
```

### Connection Lifecycle Management

```go
client, err := api.NewClient(nil)
if err != nil {
    log.Fatal(err)
}

// Explicitly connect
err = client.Connect(ctx)
if err != nil {
    log.Printf("Failed to connect: %v", err)
    return
}

// Check connection status
if client.IsConnected() {
    fmt.Println("Connected to database")
}

// Perform operations...

// Explicitly close
err = client.Close()
if err != nil {
    log.Printf("Failed to close connection: %v", err)
}
```

## 🔄 Concurrent Usage

The SDK is safe for concurrent use across multiple goroutines:

```go
client, err := api.NewClient(nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Safe to use from multiple goroutines
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        key := fmt.Sprintf("key:%d", id)
        value := fmt.Sprintf("value:%d", id)

        err := client.Set(context.Background(), key, value)
        if err != nil {
            log.Printf("Failed to set %s: %v", key, err)
        }
    }(i)
}
wg.Wait()
```

## 📋 Requirements

- Go 1.19 or higher
- Access to a running kv-db server

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the same license as the main kv-db project.
