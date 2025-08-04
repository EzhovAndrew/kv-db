// Package main demonstrates basic usage of the kv-db SDK.
// This example shows how to connect to a kv-db server and perform
// basic operations like GET, SET, and DELETE.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/EzhovAndrew/kv-db/api"
)

func main() {
	fmt.Println("🗄️  kv-db SDK Example")
	fmt.Println("====================")

	// Example 1: Using default configuration
	fmt.Println("\n1. Creating client with default configuration...")
	client, err := api.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Context with timeout for operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Example 2: Basic operations
	fmt.Println("\n2. Performing basic operations...")

	// Set a key-value pair
	fmt.Println("   Setting key 'user:123' = 'John Doe'...")
	err = client.Set(ctx, "user:123", "John Doe")
	if err != nil {
		log.Printf("Failed to set value: %v", err)
		return
	}
	fmt.Println("   ✅ Value set successfully")

	// Get the value
	fmt.Println("   Getting key 'user:123'...")
	value, err := client.Get(ctx, "user:123")
	if err != nil {
		log.Printf("Failed to get value: %v", err)
		return
	}
	fmt.Printf("   ✅ Retrieved value: %s\n", value)

	// Delete the key
	fmt.Println("   Deleting key 'user:123'...")
	err = client.Delete(ctx, "user:123")
	if err != nil {
		log.Printf("Failed to delete key: %v", err)
		return
	}
	fmt.Println("   ✅ Key deleted successfully")

	// Example 3: Error handling
	fmt.Println("\n3. Demonstrating error handling...")

	// Try to get a non-existent key
	fmt.Println("   Trying to get non-existent key 'user:123'...")
	value, err = client.Get(ctx, "user:123")
	if err != nil {
		// Check if it's a "key not found" error
		if errors.Is(err, api.ErrKeyNotFound) {
			fmt.Println("   ✅ Key not found (expected)")
		} else {
			log.Printf("   ✗ Unexpected error: %v", err)
		}
	} else {
		log.Printf("   ✗ Expected error but got value: %s", value)
	}

	// Example 4: Custom configuration
	fmt.Println("\n4. Creating client with custom configuration...")

	config := api.DefaultConfig().
		WithHost("127.0.0.1").
		WithPort(3223).
		WithConnectionTimeout(5 * time.Second).
		WithOperationTimeout(3 * time.Second)

	customClient, err := api.NewClient(config)
	if err != nil {
		log.Printf("Failed to create custom client: %v", err)
		return
	}
	defer customClient.Close()

	// Test connection
	fmt.Println("   Testing connection...")
	err = customClient.Ping(ctx)
	if err != nil {
		log.Printf("Failed to ping server: %v", err)
		return
	}
	fmt.Println("   ✅ Connection successful")

	// Example 5: Working with different data types
	fmt.Println("\n5. Working with different data types...")

	// Store various types of data
	examples := map[string]string{
		"user:profile:123":   `{"name":"Alice","age":30,"email":"alice@example.com"}`,
		"config:app:timeout": "30",
		"cache:session:abc":  "eyJhbGciOiJIUzI1NiJ9...",
		"counter:page_views": "1042",
		"status:server":      "online",
		"version:api":        "v2.1.0",
	}

	for key, value := range examples {
		fmt.Printf("   Setting %s...\n", key)
		err = customClient.Set(ctx, key, value)
		if err != nil {
			log.Printf("Failed to set %s: %v", key, err)
			continue
		}

		// Retrieve and verify
		retrieved, err := customClient.Get(ctx, key)
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
			continue
		}

		if retrieved == value {
			fmt.Printf("   ✅ %s stored and retrieved successfully\n", key)
		} else {
			fmt.Printf("   ✗ %s: expected '%s', got '%s'\n", key, value, retrieved)
		}
	}

	// Clean up
	fmt.Println("\n6. Cleaning up...")
	for key := range examples {
		err = customClient.Delete(ctx, key)
		if err != nil {
			log.Printf("Failed to delete %s: %v", key, err)
		}
	}
	fmt.Println("   ✅ Cleanup completed")

	fmt.Println("\n🎉 Example completed successfully!")
}

// Additional example functions that users can reference

// ExampleWithRetry demonstrates how you might implement retry logic
func ExampleWithRetry() {
	client, err := api.NewClient(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	key := "example:key"
	value := "example_value" // No spaces in value

	// Retry logic for SET operation
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		err = client.Set(ctx, key, value)
		if err == nil {
			fmt.Println("✅ Set operation successful")
			break
		}

		if i == maxRetries-1 {
			log.Printf("Failed to set after %d attempts: %v", maxRetries, err)
			return
		}

		fmt.Printf("Set failed (attempt %d/%d), retrying...\n", i+1, maxRetries)
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
}

// ExampleConnectionManagement shows connection lifecycle management
func ExampleConnectionManagement() {
	client, err := api.NewClient(nil)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Explicitly connect
	err = client.Connect(ctx)
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		return
	}
	fmt.Println("✅ Connected")

	// Check connection status
	if client.IsConnected() {
		fmt.Println("✅ Client is connected")
	}

	// Perform operations...
	err = client.Set(ctx, "test", "simple_value") // No spaces in value
	if err != nil {
		log.Printf("Operation failed: %v", err)
	}

	// Explicitly close connection
	err = client.Close()
	if err != nil {
		log.Printf("Failed to close: %v", err)
	} else {
		fmt.Println("✅ Connection closed")
	}
}
