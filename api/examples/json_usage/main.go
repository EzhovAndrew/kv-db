// Package main demonstrates using GetBytes and SetBytes methods with JSON data.
// This shows the efficiency benefits of working directly with byte slices
// when marshaling/unmarshaling JSON or other binary data.
//
// To run this example:
// 1. Start the kv-db server: go run cmd/server/main.go
// 2. Run this example: go run api/examples/json_usage/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/EzhovAndrew/kv-db/api"
)

// User represents a user object for demonstration
type User struct {
	ID       int                    `json:"id"`
	Name     string                 `json:"name"`
	Email    string                 `json:"email"`
	Age      int                    `json:"age"`
	Active   bool                   `json:"active"`
	Tags     []string               `json:"tags"`
	Metadata map[string]interface{} `json:"metadata"`
}

// Config represents application configuration
type Config struct {
	AppName  string         `json:"app_name"`
	Version  string         `json:"version"`
	Debug    bool           `json:"debug"`
	Timeouts map[string]int `json:"timeouts"`
	Features []string       `json:"features"`
}

func main() {
	fmt.Println("🗄️  kv-db API - JSON Usage Example")
	fmt.Println("===================================")

	// Create client
	client, err := api.NewClient(nil)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection
	err = client.Ping(ctx)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v\n   Make sure kv-db server is running: go run cmd/server/main.go", err)
	}
	fmt.Println("✅ Connected to kv-db server")

	// Example 1: Working with User objects
	fmt.Println("\n1. Working with User objects using JSON...")

	user := User{
		ID:     123,
		Name:   "AliceJohnson",
		Email:  "alice@example.com",
		Age:    28,
		Active: true,
		Tags:   []string{"admin", "developer", "team-lead"},
		Metadata: map[string]interface{}{
			"department":    "Engineering",
			"hire_date":     "2022-03-15",
			"salary_grade":  "L5",
			"remote_worker": true,
		},
	}

	// Efficient way: Marshal to bytes and use SetBytes
	fmt.Println("   📝 Storing user with SetBytes...")
	userBytes, err := json.Marshal(user)
	if err != nil {
		log.Printf("❌ Failed to marshal user: %v", err)
		return
	}

	err = client.SetBytes(ctx, "user:123", userBytes)
	if err != nil {
		log.Printf("❌ Failed to store user: %v", err)
		return
	}
	fmt.Printf("   ✅ User stored (%d bytes)\n", len(userBytes))

	// Efficient way: Use GetBytes and unmarshal
	fmt.Println("   📖 Retrieving user with GetBytes...")
	retrievedBytes, err := client.GetBytes(ctx, "user:123")
	if err != nil {
		log.Printf("❌ Failed to get user: %v", err)
		return
	}

	var retrievedUser User
	err = json.Unmarshal(retrievedBytes, &retrievedUser)
	if err != nil {
		log.Printf("❌ Failed to unmarshal user: %v", err)
		return
	}
	fmt.Printf("   ✅ User retrieved: %s (%s)\n", retrievedUser.Name, retrievedUser.Email)

	// Example 2: Working with complex configuration
	fmt.Println("\n2. Working with application configuration...")

	config := Config{
		AppName: "kv-db-app",
		Version: "1.2.3",
		Debug:   false,
		Timeouts: map[string]int{
			"http_read":    30,
			"http_write":   30,
			"db_connect":   10,
			"cache_expiry": 300,
		},
		Features: []string{"authentication", "rate_limiting", "metrics", "health_checks"},
	}

	// Store configuration
	fmt.Println("   📝 Storing configuration...")
	configBytes, err := json.Marshal(config)
	if err != nil {
		log.Printf("❌ Failed to marshal config: %v", err)
		return
	}

	err = client.SetBytes(ctx, "app:config", configBytes)
	if err != nil {
		log.Printf("❌ Failed to store config: %v", err)
		return
	}
	fmt.Printf("   ✅ Configuration stored (%d bytes)\n", len(configBytes))

	// Retrieve and verify configuration
	fmt.Println("   📖 Retrieving configuration...")
	configData, err := client.GetBytes(ctx, "app:config")
	if err != nil {
		log.Printf("❌ Failed to get config: %v", err)
		return
	}

	var retrievedConfig Config
	err = json.Unmarshal(configData, &retrievedConfig)
	if err != nil {
		log.Printf("❌ Failed to unmarshal config: %v", err)
		return
	}
	fmt.Printf("   ✅ Config retrieved: %s v%s (Debug: %v)\n",
		retrievedConfig.AppName, retrievedConfig.Version, retrievedConfig.Debug)

	// Example 3: Comparison with string methods (for demonstration)
	fmt.Println("\n3. Comparing efficiency approaches...")

	// Inefficient way (for comparison): Convert to string
	fmt.Println("   📊 Method comparison:")
	fmt.Println("      🔄 String method: bytes → string → bytes (2 conversions)")
	fmt.Println("      ⚡ Bytes method:  bytes → bytes (0 conversions)")

	// Example 4: Working with arrays of objects
	fmt.Println("\n4. Working with arrays of objects...")

	users := []User{
		{ID: 1, Name: "BobSmith", Email: "bob@example.com", Age: 32, Active: true},
		{ID: 2, Name: "CarolDavis", Email: "carol@example.com", Age: 27, Active: true},
		{ID: 3, Name: "DavidWilson", Email: "david@example.com", Age: 35, Active: false},
	}

	// Store array
	fmt.Println("   📝 Storing user array...")
	usersBytes, err := json.Marshal(users)
	if err != nil {
		log.Printf("❌ Failed to marshal users: %v", err)
		return
	}

	err = client.SetBytes(ctx, "users:active", usersBytes)
	if err != nil {
		log.Printf("❌ Failed to store users: %v", err)
		return
	}
	fmt.Printf("   ✅ User array stored (%d users, %d bytes)\n", len(users), len(usersBytes))

	// Retrieve array
	fmt.Println("   📖 Retrieving user array...")
	usersData, err := client.GetBytes(ctx, "users:active")
	if err != nil {
		log.Printf("❌ Failed to get users: %v", err)
		return
	}

	var retrievedUsers []User
	err = json.Unmarshal(usersData, &retrievedUsers)
	if err != nil {
		log.Printf("❌ Failed to unmarshal users: %v", err)
		return
	}
	fmt.Printf("   ✅ Retrieved %d users\n", len(retrievedUsers))

	// Example 5: Working with nested JSON
	fmt.Println("\n5. Working with deeply nested JSON...")

	complexData := map[string]interface{}{
		"application": map[string]interface{}{
			"name":    "kv-db-service",
			"version": "2.1.0",
			"config": map[string]interface{}{
				"database": map[string]interface{}{
					"host":     "localhost",
					"port":     3223,
					"timeouts": map[string]int{"read": 5, "write": 10},
				},
				"cache": map[string]interface{}{
					"enabled": true,
					"ttl":     3600,
					"size":    1000,
				},
			},
		},
		"metrics": []map[string]interface{}{
			{"name": "requests_total", "value": 12345, "type": "counter"},
			{"name": "response_time", "value": 125.5, "type": "histogram"},
		},
	}

	// Store complex nested data
	complexBytes, err := json.Marshal(complexData)
	if err != nil {
		log.Printf("❌ Failed to marshal complex data: %v", err)
		return
	}

	err = client.SetBytes(ctx, "app:metrics", complexBytes)
	if err != nil {
		log.Printf("❌ Failed to store complex data: %v", err)
		return
	}
	fmt.Printf("   ✅ Complex data stored (%d bytes)\n", len(complexBytes))

	// Cleanup
	fmt.Println("\n6. Cleaning up...")
	keys := []string{"user:123", "app:config", "users:active", "app:metrics"}
	for _, key := range keys {
		err = client.Delete(ctx, key)
		if err != nil {
			log.Printf("❌ Failed to delete %s: %v", key, err)
		}
	}
	fmt.Println("   ✅ Cleanup completed")

	// Performance tips
	fmt.Println("\n💡 Performance Tips:")
	fmt.Println("   • Use GetBytes/SetBytes for JSON data to avoid string conversion overhead")
	fmt.Println("   • Marshal once, store as bytes, unmarshal when needed")
	fmt.Println("   • Consider using json.RawMessage for partial parsing of large objects")
	fmt.Println("   • Batch operations when possible to reduce network round trips")

	fmt.Println("\n🎉 JSON usage example completed successfully!")
}
