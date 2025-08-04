// Package main provides a quick demonstration of the kv-db SDK.
// This example can be run against a live kv-db server to showcase
// the SDK's capabilities.
//
// To run this demo:
// 1. Start the kv-db server: go run cmd/server/main.go
// 2. Run this demo: go run sdk/examples/quick_demo/main.go
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
	fmt.Println("🗄️  kv-db SDK Quick Demo")
	fmt.Println("========================")

	// Step 1: Create a client
	fmt.Println("\n1. Creating API client...")
	client, err := api.NewClient(nil)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()
	fmt.Println("✅ Client created successfully")

	// Step 2: Test connection
	fmt.Println("\n2. Testing connection...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Ping(ctx)
	if err != nil {
		log.Fatalf("❌ Failed to connect to server: %v\n   Make sure the kv-db server is running with: go run cmd/server/main.go", err)
	}
	fmt.Println("✅ Connected to kv-db server")

	// Step 3: Basic operations
	fmt.Println("\n3. Performing basic operations...")

	// SET operation
	fmt.Println("   📝 Setting 'demo:user' = 'Alice'...")
	err = client.Set(ctx, "demo:user", "Alice")
	if err != nil {
		log.Printf("❌ SET failed: %v", err)
		return
	}
	fmt.Println("   ✅ SET successful")

	// GET operation
	fmt.Println("   📖 Getting 'demo:user'...")
	value, err := client.Get(ctx, "demo:user")
	if err != nil {
		log.Printf("❌ GET failed: %v", err)
		return
	}
	fmt.Printf("   ✅ GET successful: %s\n", value)

	// Step 4: Demonstrate error handling
	fmt.Println("\n4. Demonstrating error handling...")
	fmt.Println("   📖 Trying to get non-existent key 'demo:nonexistent'...")
	value, err = client.Get(ctx, "demo:nonexistent")
	if err != nil {
		if errors.Is(err, api.ErrKeyNotFound) {
			fmt.Println("   ✅ Correctly handled 'key not found' error")
		} else {
			fmt.Printf("   ❌ Unexpected error: %v\n", err)
		}
	} else {
		fmt.Printf("   ❌ Expected error but got value: %s\n", value)
	}

	// Step 5: Multiple operations
	fmt.Println("\n5. Performing multiple operations...")
	testData := map[string]string{
		"demo:config:timeout": "30",
		"demo:user:123":       "John Doe",
		"demo:session:abc123": "active",
		"demo:cache:popular":  "golang,database,performance",
	}

	for key, val := range testData {
		fmt.Printf("   📝 Setting %s...\n", key)
		err = client.Set(ctx, key, val)
		if err != nil {
			log.Printf("❌ Failed to set %s: %v", key, err)
			continue
		}

		// Verify by reading back
		retrieved, err := client.Get(ctx, key)
		if err != nil {
			log.Printf("❌ Failed to get %s: %v", key, err)
			continue
		}

		if retrieved == val {
			fmt.Printf("   ✅ %s verified\n", key)
		} else {
			fmt.Printf("   ❌ %s: expected '%s', got '%s'\n", key, val, retrieved)
		}
	}

	// Step 6: Cleanup
	fmt.Println("\n6. Cleaning up demo data...")
	for key := range testData {
		err = client.Delete(ctx, key)
		if err != nil {
			log.Printf("❌ Failed to delete %s: %v", key, err)
		}
	}

	// Clean up the original demo key
	err = client.Delete(ctx, "demo:user")
	if err != nil {
		log.Printf("❌ Failed to delete demo:user: %v", err)
	}

	fmt.Println("   ✅ Cleanup completed")

	// Step 7: Show configuration example
	fmt.Println("\n7. Configuration example...")
	customConfig := api.DefaultConfig().
		WithHost("127.0.0.1").
		WithPort(3223).
		WithConnectionTimeout(5 * time.Second).
		WithOperationTimeout(3 * time.Second)

	fmt.Printf("   ⚙️  Custom config: Host=%s, Port=%d, ConnTimeout=%v\n",
		customConfig.Host, customConfig.Port, customConfig.ConnectionTimeout)

	// Final message
	fmt.Println("\n🎉 Demo completed successfully!")
	fmt.Println("\n💡 Next steps:")
	fmt.Println("   • Read the full documentation: api/README.md")
	fmt.Println("   • Check out more examples: api/examples/basic_usage/main.go")
	fmt.Println("   • Import the API client in your project: go get github.com/EzhovAndrew/kv-db/api")
	fmt.Println("\n📖 Documentation: https://github.com/EzhovAndrew/kv-db")
}
