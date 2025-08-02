// Package main demonstrates the performance difference between string and bytes methods.
// This example shows the real efficiency gains of using GetBytes/SetBytes for JSON operations
// versus the string conversion overhead of Get/Set methods.
//
// Note: Test data is sized to fit within the default server message limit (4096 bytes).
// For larger data testing, increase max_message_size in the server configuration.
//
// To run this benchmark:
// 1. Start the kv-db server: go run cmd/server/main.go
// 2. Run this example: go run api/examples/performance_comparison/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/EzhovAndrew/kv-db/api"
)

// TestData represents a typical JSON object you might store
type TestData struct {
	ID       int                    `json:"id"`
	Name     string                 `json:"name"`
	Email    string                 `json:"email"`
	Active   bool                   `json:"active"`
	Tags     []string               `json:"tags"`
	Metadata map[string]interface{} `json:"metadata"`
	Content  string                 `json:"content"`
}

func generateTestData(id int) TestData {
	// Generate realistic test data that fits within 4096 byte limit
	content := "Loremipsumdolorsitamet,consecteturadipiscingelit.Seddoeiusmodtemporincididuntutlabore."

	return TestData{
		ID:     id,
		Name:   fmt.Sprintf("User_%d", id),
		Email:  fmt.Sprintf("user%d@example.com", id),
		Active: id%2 == 0,
		Tags:   []string{"engineering", "backend", fmt.Sprintf("level_%d", id%5)},
		Metadata: map[string]any{
			"department": "Engineering",
			"level":      id % 5,
			"projects":   []string{"kv-db", "api-client"},
			"settings": map[string]any{
				"theme":    "dark",
				"language": "en",
			},
		},
		Content: content, // Much smaller but still realistic
	}
}

func main() {
	fmt.Println("🗄️  kv-db API - Performance Comparison")
	fmt.Println("======================================")

	// Create client
	client, err := api.NewClient(nil)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Test connection
	err = client.Ping(ctx)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v\n   Make sure kv-db server is running: go run cmd/server/main.go", err)
	}
	fmt.Println("✅ Connected to kv-db server")

	// Test parameters
	numOperations := 1000
	fmt.Printf("\n📊 Performance test with %d operations\n", numOperations)

	// Generate test data
	fmt.Println("\n1. Generating test data...")
	testData := make([]TestData, numOperations)
	for i := range numOperations {
		testData[i] = generateTestData(i)
	}

	// Pre-marshal all data to avoid including JSON marshal time in benchmarks
	marshaledData := make([][]byte, numOperations)
	for i, data := range testData {
		marshaledData[i], err = json.Marshal(data)
		if err != nil {
			log.Fatalf("❌ Failed to marshal test data: %v", err)
		}
	}

	avgSize := len(marshaledData[0])
	fmt.Printf("   ✅ Generated %d objects (avg size: %d bytes)\n", numOperations, avgSize)

	// Verify data fits within server's default message size limit
	if avgSize > 3500 { // Leave some buffer for command overhead
		fmt.Printf("   ⚠️  Warning: Data size (%d bytes) approaches server limit (4096 bytes)\n", avgSize)
		fmt.Println("   💡 Consider reducing test data size or increasing server max_message_size")
	} else {
		fmt.Printf("   ✅ Data size fits comfortably within server limit (4096 bytes)\n")
	}

	// Test 1: String methods (with conversions)
	fmt.Println("\n2. Testing String methods (Set + Get with conversions)...")

	runtime.GC() // Clean slate
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()

	// Write with string methods (includes conversion overhead)
	for i, data := range marshaledData {
		key := fmt.Sprintf("string_test:%d", i)
		// This internally: []byte → string → []byte
		err = client.Set(ctx, key, string(data))
		if err != nil {
			log.Printf("❌ String SET failed for %s: %v", key, err)
			continue
		}
	}

	writeTimeString := time.Since(start)

	start = time.Now()

	// Read with string methods (includes conversion overhead)
	retrievedString := 0
	for i := range numOperations {
		key := fmt.Sprintf("string_test:%d", i)
		// This internally: []byte → string → []byte (for unmarshal)
		value, err := client.Get(ctx, key)
		if err != nil {
			log.Printf("❌ String GET failed for %s: %v", key, err)
			continue
		}

		// Unmarshal (requires converting back to []byte)
		var data TestData
		err = json.Unmarshal([]byte(value), &data)
		if err != nil {
			log.Printf("❌ Unmarshal failed: %v", err)
			continue
		}
		retrievedString++
	}

	readTimeString := time.Since(start)
	runtime.ReadMemStats(&m2)

	stringMemory := m2.TotalAlloc - m1.TotalAlloc

	fmt.Printf("   📝 String SET: %v (%v per operation)\n",
		writeTimeString, writeTimeString/time.Duration(numOperations))
	fmt.Printf("   📖 String GET: %v (%v per operation)\n",
		readTimeString, readTimeString/time.Duration(numOperations))
	fmt.Printf("   📊 Retrieved: %d/%d objects\n", retrievedString, numOperations)
	fmt.Printf("   💾 Memory used: %d bytes\n", stringMemory)

	// Test 2: Bytes methods (no conversions)
	fmt.Println("\n3. Testing Bytes methods (SetBytes + GetBytes, no conversions)...")

	runtime.GC() // Clean slate
	runtime.ReadMemStats(&m1)

	start = time.Now()

	// Write with bytes methods (no conversion overhead)
	for i, data := range marshaledData {
		key := fmt.Sprintf("bytes_test:%d", i)
		// This works directly with []byte
		err = client.SetBytes(ctx, key, data)
		if err != nil {
			log.Printf("❌ Bytes SET failed for %s: %v", key, err)
			continue
		}
	}

	writeTimeBytes := time.Since(start)

	start = time.Now()

	// Read with bytes methods (no conversion overhead)
	retrievedBytes := 0
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("bytes_test:%d", i)
		// This returns []byte directly
		data, err := client.GetBytes(ctx, key)
		if err != nil {
			log.Printf("❌ Bytes GET failed for %s: %v", key, err)
			continue
		}

		// Unmarshal directly (no conversion needed)
		var testData TestData
		err = json.Unmarshal(data, &testData)
		if err != nil {
			log.Printf("❌ Unmarshal failed: %v", err)
			continue
		}
		retrievedBytes++
	}

	readTimeBytes := time.Since(start)
	runtime.ReadMemStats(&m2)

	bytesMemory := m2.TotalAlloc - m1.TotalAlloc

	fmt.Printf("   📝 Bytes SET: %v (%v per operation)\n",
		writeTimeBytes, writeTimeBytes/time.Duration(numOperations))
	fmt.Printf("   📖 Bytes GET: %v (%v per operation)\n",
		readTimeBytes, readTimeBytes/time.Duration(numOperations))
	fmt.Printf("   📊 Retrieved: %d/%d objects\n", retrievedBytes, numOperations)
	fmt.Printf("   💾 Memory used: %d bytes\n", bytesMemory)

	// Calculate improvements
	fmt.Println("\n4. Performance Analysis:")

	totalTimeString := writeTimeString + readTimeString
	totalTimeBytes := writeTimeBytes + readTimeBytes

	if totalTimeBytes > 0 {
		improvement := float64(totalTimeString-totalTimeBytes) / float64(totalTimeBytes) * 100
		fmt.Printf("   ⚡ Bytes methods are %.1f%% faster overall\n", improvement)

		writeImprovement := float64(writeTimeString-writeTimeBytes) / float64(writeTimeBytes) * 100
		fmt.Printf("   📝 SET operations: %.1f%% faster with SetBytes\n", writeImprovement)

		readImprovement := float64(readTimeString-readTimeBytes) / float64(readTimeBytes) * 100
		fmt.Printf("   📖 GET operations: %.1f%% faster with GetBytes\n", readImprovement)
	}

	if stringMemory > bytesMemory {
		memorySaved := float64(stringMemory-bytesMemory) / float64(stringMemory) * 100
		fmt.Printf("   💾 Memory savings: %.1f%% less allocation with bytes methods\n", memorySaved)
	}

	// Explain the differences
	fmt.Println("\n5. Why Bytes Methods Are More Efficient:")
	fmt.Println("   String Methods:")
	fmt.Println("     📊 json.Marshal(data) → []byte")
	fmt.Println("     🔄 []byte → string (conversion #1)")
	fmt.Println("     📡 string → []byte for TCP (conversion #2)")
	fmt.Println("     📡 []byte from TCP → string (conversion #3)")
	fmt.Println("     🔄 string → []byte for unmarshal (conversion #4)")
	fmt.Println("     📊 json.Unmarshal([]byte) → struct")
	fmt.Println()
	fmt.Println("   Bytes Methods:")
	fmt.Println("     📊 json.Marshal(data) → []byte")
	fmt.Println("     📡 []byte direct to TCP (no conversion)")
	fmt.Println("     📡 []byte direct from TCP (no conversion)")
	fmt.Println("     📊 json.Unmarshal([]byte) → struct")
	fmt.Println()
	fmt.Printf("   🎯 Eliminated %d string conversions per round trip!\n", 4)

	// Cleanup
	fmt.Println("\n6. Cleaning up test data...")
	cleanupStart := time.Now()

	// Clean up both test sets
	for i := 0; i < numOperations; i++ {
		client.Delete(ctx, fmt.Sprintf("string_test:%d", i))
		client.Delete(ctx, fmt.Sprintf("bytes_test:%d", i))
	}

	cleanupTime := time.Since(cleanupStart)
	fmt.Printf("   ✅ Cleanup completed in %v\n", cleanupTime)

	fmt.Println("\n🎉 Performance comparison completed!")
	fmt.Println("\n💡 Recommendation:")
	fmt.Println("   • Use SetBytes/GetBytes for JSON data and binary content")
	fmt.Println("   • Use Set/Get for simple string values")
	fmt.Println("   • The efficiency gains increase with data size and operation volume")
}
