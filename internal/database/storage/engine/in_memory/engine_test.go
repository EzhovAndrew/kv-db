package in_memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/utils"
)

var testLSN uint64 = 1

// getTestContext creates a context with LSN and count for testing
func getTestContext(count int) context.Context {
	ctx := context.Background()
	testLSN++
	return utils.ContextWithLSNAndCount(ctx, testLSN, count)
}

func engineLen(e *Engine) int {
	total := 0
	for i := range NumShards {
		concurrency.WithLock(e.shards[i].lock.RLocker(), func() error {
			total += len(e.shards[i].data)
			return nil
		})
	}
	return total
}

func engineSetDirect(e *Engine, key, value string) {
	shard := e.getShard(key)
	concurrency.WithLock(&shard.lock, func() error {
		shard.data[key] = value
		return nil
	})
}

func engineExists(e *Engine, key string) bool {
	shard := e.getShard(key)
	var exists bool
	concurrency.WithLock(shard.lock.RLocker(), func() error {
		_, exists = shard.data[key]
		return nil
	})
	return exists
}

func TestNewEngine(t *testing.T) {
	engine, err := NewEngine()

	assert.NoError(t, err)
	assert.NotNil(t, engine)
	assert.NotNil(t, engine.shards)
	assert.Equal(t, 0, engineLen(engine))

	// Clean up workers
	defer engine.Shutdown()
}

func TestEngine_Set(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	t.Run("set new key-value pair", func(t *testing.T) {
		ctx := getTestContext(1) // count = 1 means immediate execution
		err := engine.Set(ctx, "key1", "value1")
		assert.NoError(t, err)
		value, err := engine.Get(context.Background(), "key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", value)
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		ctx := getTestContext(1)
		err := engine.Set(ctx, "key1", "newvalue1")
		assert.NoError(t, err)
		value, err := engine.Get(context.Background(), "key1")
		assert.NoError(t, err)
		assert.Equal(t, "newvalue1", value)
	})

	t.Run("set empty value", func(t *testing.T) {
		ctx := getTestContext(1)
		err := engine.Set(ctx, "emptykey", "")
		assert.Error(t, err)
		assert.Equal(t, "value cannot be empty", err.Error())
	})

	t.Run("set empty key", func(t *testing.T) {
		ctx := getTestContext(1)
		err := engine.Set(ctx, "", "value")
		assert.Error(t, err)
		assert.Equal(t, "key cannot be empty", err.Error())
	})

	t.Run("set with special characters", func(t *testing.T) {
		ctx := getTestContext(1)
		key := "key with spaces and symbols!@#$%"
		value := "value with\nnewlines\tand\ttabs"
		err := engine.Set(ctx, key, value)
		assert.NoError(t, err)
		actualValue, err := engine.Get(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, value, actualValue)
	})
}

func TestEngine_Get(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	t.Run("get existing key", func(t *testing.T) {
		engineSetDirect(engine, "testkey", "testvalue")

		value, err := engine.Get(context.Background(), "testkey")
		assert.NoError(t, err)
		assert.Equal(t, "testvalue", value)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		value, err := engine.Get(context.Background(), "nonexistent")
		assert.Error(t, err)
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Equal(t, "", value)
	})

	t.Run("get key with special characters", func(t *testing.T) {
		key := "special!@#$%^&*()"
		expectedValue := "special value with unicode: 你好"
		engineSetDirect(engine, key, expectedValue)

		value, err := engine.Get(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	})
}

func TestEngine_Delete(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	t.Run("delete existing key", func(t *testing.T) {
		engineSetDirect(engine, "deletekey", "deletevalue")

		ctx := getTestContext(1)
		err := engine.Delete(ctx, "deletekey")
		assert.NoError(t, err)

		exists := engineExists(engine, "deletekey")
		assert.False(t, exists)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		key := "nonexistent"
		engineSetDirect(engine, key, "value")

		ctx := getTestContext(1)
		err := engine.Delete(ctx, "different_key")
		assert.NoError(t, err)

		exists := engineExists(engine, key)
		assert.True(t, exists)
	})
}

func TestEngine_ConcurrentOperations(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	numGoroutines := 10
	numOperations := 100

	t.Run("concurrent sets", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("key_%d_%d", id, j)
					value := fmt.Sprintf("value_%d_%d", id, j)
					ctx := getTestContext(1)
					err := engine.Set(ctx, key, value)
					assert.NoError(t, err)
				}
			}(i)
		}

		wg.Wait()

		assert.Equal(t, numGoroutines*numOperations, engineLen(engine))
	})

	t.Run("concurrent gets and sets", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			engineSetDirect(engine, fmt.Sprintf("prekey_%d", i), fmt.Sprintf("prevalue_%d", i))
		}

		var wg sync.WaitGroup

		for i := 0; i < numGoroutines/2; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("readkey_%d_%d", id, j)
					value := fmt.Sprintf("readvalue_%d_%d", id, j)
					ctx := getTestContext(1)
					err := engine.Set(ctx, key, value)
					assert.NoError(t, err)
				}
			}(i)
		}

		for i := 0; i < numGoroutines/2; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("prekey_%d", j%50)
					value, err := engine.Get(context.Background(), key)
					if err == nil {
						assert.True(t, strings.HasPrefix(value, "prevalue_"))
					}
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent deletes", func(t *testing.T) {
		for i := range 50 {
			key := fmt.Sprintf("delkey_%d", i)
			engineSetDirect(engine, key, fmt.Sprintf("delvalue_%d", i))
		}

		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					key := fmt.Sprintf("delkey_%d", (id*5+j)%50)
					ctx := getTestContext(1)
					err := engine.Delete(ctx, key)
					assert.NoError(t, err)
				}
			}(i)
		}

		wg.Wait()

		for i := range 50 {
			key := fmt.Sprintf("delkey_%d", i)
			exists := engineExists(engine, key)
			assert.False(t, exists)
		}
	})
}

func TestEngine_WorkerSystem_LSNOrdering(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	t.Run("workers apply operations in LSN order", func(t *testing.T) {
		key := "lsn_order_test"

		// Set up operations with known LSN order but submit them out of order
		ctx1 := utils.ContextWithLSNAndCount(context.Background(), 1, 5) // LSN 1
		ctx2 := utils.ContextWithLSNAndCount(context.Background(), 2, 5) // LSN 2
		ctx3 := utils.ContextWithLSNAndCount(context.Background(), 3, 5) // LSN 3
		ctx4 := utils.ContextWithLSNAndCount(context.Background(), 4, 5) // LSN 4
		ctx5 := utils.ContextWithLSNAndCount(context.Background(), 5, 5) // LSN 5

		var wg sync.WaitGroup

		// Submit operations in reverse order to test LSN sorting
		operations := []struct {
			ctx   context.Context
			value string
			lsn   uint64
		}{
			{ctx5, "final_value", 5},
			{ctx3, "middle_value", 3},
			{ctx1, "first_value", 1},
			{ctx4, "fourth_value", 4},
			{ctx2, "second_value", 2},
		}

		wg.Add(len(operations))
		for _, op := range operations {
			go func(ctx context.Context, value string) {
				defer wg.Done()
				err := engine.Set(ctx, key, value)
				assert.NoError(t, err)
			}(op.ctx, op.value)
		}

		wg.Wait()

		// The final value should be from LSN 5 (highest LSN)
		finalValue, err := engine.Get(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, "final_value", finalValue)
	})

	t.Run("count=1 executes immediately, count>1 uses workers", func(t *testing.T) {
		// Test that operations with count=1 execute immediately
		// while operations with count>1 go through workers

		// Immediate execution test
		immediateCtx := getTestContext(1) // count = 1
		err := engine.Set(immediateCtx, "immediate_key", "immediate_value")
		assert.NoError(t, err)

		// Should be available immediately
		value, err := engine.Get(context.Background(), "immediate_key")
		assert.NoError(t, err)
		assert.Equal(t, "immediate_value", value)

		// Worker execution test - operations with same key but count > 1
		workerKey := "worker_key"
		ctx1 := utils.ContextWithLSNAndCount(context.Background(), 10, 2) // LSN 10, count 2
		ctx2 := utils.ContextWithLSNAndCount(context.Background(), 11, 2) // LSN 11, count 2

		var wg sync.WaitGroup
		wg.Add(2)

		// Submit operations concurrently - these should go through workers
		go func() {
			defer wg.Done()
			err := engine.Set(ctx2, workerKey, "second_operation") // Higher LSN
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			err := engine.Set(ctx1, workerKey, "first_operation") // Lower LSN
			assert.NoError(t, err)
		}()

		wg.Wait()

		// Final value should be from higher LSN (11)
		value, err = engine.Get(context.Background(), workerKey)
		assert.NoError(t, err)
		assert.Equal(t, "second_operation", value)
	})

	t.Run("workers handle mixed Set and Delete operations", func(t *testing.T) {
		key := "mixed_ops_key"

		// Set initial value directly
		engineSetDirect(engine, key, "initial")

		// Create operations: Set(LSN=1), Delete(LSN=2), Set(LSN=3)
		ctx1 := utils.ContextWithLSNAndCount(context.Background(), 1, 3) // Set
		ctx2 := utils.ContextWithLSNAndCount(context.Background(), 2, 3) // Delete
		ctx3 := utils.ContextWithLSNAndCount(context.Background(), 3, 3) // Set

		var wg sync.WaitGroup
		wg.Add(3)

		// Submit in random order
		go func() {
			defer wg.Done()
			err := engine.Set(ctx3, key, "final_set")
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			err := engine.Delete(ctx2, key)
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			err := engine.Set(ctx1, key, "first_set")
			assert.NoError(t, err)
		}()

		wg.Wait()

		// Final result should be from LSN 3 (Set operation)
		value, err := engine.Get(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, "final_set", value)
	})

	t.Run("multiple keys processed by different workers in parallel", func(t *testing.T) {
		// Test that different keys can be processed by different workers simultaneously
		numKeys := 10
		var wg sync.WaitGroup

		for i := range numKeys {
			wg.Add(1)
			go func(keyIndex int) {
				defer wg.Done()

				key := fmt.Sprintf("parallel_key_%d", keyIndex)

				// Create multiple operations for this key
				ctx1 := utils.ContextWithLSNAndCount(context.Background(), uint64(keyIndex*10+1), 3)
				ctx2 := utils.ContextWithLSNAndCount(context.Background(), uint64(keyIndex*10+2), 3)
				ctx3 := utils.ContextWithLSNAndCount(context.Background(), uint64(keyIndex*10+3), 3)

				var keyWg sync.WaitGroup
				keyWg.Add(3)

				// Submit operations for this key
				go func() {
					defer keyWg.Done()
					err := engine.Set(ctx3, key, "value3")
					assert.NoError(t, err)
				}()
				go func() {
					defer keyWg.Done()
					err := engine.Set(ctx1, key, "value1")
					assert.NoError(t, err)
				}()
				go func() {
					defer keyWg.Done()
					err := engine.Set(ctx2, key, "value2")
					assert.NoError(t, err)
				}()

				keyWg.Wait()

				// Verify final value is from highest LSN
				value, err := engine.Get(context.Background(), key)
				assert.NoError(t, err)
				assert.Equal(t, "value3", value)
			}(i)
		}

		wg.Wait()
	})

	t.Run("worker waits for all operations before processing", func(t *testing.T) {
		// Test that worker waits until it receives all expected operations for a key
		key := "wait_test_key"

		// We'll send operations with a delay to test that worker waits
		ctx1 := utils.ContextWithLSNAndCount(context.Background(), 1, 3)
		ctx2 := utils.ContextWithLSNAndCount(context.Background(), 2, 3)
		ctx3 := utils.ContextWithLSNAndCount(context.Background(), 3, 3)

		var wg sync.WaitGroup

		// Send first operation
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := engine.Set(ctx1, key, "first")
			assert.NoError(t, err)
		}()

		// Small delay to ensure first operation is received
		time.Sleep(10 * time.Millisecond)

		// Send remaining operations
		wg.Add(2)
		go func() {
			defer wg.Done()
			err := engine.Set(ctx3, key, "third")
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			err := engine.Set(ctx2, key, "second")
			assert.NoError(t, err)
		}()

		wg.Wait()

		// Final value should be from highest LSN (3)
		value, err := engine.Get(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, "third", value)
	})
}

func TestEngine_ContextHandling(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	t.Run("operations without context execute immediately", func(t *testing.T) {
		// Operations without LSN context should execute immediately
		ctx := context.Background() // No LSN or count

		err := engine.Set(ctx, "no_context_key", "no_context_value")
		assert.NoError(t, err)

		value, err := engine.Get(ctx, "no_context_key")
		assert.NoError(t, err)
		assert.Equal(t, "no_context_value", value)
	})

	t.Run("operations with LSN-only context execute immediately", func(t *testing.T) {
		// Operations with only LSN (no count) should execute immediately (recovery/replication case)
		ctx := utils.ContextWithLSN(context.Background(), 100)

		err := engine.Set(ctx, "lsn_only_key", "lsn_only_value")
		assert.NoError(t, err)

		value, err := engine.Get(context.Background(), "lsn_only_key")
		assert.NoError(t, err)
		assert.Equal(t, "lsn_only_value", value)
	})

	t.Run("operations with LSN and count=1 execute immediately", func(t *testing.T) {
		ctx := utils.ContextWithLSNAndCount(context.Background(), 200, 1)

		err := engine.Set(ctx, "count_one_key", "count_one_value")
		assert.NoError(t, err)

		value, err := engine.Get(context.Background(), "count_one_key")
		assert.NoError(t, err)
		assert.Equal(t, "count_one_value", value)
	})
}

func TestEngine_EdgeCases(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	defer engine.Shutdown()

	t.Run("very long key and value", func(t *testing.T) {
		longKey := strings.Repeat("a", 10000)
		longValue := strings.Repeat("b", 10000)

		ctx := getTestContext(1)
		err := engine.Set(ctx, longKey, longValue)
		assert.NoError(t, err)

		value, err := engine.Get(context.Background(), longKey)
		assert.NoError(t, err)
		assert.Equal(t, longValue, value)
	})

	t.Run("unicode keys and values", func(t *testing.T) {
		unicodeKey := "键"
		unicodeValue := "值"

		ctx := getTestContext(1)
		err := engine.Set(ctx, unicodeKey, unicodeValue)
		assert.NoError(t, err)

		value, err := engine.Get(context.Background(), unicodeKey)
		assert.NoError(t, err)
		assert.Equal(t, unicodeValue, value)
	})
}

func TestErrKeyNotFound(t *testing.T) {
	assert.NotNil(t, ErrKeyNotFound)
	assert.Equal(t, "key not found", ErrKeyNotFound.Error())
	assert.NotNil(t, ErrEmptyKey)
	assert.Equal(t, "key cannot be empty", ErrEmptyKey.Error())
	assert.NotNil(t, ErrEmptyValue)
	assert.Equal(t, "value cannot be empty", ErrEmptyValue.Error())
}
