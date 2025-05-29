package in_memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngine(t *testing.T) {
	engine, err := NewEngine()

	assert.NoError(t, err)
	assert.NotNil(t, engine)
	assert.NotNil(t, engine.db)
	assert.Equal(t, 0, len(engine.db))
}

func TestEngine_Set(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("set new key-value pair", func(t *testing.T) {
		err := engine.Set(ctx, "key1", "value1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", engine.db["key1"])
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		err := engine.Set(ctx, "key1", "newvalue1")
		assert.NoError(t, err)
		assert.Equal(t, "newvalue1", engine.db["key1"])
	})

	t.Run("set empty value", func(t *testing.T) {
		err := engine.Set(ctx, "emptykey", "")
		assert.Error(t, err)
		assert.Equal(t, "value cannot be empty", err.Error())
	})

	t.Run("set empty key", func(t *testing.T) {
		err := engine.Set(ctx, "", "value")
		assert.Error(t, err)
		assert.Equal(t, "key cannot be empty", err.Error())
	})

	t.Run("set with special characters", func(t *testing.T) {
		key := "key with spaces and symbols!@#$%"
		value := "value with\nnewlines\tand\ttabs"
		err := engine.Set(ctx, key, value)
		assert.NoError(t, err)
		assert.Equal(t, value, engine.db[key])
	})
}

func TestEngine_Get(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("get existing key", func(t *testing.T) {
		engine.db["testkey"] = "testvalue"

		value, err := engine.Get(ctx, "testkey")
		assert.NoError(t, err)
		assert.Equal(t, "testvalue", value)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		value, err := engine.Get(ctx, "nonexistent")
		assert.Error(t, err)
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Equal(t, "", value)
	})

	t.Run("get key with special characters", func(t *testing.T) {
		key := "special!@#$%^&*()"
		expectedValue := "special value with unicode: 你好"
		engine.db[key] = expectedValue

		value, err := engine.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	})
}

func TestEngine_Delete(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("delete existing key", func(t *testing.T) {
		engine.db["deletekey"] = "deletevalue"

		err := engine.Delete(ctx, "deletekey")
		assert.NoError(t, err)

		_, exists := engine.db["deletekey"]
		assert.False(t, exists)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err := engine.Delete(ctx, "nonexistent")
		assert.NoError(t, err)
	})

	t.Run("delete key with special characters", func(t *testing.T) {
		key := "key with spaces and symbols!@#$%"
		engine.db[key] = "value"

		err := engine.Delete(ctx, key)
		assert.NoError(t, err)

		_, exists := engine.db[key]
		assert.False(t, exists)
	})
}

func TestEngine_ConcurrentAccess(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	ctx := context.Background()
	numGoroutines := 100
	numOperations := 10

	t.Run("concurrent sets", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("key_%d_%d", id, j)
					value := fmt.Sprintf("value_%d_%d", id, j)
					err := engine.Set(ctx, key, value)
					assert.NoError(t, err)
				}
			}(i)
		}

		wg.Wait()

		// Verify all keys were set
		assert.Equal(t, numGoroutines*numOperations, len(engine.db))
	})

	t.Run("concurrent gets and sets", func(t *testing.T) {
		// Pre-populate some data
		for i := 0; i < 50; i++ {
			engine.db[fmt.Sprintf("prekey_%d", i)] = fmt.Sprintf("prevalue_%d", i)
		}

		var wg sync.WaitGroup

		// Readers
		for i := 0; i < numGoroutines/2; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("prekey_%d", j%50)
					value, err := engine.Get(ctx, key)
					if err == nil {
						assert.Contains(t, value, "prevalue_")
					}
				}
			}(i)
		}

		// Writers
		for i := 0; i < numGoroutines/2; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("newkey_%d_%d", id, j)
					value := fmt.Sprintf("newvalue_%d_%d", id, j)
					err := engine.Set(ctx, key, value)
					assert.NoError(t, err)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent deletes", func(t *testing.T) {
		// Pre-populate data
		keys := make([]string, numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			key := fmt.Sprintf("delkey_%d", i)
			keys[i] = key
			engine.db[key] = fmt.Sprintf("delvalue_%d", i)
		}

		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				err := engine.Delete(ctx, keys[id])
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		// Verify all keys were deleted
		for _, key := range keys {
			_, exists := engine.db[key]
			assert.False(t, exists)
		}
	})
}

func TestEngine_EdgeCases(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("very long key and value", func(t *testing.T) {
		longKey := strings.Repeat("a", 10000)
		longValue := strings.Repeat("b", 10000)

		err := engine.Set(ctx, longKey, longValue)
		assert.NoError(t, err)

		value, err := engine.Get(ctx, longKey)
		assert.NoError(t, err)
		assert.Equal(t, longValue, value)
	})

	t.Run("unicode keys and values", func(t *testing.T) {
		unicodeKey := "键"
		unicodeValue := "值"

		err := engine.Set(ctx, unicodeKey, unicodeValue)
		assert.NoError(t, err)

		value, err := engine.Get(ctx, unicodeKey)
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
