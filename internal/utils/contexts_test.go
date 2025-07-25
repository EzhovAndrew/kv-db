package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContextWithCount(t *testing.T) {
	ctx := context.Background()
	count := 42

	newCtx := ContextWithCount(ctx, count)

	retrievedCount, ok := CountFromContext(newCtx)
	assert.True(t, ok)
	assert.Equal(t, count, retrievedCount)
}

func TestCountFromContext_NotFound(t *testing.T) {
	ctx := context.Background()

	count, ok := CountFromContext(ctx)
	assert.False(t, ok)
	assert.Equal(t, 0, count)
}

func TestMustCountFromContext_Success(t *testing.T) {
	ctx := context.Background()
	count := 123

	newCtx := ContextWithCount(ctx, count)

	retrievedCount, err := MustCountFromContext(newCtx)
	assert.NoError(t, err)
	assert.Equal(t, count, retrievedCount)
}

func TestMustCountFromContext_Error(t *testing.T) {
	ctx := context.Background()

	count, err := MustCountFromContext(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrCountNotFound, err)
	assert.Equal(t, 0, count)
}

func TestContextWithLSNAndCount(t *testing.T) {
	ctx := context.Background()
	lsn := uint64(100)
	count := 5

	newCtx := ContextWithLSNAndCount(ctx, lsn, count)

	// Verify LSN
	retrievedLSN, lsnOk := LSNFromContext(newCtx)
	assert.True(t, lsnOk)
	assert.Equal(t, lsn, retrievedLSN)

	// Verify Count
	retrievedCount, countOk := CountFromContext(newCtx)
	assert.True(t, countOk)
	assert.Equal(t, count, retrievedCount)
}

func TestLSNAndCountFromContext_Success(t *testing.T) {
	ctx := context.Background()
	lsn := uint64(200)
	count := 10

	newCtx := ContextWithLSNAndCount(ctx, lsn, count)

	retrievedLSN, retrievedCount, ok := LSNAndCountFromContext(newCtx)
	assert.True(t, ok)
	assert.Equal(t, lsn, retrievedLSN)
	assert.Equal(t, count, retrievedCount)
}

func TestLSNAndCountFromContext_MissingLSN(t *testing.T) {
	ctx := context.Background()
	count := 15

	newCtx := ContextWithCount(ctx, count)

	lsn, retrievedCount, ok := LSNAndCountFromContext(newCtx)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), lsn)
	assert.Equal(t, count, retrievedCount)
}

func TestLSNAndCountFromContext_MissingCount(t *testing.T) {
	ctx := context.Background()
	lsn := uint64(300)

	newCtx := ContextWithLSN(ctx, lsn)

	retrievedLSN, count, ok := LSNAndCountFromContext(newCtx)
	assert.False(t, ok)
	assert.Equal(t, lsn, retrievedLSN)
	assert.Equal(t, 0, count)
}

func TestLSNAndCountFromContext_Both_Missing(t *testing.T) {
	ctx := context.Background()

	lsn, count, ok := LSNAndCountFromContext(ctx)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), lsn)
	assert.Equal(t, 0, count)
}

func TestMustLSNAndCountFromContext_Success(t *testing.T) {
	ctx := context.Background()
	lsn := uint64(400)
	count := 20

	newCtx := ContextWithLSNAndCount(ctx, lsn, count)

	retrievedLSN, retrievedCount, err := MustLSNAndCountFromContext(newCtx)
	assert.NoError(t, err)
	assert.Equal(t, lsn, retrievedLSN)
	assert.Equal(t, count, retrievedCount)
}

func TestMustLSNAndCountFromContext_MissingLSN(t *testing.T) {
	ctx := context.Background()
	count := 25

	newCtx := ContextWithCount(ctx, count)

	lsn, retrievedCount, err := MustLSNAndCountFromContext(newCtx)
	assert.Error(t, err)
	assert.Equal(t, ErrLSNNotFound, err)
	assert.Equal(t, uint64(0), lsn)
	assert.Equal(t, 0, retrievedCount)
}

func TestMustLSNAndCountFromContext_MissingCount(t *testing.T) {
	ctx := context.Background()
	lsn := uint64(500)

	newCtx := ContextWithLSN(ctx, lsn)

	retrievedLSN, count, err := MustLSNAndCountFromContext(newCtx)
	assert.Error(t, err)
	assert.Equal(t, ErrCountNotFound, err)
	assert.Equal(t, uint64(0), retrievedLSN)
	assert.Equal(t, 0, count)
}

func TestMustLSNAndCountFromContext_BothMissing(t *testing.T) {
	ctx := context.Background()

	lsn, count, err := MustLSNAndCountFromContext(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrLSNNotFound, err)
	assert.Equal(t, uint64(0), lsn)
	assert.Equal(t, 0, count)
}
