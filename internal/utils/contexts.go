package utils

import (
	"context"
	"errors"
)

type contextKey string

var (
	ErrLSNNotFound   = errors.New("lsn not found in context")
	ErrCountNotFound = errors.New("count not found in context")
)

const (
	lsnKey   contextKey = "lsn"
	countKey contextKey = "count"
)

func ContextWithLSN(ctx context.Context, lsn uint64) context.Context {
	return context.WithValue(ctx, lsnKey, lsn)
}

func LSNFromContext(ctx context.Context) (uint64, bool) {
	lsn, ok := ctx.Value(lsnKey).(uint64)
	return lsn, ok
}

func MustLSNFromContext(ctx context.Context) (uint64, error) {
	lsn, ok := LSNFromContext(ctx)
	if !ok {
		return 0, ErrLSNNotFound
	}
	return lsn, nil
}

// Count context functions
func ContextWithCount(ctx context.Context, count int) context.Context {
	return context.WithValue(ctx, countKey, count)
}

func CountFromContext(ctx context.Context) (int, bool) {
	count, ok := ctx.Value(countKey).(int)
	return count, ok
}

func MustCountFromContext(ctx context.Context) (int, error) {
	count, ok := CountFromContext(ctx)
	if !ok {
		return 0, ErrCountNotFound
	}
	return count, nil
}

// Combined LSN and Count context functions
func ContextWithLSNAndCount(ctx context.Context, lsn uint64, count int) context.Context {
	ctx = context.WithValue(ctx, lsnKey, lsn)
	ctx = context.WithValue(ctx, countKey, count)
	return ctx
}

func LSNAndCountFromContext(ctx context.Context) (uint64, int, bool) {
	lsn, lsnOk := LSNFromContext(ctx)
	count, countOk := CountFromContext(ctx)
	return lsn, count, lsnOk && countOk
}

func MustLSNAndCountFromContext(ctx context.Context) (uint64, int, error) {
	lsn, count, ok := LSNAndCountFromContext(ctx)
	if !ok {
		if _, lsnOk := LSNFromContext(ctx); !lsnOk {
			return 0, 0, ErrLSNNotFound
		}
		return 0, 0, ErrCountNotFound
	}
	return lsn, count, nil
}
