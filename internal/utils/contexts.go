package utils

import (
	"context"
	"errors"
)

type contextKey string

var ErrLSNNotFound = errors.New("lsn not found in context")

const lsnKey contextKey = "lsn"

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
