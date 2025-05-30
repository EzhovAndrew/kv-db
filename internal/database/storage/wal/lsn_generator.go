package wal

import "sync/atomic"

type LSNGenerator struct {
	lsn atomic.Uint64
}

func NewLSNGenerator(lastLSN uint64) *LSNGenerator {
	var lsn atomic.Uint64
	if lastLSN != 0 {
		lsn.Store(lastLSN)
	}
	return &LSNGenerator{lsn: lsn}
}

func (g *LSNGenerator) Next() uint64 {
	return g.lsn.Add(1)
}
