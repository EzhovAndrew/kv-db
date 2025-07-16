package wal

import "sync/atomic"

type LSNGenerator struct {
	lsn atomic.Uint64
}

func NewLSNGenerator(lastLSN uint64) *LSNGenerator {
	g := &LSNGenerator{}
	if lastLSN != 0 {
		g.lsn.Store(lastLSN)
	}
	return g
}

func (g *LSNGenerator) ResetToLSN(lsn uint64) {
	g.lsn.Store(lsn)
}

func (g *LSNGenerator) Next() uint64 {
	return g.lsn.Add(1)
}

func (g *LSNGenerator) Current() uint64 {
	return g.lsn.Load()
}
