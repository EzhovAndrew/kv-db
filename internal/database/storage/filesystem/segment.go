package filesystem

import (
	"fmt"
	"os"
	"sync/atomic"
)

type Segment struct {
	FileName    string
	currentSize atomic.Int64
	fd          *os.File
}

func NewSegment(fileName string) *Segment {
	return &Segment{
		FileName: fileName,
	}
}

func (s *Segment) open() error {
	fd, err := os.OpenFile(s.FileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", s.FileName, err)
	}
	s.fd = fd
	s.currentSize.Store(0)
	return nil
}

func (s *Segment) openForAppend() error {
	fd, err := os.OpenFile(s.FileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s for append: %w", s.FileName, err)
	}
	s.fd = fd
	return nil
}

func (s *Segment) setCurrentSize(size int64) {
	s.currentSize.Store(size)
}

func (s *Segment) getCurrentSize() int64 {
	return s.currentSize.Load()
}

func (s *Segment) checkOverflow(maxSize, dataSize int) bool {
	currentSize := s.currentSize.Load()
	return currentSize+int64(dataSize) >= int64(maxSize)
}

func (s *Segment) writeSync(data []byte) error {
	if s.fd == nil {
		return fmt.Errorf("segment file is not open")
	}

	n, err := s.fd.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := s.fd.Sync(); err != nil {
		return fmt.Errorf("failed to sync data: %w", err)
	}

	s.currentSize.Add(int64(n))
	return nil
}

func (s *Segment) close() error {
	if s.fd == nil {
		return nil // Already closed
	}
	err := s.fd.Close()
	if err != nil {
		return fmt.Errorf("failed to close file %s: %w", s.FileName, err)
	}
	s.fd = nil
	return nil
}
