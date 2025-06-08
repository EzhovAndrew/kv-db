package filesystem

import (
	"fmt"
	"os"
)

type Segment struct {
	FileName    string
	currentSize int
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
	s.currentSize = 0
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

func (s *Segment) setCurrentSize(size int) {
	s.currentSize = size
}

func (s *Segment) checkOverflow(maxSize, dataSize int) bool {
	return s.currentSize+dataSize >= maxSize
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

	s.currentSize += n
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
