package filesystem

import "os"

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
	fd, err := os.OpenFile(s.FileName, os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	s.fd = fd
	return nil
}

func (s *Segment) checkOverflow(maxSize, dataSize int) bool {
	return s.currentSize+dataSize >= maxSize
}

func (s *Segment) writeSync(data []byte) error {
	if s.fd == nil {
		return os.ErrClosed
	}
	_, err := s.fd.Write(data)
	if err != nil {
		return err
	}
	err = s.fd.Sync()
	if err != nil {
		return err
	}
	s.currentSize += len(data)
	return nil
}

func (s *Segment) close() error {
	if s.fd == nil {
		return os.ErrClosed
	}
	err := s.fd.Close()
	if err != nil {
		return err
	}
	s.fd = nil
	return nil
}
