package filesystem

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type FileMetadataManager struct {
	metadataFilePath string
	mu               sync.Mutex
	fd               *os.File
}

var globalManager *FileMetadataManager

func NewFileMetadataManager(metadataFilePath string) (*FileMetadataManager, error) {
	if globalManager != nil {
		return globalManager, nil
	}
	fd, err := os.OpenFile(metadataFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	globalManager = &FileMetadataManager{metadataFilePath: metadataFilePath, fd: fd}
	return globalManager, nil
}

func (m *FileMetadataManager) AddNewSegmentOffset(segmentFilename string, lsnStart, lsnEnd uint64) error {
	return concurrency.WithLock(
		&m.mu,
		func() error {
			_, err := m.fd.WriteString(segmentFilename + "," + strconv.FormatUint(lsnStart, 10) + "\n")
			if err != nil {
				return err
			}
			return m.fd.Sync()
		},
	)
}

func (m *FileMetadataManager) GetSegmentNameForLSN(lsn uint64) (filename string, err error) {
	defer func() {
		if v := recover(); v != nil {
			logging.Error("Recovered from panic", zap.Any("panic", v))
			err = fmt.Errorf("failed to get segment name for LSN: %v", v)
		}
	}()
	err = concurrency.WithLock(
		&m.mu,
		func() error {
			_, err := m.fd.Seek(0, 0)
			if err != nil {
				return err
			}
			scanner := bufio.NewScanner(m.fd)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.Split(line, ",")
				if len(parts) < 2 {
					continue
				}
				startLSN, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					continue
				}
				if startLSN > lsn {
					break
				}
				filename = parts[0]
			}
			// Reset the file pointer to the end of the file
			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
	return filename, err
}

func (m *FileMetadataManager) GetLastWALFileName() (string, error) {
	defer func() {
		if v := recover(); v != nil {
			logging.Error("Recovered from panic", zap.Any("panic", v))
		}
	}()
	
	var lastFilename string
	err := concurrency.WithLock(
		&m.mu,
		func() error {
			_, err := m.fd.Seek(0, 0)
			if err != nil {
				return err
			}
			
			scanner := bufio.NewScanner(m.fd)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.Split(line, ",")
				if len(parts) >= 1 && parts[0] != "" {
					lastFilename = parts[0]
				}
			}

			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
	return lastFilename, err
}

func (m *FileMetadataManager) GetWALFilenames() ([]string, error) {
	defer func() {
		if v := recover(); v != nil {
			logging.Error("Recovered from panic", zap.Any("panic", v))
		}
	}()
	
	var filenames []string
	err := concurrency.WithLock(
		&m.mu,
		func() error {
			_, err := m.fd.Seek(0, 0)
			if err != nil {
				return err
			}
			
			scanner := bufio.NewScanner(m.fd)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.Split(line, ",")
				if len(parts) >= 1 && parts[0] != "" {
					filenames = append(filenames, parts[0])
				}
			}

			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
	return filenames, err
}

func (m *FileMetadataManager) Close() error {
	return concurrency.WithLock(
		&m.mu,
		func() error {
			if m.fd != nil {
				return m.fd.Close()
			}
			return nil
		},
	)
}
