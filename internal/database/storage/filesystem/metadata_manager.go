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

type WALMetadata struct {
	segmentFilename string
	lsnStart        uint64
}

func (w *WALMetadata) GetSegmentFilename() string {
	return w.segmentFilename
}

func (w *WALMetadata) GetLSNStart() uint64 {
	return w.lsnStart
}

var globalManager *FileMetadataManager
var ErrNoWALFilesFound = fmt.Errorf("no WAL files found")

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

func (m *FileMetadataManager) AddNewSegmentOffset(segmentFilename string, lsnStart uint64) error {
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

func (m *FileMetadataManager) GetSegmentMetadataForLSN(lsn uint64) (*WALMetadata, error) {
	var metadata *WALMetadata
	var err error

	defer func() {
		if v := recover(); v != nil {
			logging.Error("Recovered from panic", zap.Any("panic", v))
			err = fmt.Errorf("failed to get segment name for LSN: %v", v)
			metadata = nil
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
				if len(parts) != 2 {
					continue
				}

				startLSN, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					continue
				}

				// If this segment's start LSN is greater than our target LSN,
				// we've gone too far - the previous segment is the right one
				if startLSN > lsn {
					break
				}

				// This segment could contain our LSN
				metadata = &WALMetadata{
					segmentFilename: parts[0],
					lsnStart:        startLSN,
				}
			}

			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
	return metadata, err
}

func (m *FileMetadataManager) GetLastWALFileMetadata() (*WALMetadata, error) {
	var lastMetadata *WALMetadata
	var err error

	defer func() {
		if v := recover(); v != nil {
			logging.Error("Recovered from panic", zap.Any("panic", v))
			err = fmt.Errorf("failed to get last WAL filename: %v", v)
			lastMetadata = nil
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
				if len(parts) >= 2 && parts[0] != "" {
					startLSN, err := strconv.ParseUint(parts[1], 10, 64)
					if err != nil {
						continue
					}

					lastMetadata = &WALMetadata{
						segmentFilename: parts[0],
						lsnStart:        startLSN,
					}
				}
			}

			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
	if lastMetadata == nil {
		return nil, ErrNoWALFilesFound
	}
	return lastMetadata, err
}

func (m *FileMetadataManager) GetWALFilesMetadata() ([]*WALMetadata, error) {
	var metadataList []*WALMetadata
	var err error

	defer func() {
		if v := recover(); v != nil {
			logging.Error("Recovered from panic", zap.Any("panic", v))
			err = fmt.Errorf("failed to get WAL filenames: %v", v)
			metadataList = nil
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
				if len(parts) >= 2 && parts[0] != "" {
					startLSN, err := strconv.ParseUint(parts[1], 10, 64)
					if err != nil {
						continue
					}

					metadata := &WALMetadata{
						segmentFilename: parts[0],
						lsnStart:        startLSN,
					}
					metadataList = append(metadataList, metadata)
				}
			}

			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
	return metadataList, err
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
