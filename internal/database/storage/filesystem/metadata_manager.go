package filesystem

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type FileMetadataManager struct {
	metadataFilePath string
	mu               sync.Mutex
	fd               *os.File

	metadataCache atomic.Pointer[[]*WALMetadata]
	cacheValid    atomic.Bool
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

var ErrNoWALFilesFound = fmt.Errorf("no WAL files found")

func NewFileMetadataManager(metadataFilePath string) (*FileMetadataManager, error) {
	fd, err := os.OpenFile(metadataFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file %s: %w", metadataFilePath, err)
	}

	manager := &FileMetadataManager{
		metadataFilePath: metadataFilePath,
		fd:               fd,
	}

	if err := manager.refreshCache(); err != nil {
		logging.Warn("Failed to pre-load metadata cache", zap.Error(err))
	}

	return manager, nil
}

func (m *FileMetadataManager) AddNewSegmentOffset(segmentFilename string, lsnStart uint64) error {
	return concurrency.WithLock(
		&m.mu,
		func() error {
			_, err := m.fd.WriteString(segmentFilename + "," + strconv.FormatUint(lsnStart, 10) + "\n")
			if err != nil {
				return err
			}
			if err := m.fd.Sync(); err != nil {
				return err
			}

			m.invalidateCache()
			return nil
		},
	)
}

func (m *FileMetadataManager) refreshCache() error {
	return concurrency.WithLock(
		&m.mu,
		func() error {
			// Seek to beginning for reading
			_, err := m.fd.Seek(0, 0)
			if err != nil {
				return err
			}

			var metadataList []*WALMetadata
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

			// Update cache atomically
			m.metadataCache.Store(&metadataList)
			m.cacheValid.Store(true)

			// Seek back to end for appending
			_, err = m.fd.Seek(0, 2)
			return err
		},
	)
}

func (m *FileMetadataManager) getCachedMetadata() ([]*WALMetadata, error) {
	if !m.cacheValid.Load() {
		if err := m.refreshCache(); err != nil {
			return nil, err
		}
	}

	cached := m.metadataCache.Load()
	if cached == nil {
		return nil, fmt.Errorf("cache is nil after refresh")
	}

	// Return a copy to prevent external modifications
	result := make([]*WALMetadata, len(*cached))
	copy(result, *cached)
	return result, nil
}

func (m *FileMetadataManager) invalidateCache() {
	m.cacheValid.Store(false)
}

func (m *FileMetadataManager) GetSegmentMetadataForLSN(lsn uint64) (*WALMetadata, error) {
	metadataList, err := m.getCachedMetadata()
	if err != nil {
		return nil, err
	}

	var result *WALMetadata
	for _, metadata := range metadataList {
		// If this segment's start LSN is greater than our target LSN,
		// we've gone too far - the previous segment is the right one
		if metadata.GetLSNStart() > lsn {
			break
		}
		// This segment could contain our LSN
		result = metadata
	}

	return result, nil
}

func (m *FileMetadataManager) GetLastWALFileMetadata() (*WALMetadata, error) {
	metadataList, err := m.getCachedMetadata()
	if err != nil {
		return nil, err
	}

	if len(metadataList) == 0 {
		return nil, ErrNoWALFilesFound
	}

	// Return the last metadata entry
	return metadataList[len(metadataList)-1], nil
}

func (m *FileMetadataManager) GetWALFilesMetadata() ([]*WALMetadata, error) {
	return m.getCachedMetadata()
}

func (m *FileMetadataManager) Close() error {
	return concurrency.WithLock(
		&m.mu,
		func() error {
			if m.fd != nil {
				err := m.fd.Close()
				if err != nil {
					return err
				}
				m.fd = nil
			}
			return nil
		},
	)
}
