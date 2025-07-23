package filesystem

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type MetadataManager interface {
	AddNewSegmentOffset(segmentFilename string, lsnStart uint64) error
	GetSegmentMetadataForLSN(lsn uint64) (*WALMetadata, error)
	GetLastWALFileMetadata() (*WALMetadata, error)
	GetWALFilesMetadata() ([]*WALMetadata, error)
	Close() error
}

type SegmentedFileSystem struct {
	dataDir        string
	maxSegmentSize int
	currentSegment atomic.Pointer[Segment]
	mm             MetadataManager
	readBufferSize int
}

func createDir(dataDir string) error {
	return os.MkdirAll(dataDir, 0755)
}

func NewSegmentedFileSystem(dataDir string, maxSegmentSize int) *SegmentedFileSystem {
	if err := createDir(dataDir); err != nil {
		logging.Fatal("Failed to create directory for WAL logs", zap.Error(err))
	}
	mm, err := NewFileMetadataManager(filepath.Join(dataDir, "metadata.txt"))
	if err != nil {
		logging.Fatal("Failed to initialize metadata manager", zap.Error(err))
	}
	s := &SegmentedFileSystem{
		dataDir:        dataDir,
		maxSegmentSize: maxSegmentSize,
		mm:             mm,
		readBufferSize: 64 * 1024,
	}
	if err := s.initializeSegments(); err != nil {
		logging.Fatal("Failed to initialize segments", zap.Error(err))
	}

	return s
}

func (fs *SegmentedFileSystem) initializeSegments() error {
	lastWALFileMetadata, err := fs.mm.GetLastWALFileMetadata()
	if errors.Is(err, ErrNoWALFilesFound) {
		return fs.rotateSegment(uint64(0))
	}
	if err != nil {
		return fmt.Errorf("failed to discover WAL files: %w", err)
	}
	canReuseLastSegment, err := fs.canReuseSegment(lastWALFileMetadata.GetSegmentFilename())
	if err != nil {
		return fmt.Errorf("failed to check if last segment can be reused: %w", err)
	}

	if canReuseLastSegment {
		// Reuse the last WAL file as current segment if it's not too big
		if err := fs.reuseLastSegment(lastWALFileMetadata.GetSegmentFilename()); err != nil {
			return fmt.Errorf("failed to reuse last segment: %w", err)
		}
	} else {
		// Last WAL file is too big, create a new segment
		// Get the last valid LSN from the segment
		nextLSN, err := fs.getNextValidLSN(lastWALFileMetadata.GetSegmentFilename())
		if err != nil {
			return fmt.Errorf("failed to determine next LSN: %w", err)
		}
		return fs.rotateSegment(nextLSN)
	}

	return nil
}

func (fs *SegmentedFileSystem) getNextValidLSN(segmentFilename string) (uint64, error) {
	filePath := filepath.Join(fs.dataDir, segmentFilename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read file to get last LSN %s: %w", filePath, err)
	}

	if len(data) == 0 {
		return 0, nil
	}

	lastLSN, err := encoders.GetLastLSNInData(data)
	if err != nil {
		logging.Warn("Failed to decode LSN from segment data, using metadata LSN as fallback",
			zap.String("segment", segmentFilename),
			zap.Error(err))

		return fs.getHighestKnownLSN() + 10000, nil
	}

	return lastLSN + 1, nil
}

func (fs *SegmentedFileSystem) getHighestKnownLSN() uint64 {
	walFiles, err := fs.mm.GetWALFilesMetadata()
	if err != nil {
		return 1000000
	}

	var maxLSN uint64
	for _, metadata := range walFiles {
		if metadata.GetLSNStart() > maxLSN {
			maxLSN = metadata.GetLSNStart()
		}
	}

	return maxLSN
}

func (fs *SegmentedFileSystem) canReuseSegment(segmentFilename string) (bool, error) {
	segmentPath := filepath.Join(fs.dataDir, segmentFilename)
	fileInfo, err := os.Stat(segmentPath)
	if err != nil {
		return false, fmt.Errorf("failed to stat file %s: %w", segmentPath, err)
	}

	return fileInfo.Size() < int64(fs.maxSegmentSize), nil
}

func (fs *SegmentedFileSystem) reuseLastSegment(segmentFilename string) error {
	segmentPath := filepath.Join(fs.dataDir, segmentFilename)
	segment := NewSegment(segmentPath)

	fileInfo, err := os.Stat(segmentPath)
	if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", segmentPath, err)
	}

	if err := segment.openForAppend(); err != nil {
		return fmt.Errorf("failed to open segment for append: %w", err)
	}

	segment.setCurrentSize(fileInfo.Size())
	fs.currentSegment.Store(segment)

	return nil
}

func (fs *SegmentedFileSystem) WriteSync(data []byte, lsnStart uint64) error {
	currentSegment := fs.currentSegment.Load()
	if currentSegment == nil || currentSegment.checkOverflow(fs.maxSegmentSize, len(data)) {
		err := fs.rotateSegment(lsnStart)
		if err != nil {
			return err
		}
		currentSegment = fs.currentSegment.Load()
	}
	return currentSegment.writeSync(data)
}

func (fs *SegmentedFileSystem) ReadAll() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		walFiles, err := fs.mm.GetWALFilesMetadata()
		if err != nil {
			yield(nil, err)
			return
		}
		for _, fileMetadata := range walFiles {
			segmentPath := filepath.Join(fs.dataDir, fileMetadata.GetSegmentFilename())
			data, err := os.ReadFile(segmentPath)
			if err != nil {
				if !yield(nil, fmt.Errorf("failed to read file %s: %w", segmentPath, err)) {
					return
				}
				continue
			}
			if !yield(data, nil) {
				return
			}
		}
	}
}

func (fs *SegmentedFileSystem) rotateSegment(startLsn uint64) error {
	currentSegment := fs.currentSegment.Load()
	if currentSegment != nil {
		err := currentSegment.close()
		if err != nil {
			return err
		}
	}
	fileName := generateFileName()
	newFilePath := filepath.Join(fs.dataDir, fileName)
	newSegment := NewSegment(newFilePath)
	err := newSegment.open()
	if err != nil {
		return err
	}

	// Write metadata BEFORE atomically storing the segment to ensure crash consistency
	err = fs.mm.AddNewSegmentOffset(fileName, startLsn)
	if err != nil {
		errC := newSegment.close()
		if errC != nil {
			logging.Error("Failed to close segment after metadata write failure", zap.Error(errC))
		}
		// Clean up the orphaned file
		if removeErr := os.Remove(newFilePath); removeErr != nil {
			logging.Error("Failed to remove orphaned segment file",
				zap.String("filePath", newFilePath),
				zap.Error(removeErr))
		}
		return err
	}
	// Only store atomically after metadata is successfully written
	fs.currentSegment.Store(newSegment)
	return nil
}

// Close properly shuts down the segmented filesystem
func (fs *SegmentedFileSystem) Close() error {
	currentSegment := fs.currentSegment.Load()
	if currentSegment != nil {
		if err := currentSegment.close(); err != nil {
			logging.Error("Failed to close current segment during shutdown", zap.Error(err))
		}
	}

	if err := fs.mm.Close(); err != nil {
		logging.Error("Failed to close metadata manager", zap.Error(err))
		return err
	}

	return nil
}

func (fs *SegmentedFileSystem) GetSegmentForLSN(lsn uint64) (string, error) {
	fileMetadata, err := fs.mm.GetSegmentMetadataForLSN(lsn)
	if err != nil {
		return "", err
	}
	return fileMetadata.GetSegmentFilename(), nil
}

func (fs *SegmentedFileSystem) ReadContinuouslyFromSegment(segment string) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		currentSegment := segment
		var offset int64 = 0

		for {
			isCurrentSegment := fs.isCurrentSegment(currentSegment)

			if !isCurrentSegment {
				// For non-current segments, read the entire segment
				data, err := fs.readCompleteSegmentFromOffset(currentSegment, offset)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					return
				}

				if len(data) > 0 {
					if !yield(data, nil) {
						return
					}
				}

				nextSegment, switchErr := fs.getNextSegment(currentSegment)
				if switchErr == nil && nextSegment != "" {
					currentSegment = nextSegment
					offset = 0
					continue
				}

				// No next segment found, this shouldn't happen for non-current segments
				if !yield(nil, fmt.Errorf("no next segment found after non-current segment %s", currentSegment)) {
					return
				}
				return
			}

			// For current segment, read incrementally with size checking
			data, newOffset, err := fs.readCurrentSegmentIncremental(currentSegment, offset)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				return
			}

			if len(data) > 0 {
				if !yield(data, nil) {
					return
				}
				offset = newOffset
				continue // Continue reading immediately when we have data
			}

			// No new data in current segment, check for next segment
			nextSegment, switchErr := fs.getNextSegment(currentSegment)
			if switchErr == nil && nextSegment != "" {
				currentSegment = nextSegment
				offset = 0
				continue
			}

			// No next segment, wait for more data
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// Check if the given segment is the current active segment
func (fs *SegmentedFileSystem) isCurrentSegment(segmentName string) bool {
	currentSegment := fs.currentSegment.Load()
	if currentSegment == nil {
		return false
	}

	// Extract just the filename from the full path for comparison
	currentFileName := filepath.Base(currentSegment.FileName)
	return currentFileName == segmentName
}

// Read from current segment incrementally, respecting the current written size
func (fs *SegmentedFileSystem) readCurrentSegmentIncremental(segmentName string, offset int64) ([]byte, int64, error) {
	segmentPath := filepath.Join(fs.dataDir, segmentName)

	// Get current size of the segment to avoid partial reads
	var currentSize int64
	currentSegment := fs.currentSegment.Load()
	if currentSegment != nil && filepath.Base(currentSegment.FileName) == segmentName {
		// For current segment, use the tracked size to avoid reading partial writes
		currentSize = currentSegment.getCurrentSize()
	} else {
		// Fallback to file stat if we can't get tracked size
		fileInfo, err := os.Stat(segmentPath)
		if err != nil {
			return nil, offset, fmt.Errorf("failed to stat current segment %s: %w", segmentPath, err)
		}
		currentSize = fileInfo.Size()
	}

	// If offset is already at or beyond current size, no new data
	if offset >= currentSize {
		return nil, offset, nil
	}

	// Read only up to the current size to avoid partial reads
	readSize := currentSize - offset
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to open current segment %s: %w", segmentPath, err)
	}
	defer file.Close() // nolint:errcheck

	data := make([]byte, readSize)
	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, offset, fmt.Errorf("failed to read from current segment %s: %w", segmentPath, err)
	}

	return data[:n], offset + int64(n), nil
}

// Read complete segment from offset using streaming approach for large files
func (fs *SegmentedFileSystem) readCompleteSegmentFromOffset(segmentName string, offset int64) ([]byte, error) {
	segmentPath := filepath.Join(fs.dataDir, segmentName)

	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", segmentPath, err)
	}
	defer file.Close() // nolint:errcheck

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat segment %s: %w", segmentPath, err)
	}

	fileSize := fileInfo.Size()
	if offset >= fileSize {
		return nil, nil // No data to read
	}

	// For small remaining data, read all at once
	remainingSize := fileSize - offset
	chunkSize := min(int64(fs.readBufferSize), remainingSize)
	data := make([]byte, chunkSize)
	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read chunk from %s: %w", segmentPath, err)
	}

	return data[:n], nil
}

// Helper function to get the next segment in sequence
func (fs *SegmentedFileSystem) getNextSegment(currentSegment string) (string, error) {
	walFiles, err := fs.mm.GetWALFilesMetadata()
	if err != nil {
		return "", err
	}

	// Find current segment index
	currentIndex := -1
	for i, metadata := range walFiles {
		if metadata.GetSegmentFilename() == currentSegment {
			currentIndex = i
			break
		}
	}

	// Return next segment if exists
	if currentIndex >= 0 && currentIndex < len(walFiles)-1 {
		return walFiles[currentIndex+1].GetSegmentFilename(), nil
	}

	return "", fmt.Errorf("no next segment found")
}
