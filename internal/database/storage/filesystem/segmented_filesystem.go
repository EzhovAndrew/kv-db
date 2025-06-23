package filesystem

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type MetadataManager interface {
	AddNewSegmentOffset(segmentFilename string, lsnStart uint64) error
	GetSegmentMetadataForLSN(lsn uint64) (*WALMetadata, error)
	GetLastWALFileMetadata() (*WALMetadata, error)
	GetWALFilesMetadata() ([]*WALMetadata, error)
}

type SegmentedFileSystem struct {
	dataDir        string
	maxSegmentSize int
	currentSegment *Segment
	mm             MetadataManager
}

func NewSegmentedFileSystem(dataDir string, maxSegmentSize int) *SegmentedFileSystem {
	// TODO: add option to config to choose metadata file path
	mm, err := NewFileMetadataManager(filepath.Join(dataDir, "metadata.txt"))
	if err != nil {
		logging.Fatal("Failed to initialize metadata manager", zap.Error(err))
	}
	s := &SegmentedFileSystem{
		dataDir:        dataDir,
		maxSegmentSize: maxSegmentSize,
		mm:             mm,
	}
	if err := s.createDir(); err != nil {
		logging.Fatal("Failed to create directory for WAL logs", zap.Error(err))
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
		// Last WAL file is too big, create a new segment, but at first get last LSN in existing wal file
		filePath := filepath.Join(fs.dataDir, lastWALFileMetadata.GetSegmentFilename())
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read file to get last LSN %s: %w", filePath, err)
		}
		lastLSN, err := encoders.GetLastLSNInData(data)
		if err != nil {
			return fmt.Errorf("failed to get last LSN while decoding in file %s: %w", filePath, err)
		}
		return fs.rotateSegment(lastLSN + 1)
	}

	return nil
}

func (fs *SegmentedFileSystem) canReuseSegment(walFilePath string) (bool, error) {
	fileInfo, err := os.Stat(walFilePath)
	if err != nil {
		return false, fmt.Errorf("failed to stat file %s: %w", walFilePath, err)
	}

	return fileInfo.Size() < int64(fs.maxSegmentSize), nil
}

func (fs *SegmentedFileSystem) reuseLastSegment(walFilePath string) error {
	segment := NewSegment(walFilePath)

	fileInfo, err := os.Stat(walFilePath)
	if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", walFilePath, err)
	}

	if err := segment.openForAppend(); err != nil {
		return fmt.Errorf("failed to open segment for append: %w", err)
	}

	// Set the current size based on existing file size
	segment.setCurrentSize(int(fileInfo.Size()))
	fs.currentSegment = segment

	return nil
}

func (fs *SegmentedFileSystem) WriteSync(data []byte, lsnStart uint64) error {
	if fs.currentSegment == nil || fs.currentSegment.checkOverflow(fs.maxSegmentSize, len(data)) {
		err := fs.rotateSegment(lsnStart)
		if err != nil {
			return err
		}
	}
	return fs.currentSegment.writeSync(data)
}

func (fs *SegmentedFileSystem) ReadAll() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		walFiles, err := fs.mm.GetWALFilesMetadata()
		if err != nil {
			yield(nil, err)
			return
		}
		for _, fileMetadata := range walFiles {
			filePath := filepath.Join(fs.dataDir, fileMetadata.GetSegmentFilename())
			data, err := os.ReadFile(filePath)
			if err != nil {
				if !yield(nil, fmt.Errorf("failed to read file %s: %w", filePath, err)) {
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
	if fs.currentSegment != nil {
		err := fs.currentSegment.close()
		if err != nil {
			return err
		}
	}
	newFileName := generateFileName(fs.dataDir)
	newSegment := NewSegment(newFileName)
	err := newSegment.open()
	if err != nil {
		return err
	}
	fs.currentSegment = newSegment
	err = fs.mm.AddNewSegmentOffset(newFileName, startLsn)
	if err != nil {
		errC := fs.currentSegment.close()
		if errC != nil {
			logging.Error("Failed to close segment after not success metadata appending", zap.Error(errC))
		}
		return err
	}
	return nil
}

func (fs *SegmentedFileSystem) createDir() error {
	return os.MkdirAll(fs.dataDir, 0755)
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

			if isCurrentSegment {
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
					// Switch to next segment
					currentSegment = nextSegment
					offset = 0
					continue
				}

				// No next segment, wait for more data
				time.Sleep(1 * time.Millisecond)

			} else {
				// For non-current segments, read the entire remaining part at once
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

				// Move to next segment
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
		}
	}
}

// Check if the given segment is the current active segment
func (fs *SegmentedFileSystem) isCurrentSegment(segmentName string) bool {
	// We need to protect access to currentSegment, but we want to minimize lock time
	// Since we only read a pointer/string, we can use a quick check
	if fs.currentSegment == nil {
		return false
	}

	// Extract just the filename from the full path for comparison
	currentFileName := filepath.Base(fs.currentSegment.FileName)
	return currentFileName == segmentName
}

// Read from current segment incrementally, respecting the current written size
func (fs *SegmentedFileSystem) readCurrentSegmentIncremental(segmentName string, offset int64) ([]byte, int64, error) {
	segmentPath := filepath.Join(fs.dataDir, segmentName)

	// Get current size of the segment to avoid partial reads
	var currentSize int64
	if fs.currentSegment != nil && filepath.Base(fs.currentSegment.FileName) == segmentName {
		// For current segment, use the tracked size to avoid reading partial writes
		currentSize = int64(fs.currentSegment.currentSize)
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
	if readSize <= 0 {
		return nil, offset, nil
	}

	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to open current segment %s: %w", segmentPath, err)
	}
	defer file.Close()

	data := make([]byte, readSize)
	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, offset, fmt.Errorf("failed to read from current segment %s: %w", segmentPath, err)
	}

	return data[:n], offset + int64(n), nil
}

// Read complete segment from offset (for non-current segments)
func (fs *SegmentedFileSystem) readCompleteSegmentFromOffset(segmentName string, offset int64) ([]byte, error) {
	segmentPath := filepath.Join(fs.dataDir, segmentName)

	// For non-current segments, we can read the entire remaining file
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", segmentPath, err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat segment %s: %w", segmentPath, err)
	}

	fileSize := fileInfo.Size()
	if offset >= fileSize {
		return nil, nil // No data to read
	}

	// Read all remaining data from offset
	remainingSize := fileSize - offset
	data := make([]byte, remainingSize)

	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read complete segment %s: %w", segmentPath, err)
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
