package filesystem

import (
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type SegmentedFileSystem struct {
	dataDir        string
	maxSegmentSize int
	currentSegment *Segment
	walFiles       []string
}

func NewSegmentedFileSystem(dataDir string, maxSegmentSize int) *SegmentedFileSystem {
	s := &SegmentedFileSystem{
		dataDir:        dataDir,
		maxSegmentSize: maxSegmentSize,
		walFiles:       make([]string, 0),
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
	walFiles, err := fs.discoverAndSortWALFiles()
	if err != nil {
		return fmt.Errorf("failed to discover WAL files: %w", err)
	}

	fs.walFiles = walFiles

	if len(walFiles) == 0 {
		return fs.rotateSegment()
	}
	lastWALFile := walFiles[len(walFiles)-1]
	lastWALPath := filepath.Join(fs.dataDir, lastWALFile)

	canReuseLastSegment, err := fs.canReuseSegment(lastWALPath)
	if err != nil {
		return fmt.Errorf("failed to check if last segment can be reused: %w", err)
	}

	if canReuseLastSegment {
		// Reuse the last WAL file as current segment if it's not too big
		if err := fs.reuseLastSegment(lastWALPath); err != nil {
			return fmt.Errorf("failed to reuse last segment: %w", err)
		}
	} else {
		// Last WAL file is too big, create a new segment
		return fs.rotateSegment()
	}

	return nil
}

func (fs *SegmentedFileSystem) discoverAndSortWALFiles() ([]string, error) {
	entries, err := os.ReadDir(fs.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", fs.dataDir, err)
	}

	return fs.sortWALFiles(entries), nil
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

func (fs *SegmentedFileSystem) WriteSync(data []byte) error {
	if fs.currentSegment == nil || fs.currentSegment.checkOverflow(fs.maxSegmentSize, len(data)) {
		err := fs.rotateSegment()
		if err != nil {
			return err
		}
	}
	return fs.currentSegment.writeSync(data)
}

func (fs *SegmentedFileSystem) ReadAll() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		defer func() {
			// clean up unneccessary memory
			fs.walFiles = nil
		}()
		for _, filename := range fs.walFiles {
			filePath := filepath.Join(fs.dataDir, filename)
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

func (fs *SegmentedFileSystem) sortWALFiles(entries []os.DirEntry) []string {
	var walFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Check if file matches pattern of wal files
		if checkFileName(name) {
			walFiles = append(walFiles, name)
		}
	}

	// Sort files by timestamp (extracted from filename)
	sort.Slice(walFiles, func(i, j int) bool {
		// Extract timestamps from filenames
		timestampI := walFiles[i][4 : len(walFiles[i])-4] // Remove "wal_" prefix and ".log" suffix
		timestampJ := walFiles[j][4 : len(walFiles[j])-4]

		// Convert to integers for proper numeric comparison
		tsI, errI := strconv.ParseInt(timestampI, 10, 64)
		tsJ, errJ := strconv.ParseInt(timestampJ, 10, 64)

		if errI != nil || errJ != nil {
			// Fallback to string comparison if parsing fails
			return timestampI < timestampJ
		}

		return tsI < tsJ
	})
	return walFiles
}

func (fs *SegmentedFileSystem) rotateSegment() error {
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
	return nil
}

func (fs *SegmentedFileSystem) createDir() error {
	return os.MkdirAll(fs.dataDir, 0755)
}
