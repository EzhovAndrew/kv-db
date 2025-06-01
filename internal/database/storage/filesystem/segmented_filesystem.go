package filesystem

import (
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
}

func NewSegmentedFileSystem(dataDir string, maxSegmentSize int) *SegmentedFileSystem {
	s := &SegmentedFileSystem{dataDir: dataDir, maxSegmentSize: maxSegmentSize}
	err := s.createDir()
	if err != nil {
		logging.Fatal("Failed to create directory for WAL logs", zap.Error(err))
	}
	err = s.rotateSegment()
	if err != nil {
		logging.Fatal("Failed to rotate initial segment", zap.Error(err))
	}
	return s
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
		entries, err := os.ReadDir(fs.dataDir)
		if err != nil {
			yield(nil, err)
			return
		}

		// Filter and sort WAL files by timestamp
		var walFiles = fs.sortWALFiles(entries)

		for _, filename := range walFiles {
			filepath := filepath.Join(fs.dataDir, filename)
			data, err := os.ReadFile(filepath)
			if err != nil {
				if !yield(nil, err) {
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
