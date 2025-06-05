package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSegmentedFileSystem_EmptyDirectory(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 1024)

	assert.NotNil(t, fs)
	assert.Equal(t, tempDir, fs.dataDir)
	assert.Equal(t, 1024, fs.maxSegmentSize)
	assert.NotNil(t, fs.currentSegment)
	assert.Empty(t, fs.walFiles)
}

func TestNewSegmentedFileSystem_WithExistingSmallWALFile(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create a small existing WAL file
	existingFile := filepath.Join(tempDir, "wal_1234567890.log")
	err := os.WriteFile(existingFile, []byte("small content"), 0644)
	require.NoError(t, err)

	fs := NewSegmentedFileSystem(tempDir, 1024)

	assert.NotNil(t, fs)
	assert.Len(t, fs.walFiles, 1)
	assert.Equal(t, "wal_1234567890.log", fs.walFiles[0])
	assert.NotNil(t, fs.currentSegment)
	assert.Equal(t, existingFile, fs.currentSegment.FileName)
}

func TestNewSegmentedFileSystem_WithExistingLargeWALFile(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create a large existing WAL file that exceeds maxSegmentSize
	existingFile := filepath.Join(tempDir, "wal_1234567890.log")
	largeContent := make([]byte, 2048) // Larger than maxSegmentSize
	err := os.WriteFile(existingFile, largeContent, 0644)
	require.NoError(t, err)

	fs := NewSegmentedFileSystem(tempDir, 1024)

	assert.NotNil(t, fs)
	assert.Equal(t, "wal_1234567890.log", fs.walFiles[0])
	assert.NotNil(t, fs.currentSegment)
	// Current segment should be a new file, not the large existing one
	assert.NotEqual(t, existingFile, fs.currentSegment.FileName)
}

func TestNewSegmentedFileSystem_WithMultipleWALFiles(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create multiple WAL files with different timestamps
	files := []string{
		"wal_1234567890.log",
		"wal_1234567900.log",
		"wal_1234567880.log", // Older timestamp
	}

	for _, file := range files {
		filePath := filepath.Join(tempDir, file)
		err := os.WriteFile(filePath, []byte("content"), 0644)
		require.NoError(t, err)
	}

	fs := NewSegmentedFileSystem(tempDir, 1024)

	assert.NotNil(t, fs)
	assert.Len(t, fs.walFiles, 3)
	// Files should be sorted by timestamp
	assert.Equal(t, "wal_1234567880.log", fs.walFiles[0])
	assert.Equal(t, "wal_1234567890.log", fs.walFiles[1])
	assert.Equal(t, "wal_1234567900.log", fs.walFiles[2])
}

func TestDiscoverAndSortWALFiles(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := &SegmentedFileSystem{dataDir: tempDir}

	// Create mixed files (WAL and non-WAL)
	files := map[string]string{
		"wal_1000.log":     "wal content",
		"wal_2000.log":     "wal content",
		"not_wal.txt":      "not wal",
		"wal_500.log":      "wal content",
		"another_file.dat": "other content",
	}

	for filename, content := range files {
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)
	}

	walFiles, err := fs.discoverAndSortWALFiles()

	require.NoError(t, err)
	assert.Len(t, walFiles, 3)
	assert.Equal(t, "wal_500.log", walFiles[0])
	assert.Equal(t, "wal_1000.log", walFiles[1])
	assert.Equal(t, "wal_2000.log", walFiles[2])
}

func TestCanReuseSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := &SegmentedFileSystem{maxSegmentSize: 100}

	// Test with small file
	smallFile := filepath.Join(tempDir, "small.log")
	err := os.WriteFile(smallFile, []byte("small"), 0644)
	require.NoError(t, err)

	canReuse, err := fs.canReuseSegment(smallFile)
	require.NoError(t, err)
	assert.True(t, canReuse)

	// Test with large file
	largeFile := filepath.Join(tempDir, "large.log")
	largeContent := make([]byte, 150)
	err = os.WriteFile(largeFile, largeContent, 0644)
	require.NoError(t, err)

	canReuse, err = fs.canReuseSegment(largeFile)
	require.NoError(t, err)
	assert.False(t, canReuse)

	// Test with non-existent file
	nonExistentFile := filepath.Join(tempDir, "nonexistent.log")
	canReuse, err = fs.canReuseSegment(nonExistentFile)
	assert.Error(t, err)
	assert.False(t, canReuse)
}

func TestReuseLastSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := &SegmentedFileSystem{}

	// Create a file to reuse
	testFile := filepath.Join(tempDir, "test.log")
	content := []byte("existing content")
	err := os.WriteFile(testFile, content, 0644)
	require.NoError(t, err)

	err = fs.reuseLastSegment(testFile)
	require.NoError(t, err)

	assert.NotNil(t, fs.currentSegment)
	assert.Equal(t, testFile, fs.currentSegment.FileName)
	assert.Equal(t, len(content), fs.currentSegment.currentSize)
}

func TestWriteSync_NewSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 100)

	data := []byte("test data")
	err := fs.WriteSync(data)
	require.NoError(t, err)

	// Verify data was written
	assert.NotNil(t, fs.currentSegment)
	assert.Equal(t, len(data), fs.currentSegment.currentSize)
}

func TestWriteSync_SegmentRotation(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 50) // Small max size

	// Write data that will fit
	smallData := []byte("small")
	err := fs.WriteSync(smallData)
	require.NoError(t, err)

	// Verify data is actually written to disk
	assert.NotNil(t, fs.currentSegment)
	assert.Equal(t, len(smallData), fs.currentSegment.currentSize)

	// Read the file directly from disk to verify persistence
	writtenData, err := os.ReadFile(fs.currentSegment.FileName)
	require.NoError(t, err)
	assert.Equal(t, smallData, writtenData)
	fileInfo, err := os.Stat(fs.currentSegment.FileName)
	require.NoError(t, err)
	assert.Equal(t, int64(len(smallData)), fileInfo.Size())
	assert.False(t, fileInfo.IsDir())

	firstSegment := fs.currentSegment.FileName

	// Write data that will cause rotation
	largeData := make([]byte, 60)
	err = fs.WriteSync(largeData)
	require.NoError(t, err)

	// Should have rotated to a new segment
	assert.NotEqual(t, firstSegment, fs.currentSegment.FileName)
	assert.Equal(t, len(largeData), fs.currentSegment.currentSize)
}

func TestReadAll_WithExistingFiles(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create test files
	files := map[string][]byte{
		"wal_1000.log": []byte("content1"),
		"wal_2000.log": []byte("content2"),
		"wal_1500.log": []byte("content3"),
	}

	for filename, content := range files {
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, content, 0644)
		require.NoError(t, err)
	}

	fs := NewSegmentedFileSystem(tempDir, 1024)

	var results [][]byte
	for data, err := range fs.ReadAll() {
		require.NoError(t, err)
		if len(data) > 0 { // Skip empty files
			results = append(results, data)
		}
	}

	// Should read files in sorted order
	assert.Len(t, results, 3)
	assert.Equal(t, []byte("content1"), results[0]) // wal_1000.log
	assert.Equal(t, []byte("content3"), results[1]) // wal_1500.log
	assert.Equal(t, []byte("content2"), results[2]) // wal_2000.log
}

func TestSortWALFiles_InvalidTimestamps(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := &SegmentedFileSystem{}

	// Create files with invalid timestamps
	files := []string{
		"wal_invalid.log",
		"wal_123.log",
		"wal_abc.log",
	}

	var entries []os.DirEntry
	for _, filename := range files {
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, []byte("content"), 0644)
		require.NoError(t, err)

		_, err = os.Stat(filePath)
		require.NoError(t, err)
		entries = append(entries, &mockDirEntry{name: filename, isDir: false})
	}

	// Should handle invalid timestamps gracefully with string comparison fallback
	sortedFiles := fs.sortWALFiles(entries)
	assert.Len(t, sortedFiles, 3)
	// Should be sorted lexicographically when numeric parsing fails
	assert.Equal(t, "wal_123.log", sortedFiles[0])
	assert.Equal(t, "wal_abc.log", sortedFiles[1])
	assert.Equal(t, "wal_invalid.log", sortedFiles[2])
}

// Helper functions and mocks

func createTempDir(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "segmented_fs_test_*")
	require.NoError(t, err)
	return tempDir
}

type mockDirEntry struct {
	name  string
	isDir bool
}

func (m *mockDirEntry) Name() string               { return m.name }
func (m *mockDirEntry) IsDir() bool                { return m.isDir }
func (m *mockDirEntry) Type() os.FileMode          { return 0 }
func (m *mockDirEntry) Info() (os.FileInfo, error) { return nil, nil }
