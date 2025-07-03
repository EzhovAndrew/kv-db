package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Initialize logging once before all tests
	logging.Init(&configuration.LoggingConfig{})

	// Run all tests
	code := m.Run()

	// Exit with the test result code
	os.Exit(code)
}

func TestNewSegmentedFileSystem_EmptyDirectory(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 1024)
	defer fs.Close()

	assert.NotNil(t, fs)
	assert.Equal(t, tempDir, fs.dataDir)
	assert.Equal(t, 1024, fs.maxSegmentSize)

	// Should have a current segment
	currentSegment := fs.currentSegment.Load()
	assert.NotNil(t, currentSegment)
	assert.Equal(t, int64(0), currentSegment.getCurrentSize())

	// Should have metadata file
	metadataPath := filepath.Join(tempDir, "metadata.txt")
	assert.FileExists(t, metadataPath)
}

func TestNewSegmentedFileSystem_WithExistingMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create existing segment file using generateFileName
	segmentFilename := generateFileName()
	segmentPath := filepath.Join(tempDir, segmentFilename)
	err := os.WriteFile(segmentPath, []byte("existing content"), 0644)
	require.NoError(t, err)

	// Write metadata file
	metadataFile := filepath.Join(tempDir, "metadata.txt")
	err = os.WriteFile(metadataFile, []byte(segmentFilename+",100\n"), 0644)
	require.NoError(t, err)

	fs := NewSegmentedFileSystem(tempDir, 1024)
	defer fs.Close()

	assert.NotNil(t, fs)

	// Should reuse the existing segment since it's small enough
	currentSegment := fs.currentSegment.Load()
	assert.NotNil(t, currentSegment)
	assert.Equal(t, segmentPath, currentSegment.FileName)
	assert.Equal(t, int64(16), currentSegment.getCurrentSize()) // "existing content" length
}

func TestNewSegmentedFileSystem_WithLargeExistingSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create large existing segment using generateFileName
	segmentFilename := generateFileName()
	segmentPath := filepath.Join(tempDir, segmentFilename)
	largeContent := make([]byte, 200) // Will exceed maxSegmentSize of 100
	err := os.WriteFile(segmentPath, largeContent, 0644)
	require.NoError(t, err)

	// Write metadata file
	metadataFile := filepath.Join(tempDir, "metadata.txt")
	err = os.WriteFile(metadataFile, []byte(segmentFilename+",100\n"), 0644)
	require.NoError(t, err)

	time.Sleep(1 * time.Millisecond)
	fs := NewSegmentedFileSystem(tempDir, 100) // Small max size
	defer fs.Close()

	// Should create new segment, not reuse the large one
	currentSegment := fs.currentSegment.Load()
	assert.NotNil(t, currentSegment)
	assert.NotEqual(t, segmentPath, currentSegment.FileName)
	assert.Equal(t, int64(0), currentSegment.getCurrentSize())
	// Verify metadata was written correctly for the new segment
	metadata, err := fs.mm.GetLastWALFileMetadata()
	require.NoError(t, err)
	assert.NotEqual(t, segmentFilename, metadata.GetSegmentFilename())
	assert.Equal(t, uint64(10100), metadata.GetLSNStart())
}

func TestWriteSync_Basic(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 1024)
	defer fs.Close()

	data := []byte("test data")
	err := fs.WriteSync(data, 1)
	require.NoError(t, err)

	// Verify data was written
	currentSegment := fs.currentSegment.Load()
	assert.NotNil(t, currentSegment)
	assert.Equal(t, int64(len(data)), currentSegment.getCurrentSize())

	// Verify data persistence
	writtenData, err := os.ReadFile(currentSegment.FileName)
	require.NoError(t, err)
	assert.Equal(t, data, writtenData)
}

func TestWriteSync_SegmentRotation(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 50) // Small max size
	defer fs.Close()

	// Write small data that fits
	smallData := []byte("small")
	err := fs.WriteSync(smallData, 1)
	require.NoError(t, err)

	firstSegment := fs.currentSegment.Load()
	assert.NotNil(t, firstSegment)
	assert.Equal(t, int64(len(smallData)), firstSegment.getCurrentSize())
	firstSegmentName := firstSegment.FileName

	// Write large data that triggers rotation
	largeData := make([]byte, 60) // Exceeds maxSegmentSize
	err = fs.WriteSync(largeData, 2)
	require.NoError(t, err)

	// Should have rotated to new segment
	newSegment := fs.currentSegment.Load()
	assert.NotNil(t, newSegment)
	assert.NotEqual(t, firstSegmentName, newSegment.FileName)
	assert.Equal(t, int64(len(largeData)), newSegment.getCurrentSize())

	// Verify metadata was updated
	metadata, err := fs.mm.GetWALFilesMetadata()
	require.NoError(t, err)
	assert.Len(t, metadata, 2) // Should have 2 segments now
}

func TestReadAll_MultipleSegments(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 50)
	defer fs.Close()

	// Write data to multiple segments
	testData := [][]byte{
		[]byte("first segment data"),
		[]byte("second segment data that will cause rotation"),
		[]byte("third segment"),
	}

	lsn := uint64(1)
	for _, data := range testData {
		err := fs.WriteSync(data, lsn)
		require.NoError(t, err)
		lsn++
	}

	// Read all data back
	var readData [][]byte
	for data, err := range fs.ReadAll() {
		require.NoError(t, err)
		if len(data) > 0 {
			readData = append(readData, data)
		}
	}

	// Should read all segments in order
	assert.Len(t, readData, len(testData))
	for i, expected := range testData {
		assert.Equal(t, expected, readData[i])
	}
}

func TestGetSegmentForLSN(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 50)
	defer fs.Close()

	// Create segments with known LSNs
	err := fs.WriteSync([]byte("data1"), 100)
	require.NoError(t, err)

	firstSegmentName := fs.currentSegment.Load().FileName

	err = fs.WriteSync([]byte("data2 that will cause rotation because it's long"), 200)
	require.NoError(t, err)

	secondSegmentName := fs.currentSegment.Load().FileName

	// Test LSN lookups
	segment, err := fs.GetSegmentForLSN(100)
	require.NoError(t, err)
	assert.Equal(t, filepath.Base(firstSegmentName), segment)

	segment, err = fs.GetSegmentForLSN(200)
	require.NoError(t, err)
	assert.Equal(t, filepath.Base(secondSegmentName), segment)

	// Test LSN that doesn't exist yet - should return current segment
	segment, err = fs.GetSegmentForLSN(999)
	require.NoError(t, err)
	assert.Equal(t, filepath.Base(secondSegmentName), segment)
}

func TestReadContinuouslyFromSegment_CurrentSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 1024)
	defer fs.Close()

	// Write some initial data
	initialData := []byte("initial data")
	err := fs.WriteSync(initialData, 1)
	require.NoError(t, err)

	currentSegment := fs.currentSegment.Load()
	segmentName := filepath.Base(currentSegment.FileName)

	// Start goroutine to write additional data 5 times
	done := make(chan bool)
	go func() {
		defer close(done)
		for i := range 5 {
			additionalData := []byte(fmt.Sprintf("additional data %d", i+1))
			err := fs.WriteSync(additionalData, uint64(i+2))
			if err != nil {
				t.Logf("Error writing additional data: %v", err)
				return
			}
			time.Sleep(100 * time.Millisecond) // Small delay between writes
		}
	}()

	// Read from current segment
	var readData [][]byte
	count := 0
	for data, err := range fs.ReadContinuouslyFromSegment(segmentName) {
		if err != nil {
			t.Logf("Error reading: %v", err)
			break
		}
		if len(data) > 0 {
			readData = append(readData, data)
		}
		count++
		if count > 5 { // Prevent infinite loop in test
			break
		}
	}

	// Should read the initial data and all additional data
	assert.GreaterOrEqual(t, len(readData), 1)
	assert.Equal(t, initialData, readData[0])

	// Check that we read all the additional data that was written
	expectedCount := 6 // initial data + 5 additional writes
	assert.Equal(t, expectedCount, len(readData))

	// Verify the additional data was read correctly
	for i := 1; i < len(readData); i++ {
		expectedData := []byte(fmt.Sprintf("additional data %d", i))
		assert.Equal(t, expectedData, readData[i], "Data at index %d doesn't match", i)
	}
}

func TestSegmentOverflowCheck(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 100)
	defer fs.Close()

	// Write data close to limit
	data1 := make([]byte, 80)
	err := fs.WriteSync(data1, 1)
	require.NoError(t, err)

	firstSegment := fs.currentSegment.Load()
	firstSegmentName := firstSegment.FileName

	// Write data that will exceed limit
	data2 := make([]byte, 30) // 80 + 30 > 100
	err = fs.WriteSync(data2, 2)
	require.NoError(t, err)

	// Should have rotated
	newSegment := fs.currentSegment.Load()
	assert.NotEqual(t, firstSegmentName, newSegment.FileName)
	assert.Equal(t, int64(len(data2)), newSegment.getCurrentSize())
}

func TestClose_CleanupResources(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 1024)

	// Write some data to ensure segment is open
	err := fs.WriteSync([]byte("test data"), 1)
	require.NoError(t, err)

	// Verify segment is open
	currentSegment := fs.currentSegment.Load()
	assert.NotNil(t, currentSegment)

	// Close should not error
	err = fs.Close()
	assert.NoError(t, err)

	// Should be safe to call close multiple times
	err = fs.Close()
	assert.NoError(t, err)
}

func TestMetadataIntegration(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 50)
	defer fs.Close()

	// Create multiple segments
	for i := 1; i <= 3; i++ {
		data := make([]byte, 60) // Force rotation each time
		err := fs.WriteSync(data, uint64(i+i*100))
		require.NoError(t, err)
	}

	// Verify metadata was properly recorded
	metadata, err := fs.mm.GetWALFilesMetadata()
	require.NoError(t, err)
	assert.Len(t, metadata, 4)

	// Verify LSN ordering
	for i, meta := range metadata {
		expectedLSN := uint64(i + i*100)
		assert.Equal(t, expectedLSN, meta.GetLSNStart())
	}

	// Test last metadata
	lastMeta, err := fs.mm.GetLastWALFileMetadata()
	require.NoError(t, err)
	assert.Equal(t, uint64(303), lastMeta.GetLSNStart())
}

func TestConcurrentWrites(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	fs := NewSegmentedFileSystem(tempDir, 1024)
	defer fs.Close()

	// Test that atomic operations work correctly
	// (Note: This doesn't test true concurrency since we have single writer assumption,
	// but tests that atomic operations don't panic)

	for i := 0; i < 10; i++ {
		data := []byte("concurrent test data")
		err := fs.WriteSync(data, uint64(i+1))
		require.NoError(t, err)

		// Verify atomic reads work
		currentSegment := fs.currentSegment.Load()
		assert.NotNil(t, currentSegment)
		size := currentSegment.getCurrentSize()
		assert.Greater(t, size, int64(0))
	}
}

// Helper functions

func createTempDir(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "segmented_fs_test_*")
	require.NoError(t, err)
	return tempDir
}
