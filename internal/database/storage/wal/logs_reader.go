package wal

import "iter"

type FileLogsReader struct {
	dataDir string
}

func NewFileLogsReader(dataDir string) *FileLogsReader {
	return &FileLogsReader{dataDir: dataDir}
}

func (l *FileLogsReader) Read() iter.Seq2[Log, error] {
	return func(yield func(Log, error) bool) {
		// Implement the logic to read logs from the WAL
		// and yield them using the yield function
		// Return false from the yield function to stop reading logs
	}
}
