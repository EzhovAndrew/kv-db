package filesystem

import (
	"strconv"
	"time"
)

func generateFileName(dataDir string) string {
	return dataDir + "/" + "wal_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + ".log"
}

func checkFileName(fileName string) bool {
	return len(fileName) > 8 && fileName[:4] == "wal_" && fileName[len(fileName)-4:] == ".log"
}
