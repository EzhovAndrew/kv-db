package filesystem

import (
	"strconv"
	"time"
)

func generateFileName() string {
	return "wal_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + ".log"
}
