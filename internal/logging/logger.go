package logging

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.Logger

func Init() {
	stdout := zapcore.AddSync(os.Stdout)

	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "timestamp"
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	encoder := zapcore.NewConsoleEncoder(cfg)

	core := zapcore.NewCore(encoder, stdout, zapcore.InfoLevel)
	globalLogger = zap.New(core)
}

func Info(msg string, fields ...zapcore.Field) {
	globalLogger.Info(msg, fields...)
}

func Fatal(msg string, fields ...zapcore.Field) {
	globalLogger.Fatal(msg, fields...)
}
