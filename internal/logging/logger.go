package logging

import (
	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.Logger

func Init(cfg *configuration.LoggingConfig) {
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		level = zapcore.InfoLevel
	}

	cfgEncoder := zap.NewProductionEncoderConfig()
	cfgEncoder.TimeKey = "timestamp"
	cfgEncoder.EncodeTime = zapcore.ISO8601TimeEncoder
	cfgEncoder.EncodeLevel = zapcore.CapitalColorLevelEncoder

	if cfg.Output == "" {
		cfg.Output = "stdout"
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      false,
		Encoding:         "console",
		EncoderConfig:    cfgEncoder,
		OutputPaths:      []string{cfg.Output},
		ErrorOutputPaths: []string{cfg.Output},
		DisableCaller:    true,
	}

	globalLogger, err = config.Build()
	if err != nil {
		panic(err)
	}
}

func Info(msg string, fields ...zapcore.Field) {
	globalLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...zapcore.Field) {
	globalLogger.Warn(msg, fields...)
}

func Error(msg string, fields ...zapcore.Field) {
	globalLogger.Error(msg, fields...)
}

func Fatal(msg string, fields ...zapcore.Field) {
	globalLogger.Fatal(msg, fields...)
}
