package configuration

import (
	"errors"
	"os"
	"strconv"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

var EngineInMemoryKey = "in_memory"

type EngineConfig struct {
	Type string `yaml:"type" validate:"required,oneof=in_memory"`
}

type WALConfig struct {
	FlushBatchSize    int    `yaml:"flush_batch_size" validate:"required,min=5,max=1000"`
	FlushBatchTimeout int    `yaml:"flush_batch_timeout" validate:"required,min=5,max=1000"`
	MaxSegmentSize    int    `yaml:"max_segment_size" validate:"required,min=4096,max=104857600"`
	DataDirectory     string `yaml:"data_directory" validate:"required"`
}

type LoggingConfig struct {
	Level  string `yaml:"level" validate:"required,oneof=debug info warn error fatal"`
	Output string `yaml:"output"`
}

type NetworkConfig struct {
	Ip                      string `yaml:"ip" validate:"required,ip"`
	Port                    string `yaml:"port" validate:"required,port_range"`
	MaxConnections          int    `yaml:"max_connections" validate:"required,min=1,max=10000"`
	MaxMessageSize          int    `yaml:"max_message_size" validate:"required,min=1"`
	IdleTimeout             int    `yaml:"idle_timeout" validate:"min=1"`
	GracefulShutdownTimeout int    `yaml:"graceful_shutdown_timeout" validate:"min=0"`
}

type Config struct {
	Engine  EngineConfig  `yaml:"engine"`
	Logging LoggingConfig `yaml:"logging"`
	Network NetworkConfig `yaml:"network"`
	WAL     *WALConfig    `yaml:"wal"`
}

var (
	ErrConfigFileMissing = errors.New("no config file path provided, set CONFIG_FILEPATH env variable")
	validate             = validator.New()
)

func init() {
	err := validate.RegisterValidation("port_range", validatePortRange)
	if err != nil {
		panic(err)
	}
}

func validatePortRange(fl validator.FieldLevel) bool {
	portStr := fl.Field().String()
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}
	return port >= 1024 && port <= 65535
}

func NewConfig() (*Config, error) {
	configFilePath := os.Getenv("CONFIG_FILEPATH")
	if configFilePath == "" {
		return nil, ErrConfigFileMissing
	}

	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	var config Config
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	if err = validate.Struct(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
