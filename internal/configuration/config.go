package configuration

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

type EngineConfig struct {
	Type string `yaml:"type"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}

type NetworkConfig struct {
	Ip             string `yaml:"ip"`
	Port           string `yaml:"port"`
	MaxConnections int    `yaml:"max_connections"`
	MaxMessageSize int    `yaml:"max_message_size"`
	IdleTimeout    int    `yaml:"idle_timeout"`
}

type Config struct {
	Engine EngineConfig `yaml:"engine"`
}

var (
	ErrConfigFileMissing = errors.New("no config file path provided")
)

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
	return &config, nil
}
