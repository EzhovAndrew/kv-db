package configuration

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig_MissingConfigFilePath(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	os.Unsetenv("CONFIG_FILEPATH")

	config, err := NewConfig()

	assert.Error(t, err)
	assert.Equal(t, ErrConfigFileMissing, err)
	assert.Nil(t, config)
}

func TestNewConfig_NonExistentFile(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	os.Setenv("CONFIG_FILEPATH", "/non/existent/file.yaml")

	config, err := NewConfig()

	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "no such file or directory")
}

func TestNewConfig_InvalidYAML(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	// Create temporary file with invalid YAML
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid.yaml")

	invalidYAML := `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
invalid_yaml_syntax: [unclosed bracket
`

	err := os.WriteFile(configFile, []byte(invalidYAML), 0644)
	require.NoError(t, err)

	os.Setenv("CONFIG_FILEPATH", configFile)

	config, err := NewConfig()

	assert.Error(t, err)
	assert.Nil(t, config)
}

func TestNewConfig_ValidConfiguration(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "valid.yaml")

	validYAML := `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`

	err := os.WriteFile(configFile, []byte(validYAML), 0644)
	require.NoError(t, err)

	os.Setenv("CONFIG_FILEPATH", configFile)

	config, err := NewConfig()

	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "in_memory", config.Engine.Type)
	assert.Equal(t, "info", config.Logging.Level)
	assert.Equal(t, "stdout", config.Logging.Output)
	assert.Equal(t, "127.0.0.1", config.Network.Ip)
	assert.Equal(t, "8080", config.Network.Port)
	assert.Equal(t, 100, config.Network.MaxConnections)
	assert.Equal(t, 1024, config.Network.MaxMessageSize)
	assert.Equal(t, 30, config.Network.IdleTimeout)
	assert.Equal(t, 5, config.Network.GracefulShutdownTimeout)
}

func TestNewConfig_EngineValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name        string
		engineType  string
		shouldError bool
	}{
		{
			name:        "valid engine type",
			engineType:  "in_memory",
			shouldError: false,
		},
		{
			name:        "invalid engine type",
			engineType:  "invalid_engine",
			shouldError: true,
		},
		{
			name:        "empty engine type",
			engineType:  "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			yamlContent := `
engine:
  type: ` + tt.engineType + `
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`

			err := os.WriteFile(configFile, []byte(yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, tt.engineType, config.Engine.Type)
			}
		})
	}
}

func TestNewConfig_LoggingValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name        string
		level       string
		shouldError bool
	}{
		{
			name:        "valid debug level",
			level:       "debug",
			shouldError: false,
		},
		{
			name:        "valid info level",
			level:       "info",
			shouldError: false,
		},
		{
			name:        "valid warn level",
			level:       "warn",
			shouldError: false,
		},
		{
			name:        "valid error level",
			level:       "error",
			shouldError: false,
		},
		{
			name:        "valid fatal level",
			level:       "fatal",
			shouldError: false,
		},
		{
			name:        "invalid level",
			level:       "invalid",
			shouldError: true,
		},
		{
			name:        "empty level",
			level:       "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			yamlContent := `
engine:
  type: in_memory
logging:
  level: ` + tt.level + `
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`

			err := os.WriteFile(configFile, []byte(yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, tt.level, config.Logging.Level)
			}
		})
	}
}

func TestNewConfig_NetworkValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name        string
		ip          string
		port        string
		shouldError bool
	}{
		{
			name:        "valid IPv4",
			ip:          "127.0.0.1",
			port:        "8080",
			shouldError: false,
		},
		{
			name:        "invalid IP",
			ip:          "invalid.ip",
			port:        "8080",
			shouldError: true,
		},
		{
			name:        "empty IP",
			ip:          "",
			port:        "8080",
			shouldError: true,
		},
		{
			name:        "port too low",
			ip:          "127.0.0.1",
			port:        "1023",
			shouldError: true,
		},
		{
			name:        "port too high",
			ip:          "127.0.0.1",
			port:        "65536",
			shouldError: true,
		},
		{
			name:        "invalid port",
			ip:          "127.0.0.1",
			port:        "invalid",
			shouldError: true,
		},
		{
			name:        "empty port",
			ip:          "127.0.0.1",
			port:        "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			yamlContent := `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
network:
  ip: ` + tt.ip + `
  port: ` + tt.port + `
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`

			err := os.WriteFile(configFile, []byte(yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, tt.ip, config.Network.Ip)
				assert.Equal(t, tt.port, config.Network.Port)
			}
		})
	}
}

func TestNewConfig_NetworkConnectionsValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name           string
		maxConnections int
		shouldError    bool
	}{
		{
			name:           "valid connections",
			maxConnections: 100,
			shouldError:    false,
		},
		{
			name:           "minimum connections",
			maxConnections: 1,
			shouldError:    false,
		},
		{
			name:           "maximum connections",
			maxConnections: 10000,
			shouldError:    false,
		},
		{
			name:           "zero connections",
			maxConnections: 0,
			shouldError:    true,
		},
		{
			name:           "negative connections",
			maxConnections: -1,
			shouldError:    true,
		},
		{
			name:           "too many connections",
			maxConnections: 10001,
			shouldError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			yamlContent := `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: ` + strconv.Itoa(tt.maxConnections) + `
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`

			err := os.WriteFile(configFile, []byte(yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
			}
		})
	}
}

func TestNewConfig_MessageSizeValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name           string
		maxMessageSize int
		shouldError    bool
	}{
		{
			name:           "valid message size",
			maxMessageSize: 1024,
			shouldError:    false,
		},
		{
			name:           "minimum message size",
			maxMessageSize: 1,
			shouldError:    false,
		},
		{
			name:           "large message size",
			maxMessageSize: 1048576, // 1MB
			shouldError:    false,
		},
		{
			name:           "zero message size",
			maxMessageSize: 0,
			shouldError:    true,
		},
		{
			name:           "negative message size",
			maxMessageSize: -1,
			shouldError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			yamlContent := `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: ` + strconv.Itoa(tt.maxMessageSize) + `
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`

			err := os.WriteFile(configFile, []byte(yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, tt.maxMessageSize, config.Network.MaxMessageSize)
			}
		})
	}
}

func TestNewConfig_TimeoutValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name                    string
		idleTimeout             int
		gracefulShutdownTimeout int
		shouldError             bool
	}{
		{
			name:                    "valid timeouts",
			idleTimeout:             30,
			gracefulShutdownTimeout: 5,
			shouldError:             false,
		},
		{
			name:                    "minimum idle timeout",
			idleTimeout:             1,
			gracefulShutdownTimeout: 0,
			shouldError:             false,
		},
		{
			name:                    "zero idle timeout",
			idleTimeout:             0,
			gracefulShutdownTimeout: 5,
			shouldError:             true,
		},
		{
			name:                    "negative idle timeout",
			idleTimeout:             -1,
			gracefulShutdownTimeout: 5,
			shouldError:             true,
		},
		{
			name:                    "negative graceful shutdown timeout",
			idleTimeout:             30,
			gracefulShutdownTimeout: -1,
			shouldError:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			yamlContent := `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: ` + strconv.Itoa(tt.idleTimeout) + `
  graceful_shutdown_timeout: ` + strconv.Itoa(tt.gracefulShutdownTimeout) + `
`

			err := os.WriteFile(configFile, []byte(yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
				assert.Equal(t, tt.idleTimeout, config.Network.IdleTimeout)
				assert.Equal(t, tt.gracefulShutdownTimeout, config.Network.GracefulShutdownTimeout)
			}
		})
	}
}

func TestNewConfig_MissingSections(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name        string
		yamlContent string
		shouldError bool
	}{
		{
			name: "missing engine section",
			yamlContent: `
logging:
  level: info
  output: stdout
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`,
			shouldError: true,
		},
		{
			name: "missing logging section",
			yamlContent: `
engine:
  type: in_memory
network:
  ip: 127.0.0.1
  port: 8080
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
`,
			shouldError: true,
		},
		{
			name: "missing network section",
			yamlContent: `
engine:
  type: in_memory
logging:
  level: info
  output: stdout
`,
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")

			err := os.WriteFile(configFile, []byte(tt.yamlContent), 0644)
			require.NoError(t, err)

			os.Setenv("CONFIG_FILEPATH", configFile)

			config, err := NewConfig()

			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
			}
		})
	}
}
