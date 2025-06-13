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
  port: 3223
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

func TestNewConfig_ValidConfigurationNoWAL(t *testing.T) {
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
  port: 3223
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
	assert.Equal(t, "3223", config.Network.Port)
	assert.Equal(t, 100, config.Network.MaxConnections)
	assert.Equal(t, 1024, config.Network.MaxMessageSize)
	assert.Equal(t, 30, config.Network.IdleTimeout)
	assert.Equal(t, 5, config.Network.GracefulShutdownTimeout)
	assert.Nil(t, config.WAL)
}

func TestNewConfig_ValidConfigurationWithWAL(t *testing.T) {
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
  port: 3223
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
wal:
  flush_batch_size: 100
  flush_batch_timeout: 10
  max_segment_size: 10485760
  data_directory: "/data/kv_db/wal"
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
	assert.Equal(t, "3223", config.Network.Port)
	assert.Equal(t, 100, config.Network.MaxConnections)
	assert.Equal(t, 1024, config.Network.MaxMessageSize)
	assert.Equal(t, 30, config.Network.IdleTimeout)
	assert.Equal(t, 5, config.Network.GracefulShutdownTimeout)
	assert.NotNil(t, config.WAL)
	assert.Equal(t, 100, config.WAL.FlushBatchSize)
	assert.Equal(t, 10, config.WAL.FlushBatchTimeout)
	assert.Equal(t, 10485760, config.WAL.MaxSegmentSize)
	assert.Equal(t, "/data/kv_db/wal", config.WAL.DataDirectory)
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
  port: 3223
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
  port: 3223
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
			port:        "3223",
			shouldError: false,
		},
		{
			name:        "invalid IP",
			ip:          "invalid.ip",
			port:        "3223",
			shouldError: true,
		},
		{
			name:        "empty IP",
			ip:          "",
			port:        "3223",
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
  port: 3223
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
  port: 3223
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
  port: 3223
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

func TestNewConfig_WALPathValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name           string
		data_directory string
		shouldError    bool
	}{
		{
			name:           "valid WAL path",
			data_directory: "/path/to/wal",
			shouldError:    false,
		},
		{
			name:           "empty WAL path",
			data_directory: "",
			shouldError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")
			yamlContent := `
engine:
  type: "in_memory"
network:
  ip: "127.0.0.1"
  port: 3223
  max_connections: 100
  max_message_size: 4096
  idle_timeout: 300
  graceful_shutdown_timeout: 5
logging:
  level: "info"
  output: "stdout"
wal:
  data_directory: ` + tt.data_directory + `
  flush_batch_size: 100
  flush_batch_timeout: 10
  max_segment_size: 1048576
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
				assert.Equal(t, tt.data_directory, config.WAL.DataDirectory)
				assert.Equal(t, 100, config.WAL.FlushBatchSize)
				assert.Equal(t, 10, config.WAL.FlushBatchTimeout)
				assert.Equal(t, 1048576, config.WAL.MaxSegmentSize)
			}
		})
	}
}

func TestNewConfig_WALMaxSegmentSizeValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name             string
		max_segment_size int
		shouldError      bool
	}{
		{
			name:             "valid max segment size",
			max_segment_size: 10485760,
			shouldError:      false,
		},
		{
			name:             "too small max segment size",
			max_segment_size: 4095,
			shouldError:      true,
		},
		{
			name:             "too big max segment size",
			max_segment_size: 104857601,
			shouldError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")
			yamlContent := `
engine:
  type: "in_memory"
network:
  ip: "127.0.0.1"
  port: 3223
  max_connections: 100
  max_message_size: 4096
  idle_timeout: 300
  graceful_shutdown_timeout: 5
logging:
  level: "info"
  output: "stdout"
wal:
  data_directory: /data/kv_db/wal
  flush_batch_size: 100
  flush_batch_timeout: 10
  max_segment_size: ` + strconv.Itoa(tt.max_segment_size) + `
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
				assert.Equal(t, tt.max_segment_size, config.WAL.MaxSegmentSize)
				assert.Equal(t, 100, config.WAL.FlushBatchSize)
				assert.Equal(t, 10, config.WAL.FlushBatchTimeout)
				assert.Equal(t, "/data/kv_db/wal", config.WAL.DataDirectory)
			}
		})
	}
}

func TestNewConfig_WALFlushBatchSizeValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name             string
		flush_batch_size int
		shouldError      bool
	}{
		{
			name:             "valid flush batch size",
			flush_batch_size: 100,
			shouldError:      false,
		},
		{
			name:             "too small flush batch size",
			flush_batch_size: 4,
			shouldError:      true,
		},
		{
			name:             "too big flush batch size",
			flush_batch_size: 1001,
			shouldError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")
			yamlContent := `
engine:
  type: "in_memory"
network:
  ip: "127.0.0.1"
  port: 3223
  max_connections: 100
  max_message_size: 4096
  idle_timeout: 300
  graceful_shutdown_timeout: 5
logging:
  level: "info"
  output: "stdout"
wal:
  data_directory: /data/kv_db/wal
  flush_batch_size: ` + strconv.Itoa(tt.flush_batch_size) + `
  flush_batch_timeout: 10
  max_segment_size: 10485760
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
				assert.Equal(t, tt.flush_batch_size, config.WAL.FlushBatchSize)
				assert.Equal(t, 10485760, config.WAL.MaxSegmentSize)
				assert.Equal(t, 10, config.WAL.FlushBatchTimeout)
				assert.Equal(t, "/data/kv_db/wal", config.WAL.DataDirectory)
			}
		})
	}
}

func TestNewConfig_WALFlushBatchTimeoutValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name                string
		flush_batch_timeout int
		shouldError         bool
	}{
		{
			name:                "valid flush batch timeout",
			flush_batch_timeout: 10,
			shouldError:         false,
		},
		{
			name:                "too small flush batch timeout",
			flush_batch_timeout: 4,
			shouldError:         true,
		},
		{
			name:                "too big flush batch timeout",
			flush_batch_timeout: 1001,
			shouldError:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.yaml")
			yamlContent := `
engine:
  type: "in_memory"
network:
  ip: "127.0.0.1"
  port: 3223
  max_connections: 100
  max_message_size: 4096
  idle_timeout: 300
  graceful_shutdown_timeout: 5
logging:
  level: "info"
  output: "stdout"
wal:
  data_directory: /data/kv_db/wal
  flush_batch_size: 100
  flush_batch_timeout: ` + strconv.Itoa(tt.flush_batch_timeout) + `
  max_segment_size: 10485760
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
				assert.Equal(t, tt.flush_batch_timeout, config.WAL.FlushBatchTimeout)
				assert.Equal(t, 100, config.WAL.FlushBatchSize)
				assert.Equal(t, 10485760, config.WAL.MaxSegmentSize)
				assert.Equal(t, "/data/kv_db/wal", config.WAL.DataDirectory)
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
  port: 3223
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
  port: 3223
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

func TestNewConfig_ReplicationValidation(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tests := []struct {
		name        string
		role        string
		masterIP    string
		masterPort  string
		includeWAL  bool
		shouldError bool
	}{
		{
			name:        "valid master role",
			role:        "master",
			masterIP:    "",
			masterPort:  "",
			includeWAL:  true,
			shouldError: false,
		},
		{
			name:        "valid slave role with master ip and port",
			role:        "slave",
			masterIP:    "127.0.0.1",
			masterPort:  "3223",
			includeWAL:  true,
			shouldError: false,
		},
		{
			name:        "invalid role",
			role:        "invalid_role",
			masterIP:    "",
			masterPort:  "",
			includeWAL:  true,
			shouldError: true,
		},
		{
			name:        "slave without master ip",
			role:        "slave",
			masterIP:    "",
			masterPort:  "3223",
			includeWAL:  true,
			shouldError: true,
		},
		{
			name:        "slave without master port",
			role:        "slave",
			masterIP:    "127.0.0.1",
			masterPort:  "",
			includeWAL:  true,
			shouldError: true,
		},
		{
			name:        "slave without master ip and port",
			role:        "slave",
			masterIP:    "",
			masterPort:  "",
			includeWAL:  true,
			shouldError: true,
		},
		{
			name:        "empty role",
			role:        "",
			masterIP:    "",
			masterPort:  "",
			includeWAL:  true,
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
  ip: 127.0.0.1
  port: 3223
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5`

			if tt.includeWAL {
				yamlContent += `
wal:
  flush_batch_size: 100
  flush_batch_timeout: 10
  max_segment_size: 10485760
  data_directory: "/data/kv_db/wal"`
			}

			yamlContent += `
replication:
  role: ` + tt.role

			if tt.masterIP != "" {
				yamlContent += `
  master_address: ` + tt.masterIP
			}

			if tt.masterPort != "" {
				yamlContent += `
  master_port: ` + tt.masterPort
			}

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
				assert.NotNil(t, config.Replication)
				assert.Equal(t, tt.role, config.Replication.Role)
				if tt.masterIP != "" {
					assert.Equal(t, tt.masterIP, config.Replication.MasterAddress)
				}
				if tt.masterPort != "" {
					assert.Equal(t, tt.masterPort, config.Replication.MasterPort)
				}
			}
		})
	}
}

func TestNewConfig_ReplicationRequiresWAL(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

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
  port: 3223
  max_connections: 100
  max_message_size: 1024
  idle_timeout: 30
  graceful_shutdown_timeout: 5
replication:
  role: master
`

	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	os.Setenv("CONFIG_FILEPATH", configFile)

	config, err := NewConfig()

	assert.Error(t, err)
	assert.Equal(t, ErrWALMustBeEnabled, err)
	assert.Nil(t, config)
}

func TestNewConfig_CompleteConfigurationWithReplication(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "complete.yaml")

	validYAML := `
engine:
  type: in_memory
logging:
  level: debug
  output: /var/log/kv-db.log
network:
  ip: 0.0.0.0
  port: 9090
  max_connections: 500
  max_message_size: 2048
  idle_timeout: 60
  graceful_shutdown_timeout: 10
wal:
  flush_batch_size: 200
  flush_batch_timeout: 20
  max_segment_size: 20971520
  data_directory: "/tmp/kv_db/wal"
replication:
  role: slave
  master_address: "192.168.1.100"
  master_port: "9090"
`

	err := os.WriteFile(configFile, []byte(validYAML), 0644)
	require.NoError(t, err)

	os.Setenv("CONFIG_FILEPATH", configFile)

	config, err := NewConfig()

	assert.NoError(t, err)
	assert.NotNil(t, config)

	// Engine validation
	assert.Equal(t, "in_memory", config.Engine.Type)

	// Logging validation
	assert.Equal(t, "debug", config.Logging.Level)
	assert.Equal(t, "/var/log/kv-db.log", config.Logging.Output)

	// Network validation
	assert.Equal(t, "0.0.0.0", config.Network.Ip)
	assert.Equal(t, "9090", config.Network.Port)
	assert.Equal(t, 500, config.Network.MaxConnections)
	assert.Equal(t, 2048, config.Network.MaxMessageSize)
	assert.Equal(t, 60, config.Network.IdleTimeout)
	assert.Equal(t, 10, config.Network.GracefulShutdownTimeout)

	// WAL validation
	assert.NotNil(t, config.WAL)
	assert.Equal(t, 200, config.WAL.FlushBatchSize)
	assert.Equal(t, 20, config.WAL.FlushBatchTimeout)
	assert.Equal(t, 20971520, config.WAL.MaxSegmentSize)
	assert.Equal(t, "/tmp/kv_db/wal", config.WAL.DataDirectory)

	// Replication validation
	assert.NotNil(t, config.Replication)
	assert.Equal(t, "slave", config.Replication.Role)
	assert.Equal(t, "192.168.1.100", config.Replication.MasterAddress)
	assert.Equal(t, "9090", config.Replication.MasterPort)
}

func TestNewConfig_MinimalValidConfiguration(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "minimal.yaml")

	minimalYAML := `
engine:
  type: in_memory
logging:
  level: error
network:
  ip: 127.0.0.1
  port: 1024
  max_connections: 1
  max_message_size: 1
  idle_timeout: 1
  graceful_shutdown_timeout: 0
`

	err := os.WriteFile(configFile, []byte(minimalYAML), 0644)
	require.NoError(t, err)

	os.Setenv("CONFIG_FILEPATH", configFile)

	config, err := NewConfig()

	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "in_memory", config.Engine.Type)
	assert.Equal(t, "error", config.Logging.Level)
	assert.Equal(t, "127.0.0.1", config.Network.Ip)
	assert.Equal(t, "1024", config.Network.Port)
	assert.Equal(t, 1, config.Network.MaxConnections)
	assert.Equal(t, 1, config.Network.MaxMessageSize)
	assert.Equal(t, 1, config.Network.IdleTimeout)
	assert.Equal(t, 0, config.Network.GracefulShutdownTimeout)
	assert.Nil(t, config.WAL)
	assert.Nil(t, config.Replication)
}

func TestNewConfig_MaximumValidConfiguration(t *testing.T) {
	originalPath := os.Getenv("CONFIG_FILEPATH")
	defer os.Setenv("CONFIG_FILEPATH", originalPath)

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "maximum.yaml")

	maximalYAML := `
engine:
  type: in_memory
logging:
  level: fatal
  output: stderr
network:
  ip: 255.255.255.255
  port: 65535
  max_connections: 10000
  max_message_size: 1048576
  idle_timeout: 3600
  graceful_shutdown_timeout: 300
wal:
  flush_batch_size: 1000
  flush_batch_timeout: 1000
  max_segment_size: 104857600
  data_directory: "/maximum/path/to/wal"
replication:
  role: master
`

	err := os.WriteFile(configFile, []byte(maximalYAML), 0644)
	require.NoError(t, err)

	os.Setenv("CONFIG_FILEPATH", configFile)

	config, err := NewConfig()

	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "in_memory", config.Engine.Type)
	assert.Equal(t, "fatal", config.Logging.Level)
	assert.Equal(t, "stderr", config.Logging.Output)
	assert.Equal(t, "255.255.255.255", config.Network.Ip)
	assert.Equal(t, "65535", config.Network.Port)
	assert.Equal(t, 10000, config.Network.MaxConnections)
	assert.Equal(t, 1048576, config.Network.MaxMessageSize)
	assert.NotNil(t, config.WAL)
	assert.Equal(t, 1000, config.WAL.FlushBatchSize)
	assert.Equal(t, 1000, config.WAL.FlushBatchTimeout)
	assert.Equal(t, 104857600, config.WAL.MaxSegmentSize)
	assert.Equal(t, "/maximum/path/to/wal", config.WAL.DataDirectory)
	assert.NotNil(t, config.Replication)
	assert.Equal(t, "master", config.Replication.Role)
}
