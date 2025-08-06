package compute

import (
	"testing"

	"github.com/EzhovAndrew/kv-db/api"
	"github.com/stretchr/testify/assert"
)

func TestNewCompute(t *testing.T) {
	compute := NewCompute()

	assert.NotNil(t, compute)
	assert.IsType(t, &Compute{}, compute)
}

func TestCompute_Parse_BinaryProtocol_ValidCommands(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name         string
		command      string
		key          string
		value        string
		expectedID   int
		expectedArgs []string
	}{
		{
			name:         "GET command",
			command:      "GET",
			key:          "testkey",
			value:        "",
			expectedID:   GetCommandID,
			expectedArgs: []string{"testkey"},
		},
		{
			name:         "SET command",
			command:      "SET",
			key:          "testkey",
			value:        "testvalue",
			expectedID:   SetCommandID,
			expectedArgs: []string{"testkey", "testvalue"},
		},
		{
			name:         "DEL command",
			command:      "DEL",
			key:          "testkey",
			value:        "",
			expectedID:   DelCommandID,
			expectedArgs: []string{"testkey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := api.EncodeBinaryCommand([]byte(tt.command), []byte(tt.key), []byte(tt.value))
			query, err := compute.Parse(data)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedID, query.CommandID())
			assert.Equal(t, tt.expectedArgs, query.Arguments())
		})
	}
}

func TestCompute_Parse_WhitespaceValues(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{
			name:  "value with spaces",
			key:   "key1",
			value: "hello world with spaces",
		},
		{
			name:  "value with newlines",
			key:   "key2",
			value: "line1\nline2\nline3",
		},
		{
			name:  "value with tabs",
			key:   "key3",
			value: "col1\tcol2\tcol3",
		},
		{
			name:  "value with mixed whitespace",
			key:   "key4",
			value: "mixed\t whitespace\n here",
		},
		{
			name:  "value with carriage returns",
			key:   "key5",
			value: "text\rwith\rcarriage\rreturns",
		},
		{
			name:  "key with underscores (should work)",
			key:   "key_with_underscores",
			value: "value with spaces and\ttabs\nand newlines",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := api.EncodeBinarySet([]byte(tt.key), []byte(tt.value))
			query, err := compute.Parse(data)

			assert.NoError(t, err)
			assert.Equal(t, SetCommandID, query.CommandID())
			assert.Equal(t, []string{tt.key, tt.value}, query.Arguments())
		})
	}
}

func TestCompute_Parse_ErrorCases(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name        string
		data        []byte
		expectedErr error
	}{
		{
			name:        "empty data",
			data:        []byte{},
			expectedErr: ErrEmptyCommand,
		},
		{
			name:        "incomplete command length",
			data:        []byte{},
			expectedErr: ErrEmptyCommand,
		},
		{
			name:        "invalid command length",
			data:        []byte{10}, // command length 10 but no data
			expectedErr: ErrInvalidProtocol,
		},
		{
			name:        "unknown command",
			data:        api.EncodeBinaryCommand([]byte("INVALID"), []byte("key"), nil),
			expectedErr: ErrUnknownCommand,
		},
		{
			name:        "SET command without value",
			data:        api.EncodeBinaryCommand([]byte("SET"), []byte("key"), nil),
			expectedErr: ErrInvalidArgumentsNumber,
		},
		{
			name:        "GET command with value",
			data:        api.EncodeBinaryCommand([]byte("GET"), []byte("key"), []byte("value")),
			expectedErr: ErrInvalidArgumentsNumber,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.data)

			assert.Error(t, err)
			assert.Equal(t, tt.expectedErr, err)
			assert.Equal(t, Query{}, query)
		})
	}
}

func TestCompute_Protocol_RoundTrip(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name    string
		command string
		key     string
		value   string
	}{
		{
			name:    "simple GET",
			command: "GET",
			key:     "simple",
			value:   "",
		},
		{
			name:    "simple SET",
			command: "SET",
			key:     "simple",
			value:   "value",
		},
		{
			name:    "complex value with all whitespace types",
			command: "SET",
			key:     "complex",
			value:   "value with\nspaces\tand\ttabs\nand\r\nmultiple\nlines",
		},
		{
			name:    "unicode value",
			command: "SET",
			key:     "unicode",
			value:   "héllo wörld 🌍 with émojis",
		},
		{
			name:    "binary-like value",
			command: "SET",
			key:     "binary",
			value:   string([]byte{0, 1, 2, 255, 254, 253}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := api.EncodeBinaryCommand([]byte(tt.command), []byte(tt.key), []byte(tt.value))

			// Parse
			query, err := compute.Parse(encoded)
			assert.NoError(t, err)

			// Verify command
			expectedID := commandNameToCommandID(tt.command)
			assert.Equal(t, expectedID, query.CommandID())

			// Verify arguments
			if tt.value == "" {
				assert.Equal(t, []string{tt.key}, query.Arguments())
			} else {
				assert.Equal(t, []string{tt.key, tt.value}, query.Arguments())
			}
		})
	}
}

func TestCompute_Parse_ErrorTypes(t *testing.T) {
	// Test that all error types are properly defined
	t.Run("error constants exist", func(t *testing.T) {
		assert.NotNil(t, ErrEmptyCommand)
		assert.NotNil(t, ErrUnknownCommand)
		assert.NotNil(t, ErrInvalidArgumentsNumber)
		assert.NotNil(t, ErrInvalidProtocol)

		assert.Contains(t, ErrEmptyCommand.Error(), "empty command")
		assert.Contains(t, ErrUnknownCommand.Error(), "unknown command")
		assert.Contains(t, ErrInvalidArgumentsNumber.Error(), "invalid arguments number")
		assert.Contains(t, ErrInvalidProtocol.Error(), "invalid protocol")
	})
}
