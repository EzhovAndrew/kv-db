package compute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCompute(t *testing.T) {
	compute := NewCompute()

	assert.NotNil(t, compute)
	assert.IsType(t, &Compute{}, compute)
}

func TestCompute_Parse_EmptyCommand(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name string
		cmd  string
	}{
		{
			name: "empty string",
			cmd:  "",
		},
		{
			name: "whitespace only",
			cmd:  "   ",
		},
		{
			name: "tabs and spaces",
			cmd:  "\t  \n  ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.cmd)

			assert.Error(t, err)
			assert.Equal(t, ErrEmptyCommand, err)
			assert.Equal(t, Query{}, query)
		})
	}
}

func TestCompute_Parse_UnknownCommand(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name string
		cmd  string
	}{
		{
			name: "unknown command",
			cmd:  "UNKNOWN",
		},
		{
			name: "unknown command with args",
			cmd:  "INVALID arg1 arg2",
		},
		{
			name: "random string",
			cmd:  "randomcommand",
		},
		{
			name: "numeric command",
			cmd:  "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.cmd)

			assert.Error(t, err)
			assert.Equal(t, ErrUnknownCommand, err)
			assert.Equal(t, Query{}, query)
		})
	}
}

func TestCompute_Parse_InvalidArgumentsNumber(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name        string
		cmd         string
		description string
	}{
		{
			name:        "too few SET arguments",
			cmd:         "SET key",
			description: "SET command with insufficient arguments",
		},
		{
			name:        "too few GET arguments",
			cmd:         "GET",
			description: "GET command with insufficient arguments",
		},
		{
			name:        "too few DEL arguments",
			cmd:         "DEL",
			description: "DEL command with insufficient arguments",
		},
		{
			name:        "too many GET arguments",
			cmd:         "GET key extra_arg",
			description: "GET command with excessive arguments",
		},
		{
			name:        "too many SET arguments",
			cmd:         "SET key value extra_arg",
			description: "SET command with excessive arguments",
		},
		{
			name:        "too many DEL arguments",
			cmd:         "DEL key extra_arg",
			description: "DEL command with excessive arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.cmd)

			assert.Error(t, err)
			assert.Equal(t, ErrInvalidArgumentsNumber, err)
			assert.Equal(t, Query{}, query)
		})
	}
}

func TestCompute_Parse_ValidCommands(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name        string
		cmd         string
		description string
		commandID   int
		arguments   []string
	}{
		{
			name:        "GET command",
			cmd:         "GET key",
			description: "Valid GET command",
			commandID:   GetCommandID,
			arguments:   []string{"key"},
		},
		{
			name:        "SET command",
			cmd:         "SET key value",
			description: "Valid SET command",
			commandID:   SetCommandID,
			arguments:   []string{"key", "value"},
		},
		{
			name:        "DEL command",
			cmd:         "DEL key",
			description: "Valid DEL command",
			commandID:   DelCommandID,
			arguments:   []string{"key"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.cmd)
			assert.NoError(t, err)
			assert.NotEqual(t, Query{}, query)
			assert.Equal(t, tt.commandID, query.CommandID())
			assert.Equal(t, tt.arguments, query.Arguments())
		})
	}
}

func TestCompute_Parse_WhitespaceHandling(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name string
		cmd  string
	}{
		{
			name: "extra spaces between words",
			cmd:  "GET    key1",
		},
		{
			name: "leading spaces",
			cmd:  "   GET key1",
		},
		{
			name: "trailing spaces",
			cmd:  "GET key1   ",
		},
		{
			name: "mixed whitespace",
			cmd:  "  GET   key1  ",
		},
		{
			name: "tabs and spaces mixed",
			cmd:  "\tGET\t\tkey1\t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.cmd)

			assert.NoError(t, err)
			assert.NotEqual(t, Query{}, query)
			assert.Equal(t, GetCommandID, query.CommandID())
			assert.Equal(t, []string{"key1"}, query.Arguments())
		})
	}
}

func TestCompute_Parse_CaseSensitivity(t *testing.T) {
	compute := NewCompute()

	tests := []struct {
		name      string
		cmd       string
		wantError bool
	}{
		{
			name:      "lowercase command",
			cmd:       "get key1",
			wantError: true,
		},
		{
			name:      "uppercase correct command",
			cmd:       "GET key1",
			wantError: false,
		},
		{
			name:      "mixed case command",
			cmd:       "GeT key1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := compute.Parse(tt.cmd)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, ErrUnknownCommand, err)
				assert.Equal(t, Query{}, query)
			} else {
				assert.NoError(t, err)
				assert.NotEqual(t, Query{}, query)
				assert.Equal(t, GetCommandID, query.CommandID())
				assert.Equal(t, []string{"key1"}, query.Arguments())
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

		assert.Contains(t, ErrEmptyCommand.Error(), "empty command")
		assert.Contains(t, ErrUnknownCommand.Error(), "unknown command")
		assert.Contains(t, ErrInvalidArgumentsNumber.Error(), "invalid arguments number")
	})
}
