package compute

import (
	"encoding/binary"
	"errors"
)

type Compute struct{}

var (
	ErrEmptyCommand           = errors.New("empty command")
	ErrUnknownCommand         = errors.New("unknown command")
	ErrInvalidArgumentsNumber = errors.New("invalid arguments number")
	ErrInvalidProtocol        = errors.New("invalid protocol format")
)

func NewCompute() *Compute {
	return &Compute{}
}

// Parse parses a binary protocol command
// Protocol format:
// - 1 byte: command length
// - N bytes: command data
// - 2 bytes: key length (big-endian uint16)
// - K bytes: key data
// - 4 bytes: value length (big-endian uint32)
// - V bytes: value data (empty for GET/DEL commands)
func (c *Compute) Parse(data []byte) (Query, error) {
	if len(data) == 0 {
		return Query{}, ErrEmptyCommand
	}

	offset := 0

	// Parse command
	commandID, newOffset, err := c.parseCommand(data, offset)
	if err != nil {
		return Query{}, err
	}
	offset = newOffset

	// Parse key
	key, newOffset, err := c.parseKey(data, offset)
	if err != nil {
		return Query{}, err
	}
	offset = newOffset

	// Parse value
	valueLen, newOffset, err := c.parseValueLength(data, offset)
	if err != nil {
		return Query{}, err
	}
	offset = newOffset

	// Build arguments based on command type
	arguments, err := c.buildArguments(commandID, key, data, offset, valueLen)
	if err != nil {
		return Query{}, err
	}

	return NewQuery(commandID, arguments), nil
}

// parseCommand reads and validates the command from the protocol data
func (c *Compute) parseCommand(data []byte, offset int) (int, int, error) {
	// Read command length
	if offset >= len(data) {
		return 0, offset, ErrInvalidProtocol
	}
	cmdLen := int(data[offset])
	offset++

	// Read command data
	if offset+cmdLen > len(data) {
		return 0, offset, ErrInvalidProtocol
	}
	command := string(data[offset : offset+cmdLen])
	offset += cmdLen

	// Validate command
	commandID := commandNameToCommandID(command)
	if commandID == UnknownCommandID {
		return 0, offset, ErrUnknownCommand
	}

	return commandID, offset, nil
}

// parseKey reads the key from the protocol data
func (c *Compute) parseKey(data []byte, offset int) (string, int, error) {
	// Read key length
	if offset+2 > len(data) {
		return "", offset, ErrInvalidProtocol
	}
	keyLen := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Read key data
	if offset+int(keyLen) > len(data) {
		return "", offset, ErrInvalidProtocol
	}
	key := string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	return key, offset, nil
}

// parseValueLength reads the value length from the protocol data
func (c *Compute) parseValueLength(data []byte, offset int) (uint32, int, error) {
	// Read value length
	if offset+4 > len(data) {
		return 0, offset, ErrInvalidProtocol
	}
	valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	return valueLen, offset, nil
}

// buildArguments constructs the arguments array based on command type and validates requirements
func (c *Compute) buildArguments(commandID int, key string, data []byte, offset int, valueLen uint32) ([]string, error) {
	// Validate we have enough data for the value
	if offset+int(valueLen) > len(data) {
		return nil, ErrInvalidProtocol
	}

	if commandID == SetCommandID {
		// SET command requires key and value
		if valueLen == 0 {
			return nil, ErrInvalidArgumentsNumber
		}
		value := string(data[offset : offset+int(valueLen)])
		return []string{key, value}, nil
	} else {
		// GET and DEL commands only require key
		if valueLen != 0 {
			return nil, ErrInvalidArgumentsNumber
		}
		return []string{key}, nil
	}
}
