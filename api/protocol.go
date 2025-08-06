package api

import (
	"encoding/binary"
)

// Binary protocol encoding functions for kv-db client
// These functions handle the client-side responsibility of encoding commands
// into the binary protocol format that the server can parse.

// Command constants as byte slices for efficiency
var (
	cmdGet = []byte("GET")
	cmdSet = []byte("SET")
	cmdDel = []byte("DEL")
)

// EncodeBinaryCommand encodes a command into binary protocol format
// Protocol format:
// - 1 byte: command length
// - N bytes: command data
// - 2 bytes: key length (big-endian uint16)
// - K bytes: key data
// - 4 bytes: value length (big-endian uint32)
// - V bytes: value data (empty for GET/DEL commands)
func EncodeBinaryCommand(command, key, value []byte) []byte {
	cmdLen := len(command)
	keyLen := len(key)
	valueLen := len(value)

	// Calculate total size
	totalSize := 1 + cmdLen + 2 + keyLen + 4 + valueLen
	data := make([]byte, totalSize)

	offset := 0

	// Write command length
	data[offset] = byte(cmdLen)
	offset++

	// Write command data
	copy(data[offset:offset+cmdLen], command)
	offset += cmdLen

	// Write key length
	binary.BigEndian.PutUint16(data[offset:offset+2], uint16(keyLen))
	offset += 2

	// Write key data
	copy(data[offset:offset+keyLen], key)
	offset += keyLen

	// Write value length
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(valueLen))
	offset += 4

	// Write value data
	if valueLen > 0 {
		copy(data[offset:offset+valueLen], value)
	}

	return data
}

// EncodeBinaryGet encodes a GET command
func EncodeBinaryGet(key []byte) []byte {
	return EncodeBinaryCommand(cmdGet, key, nil)
}

// EncodeBinarySet encodes a SET command
func EncodeBinarySet(key, value []byte) []byte {
	return EncodeBinaryCommand(cmdSet, key, value)
}

// EncodeBinaryDel encodes a DEL command
func EncodeBinaryDel(key []byte) []byte {
	return EncodeBinaryCommand(cmdDel, key, nil)
}
