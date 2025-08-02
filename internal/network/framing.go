package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"go.uber.org/zap"

	"github.com/EzhovAndrew/kv-db/internal/logging"
)

const (
	// FrameHeaderSize is the size of the length prefix in bytes (4 bytes for uint32)
	FrameHeaderSize = 4
)

// setReadDeadline sets a read deadline on the connection with proper error handling
func setReadDeadline(conn net.Conn, timeout time.Duration) error {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		logging.Warn("Failed to set read deadline", zap.Error(err))
		return fmt.Errorf("failed to set read deadline: %w", err)
	}
	return nil
}

// readMessageHeader reads and validates the 4-byte message length header
// Returns the message length or an error
func readMessageHeader(conn net.Conn, maxMessageSize int) (uint32, error) {
	// Read the 4-byte length prefix
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, fmt.Errorf("failed to read message length: %w", err)
	}

	// Extract message length from header
	messageLen := binary.BigEndian.Uint32(header)

	// Validate message size
	if int(messageLen) > maxMessageSize {
		return 0, fmt.Errorf("message size %d exceeds maximum allowed size %d", messageLen, maxMessageSize)
	}

	return messageLen, nil
}

// readMessageData reads message data into the provided buffer or allocates a new one
// If buffer is nil, allocates a new slice. Otherwise uses the provided buffer.
func readMessageData(conn net.Conn, messageLen uint32, buffer []byte) ([]byte, error) {
	if messageLen == 0 {
		return []byte{}, nil
	}

	var messageData []byte

	if buffer == nil {
		// Allocate new buffer
		messageData = make([]byte, messageLen)
	} else {
		// Use provided buffer
		if len(buffer) < int(messageLen) {
			return nil, fmt.Errorf("provided buffer size %d is too small for message size %d", len(buffer), messageLen)
		}
		messageData = buffer[:messageLen]
	}

	// Read the message data
	if _, err := io.ReadFull(conn, messageData); err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	return messageData, nil
}

// WriteFramedMessage writes a message with a 4-byte length prefix
// The length prefix is written in big-endian format
//
// Note: The write deadline is set once for the entire operation (header + data).
// This ensures the total framed message write completes within the timeout,
// rather than giving each individual write the full timeout duration.
func WriteFramedMessage(conn net.Conn, data []byte, timeout time.Duration) error {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("unable to set write deadline: %w", err)
	}

	messageLen := uint32(len(data))

	// Small header allocation is very fast - no need for pooling
	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(header, messageLen)

	// Write length prefix first (4 bytes - should be very fast)
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("unable to write message length: %w", err)
	}

	// Write the actual message (skip if empty)
	// Uses the same deadline set above - any remaining time from the total timeout
	if len(data) > 0 {
		if _, err := conn.Write(data); err != nil {
			return fmt.Errorf("unable to write message data: %w", err)
		}
	}

	return nil
}

// ReadFramedMessage reads a length-prefixed message
// First reads 4 bytes to get the message length, then reads exactly that many bytes
func ReadFramedMessage(conn net.Conn, maxMessageSize int, timeout time.Duration) ([]byte, error) {
	if err := setReadDeadline(conn, timeout); err != nil {
		return nil, err
	}

	messageLen, err := readMessageHeader(conn, maxMessageSize)
	if err != nil {
		return nil, err
	}

	return readMessageData(conn, messageLen, nil)
}

// ReadFramedMessageInPlace reads a length-prefixed message directly into the provided buffer
// Returns a slice pointing to the data within the buffer (zero-copy when possible)
// The caller must process the data before the next call to avoid buffer reuse issues
func ReadFramedMessageInPlace(conn net.Conn, maxMessageSize int, timeout time.Duration, buffer []byte) ([]byte, error) {
	if err := setReadDeadline(conn, timeout); err != nil {
		return nil, err
	}

	messageLen, err := readMessageHeader(conn, maxMessageSize)
	if err != nil {
		return nil, err
	}

	return readMessageData(conn, messageLen, buffer)
}
