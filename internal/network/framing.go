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
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		logging.Warn("Failed to set read deadline", zap.Error(err))
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Small header allocation is very fast
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	// Extract message length from header
	messageLen := binary.BigEndian.Uint32(header)

	// Validate message size
	if int(messageLen) > maxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds maximum allowed size %d", messageLen, maxMessageSize)
	}

	if messageLen == 0 {
		return []byte{}, nil
	}

	// Read the actual message
	messageData := make([]byte, messageLen)
	if _, err := io.ReadFull(conn, messageData); err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	return messageData, nil
}

// ReadFramedMessageInPlace reads a length-prefixed message directly into the provided buffer
// Returns a slice pointing to the data within the buffer (zero-copy when possible)
// The caller must process the data before the next call to avoid buffer reuse issues
func ReadFramedMessageInPlace(conn net.Conn, maxMessageSize int, timeout time.Duration, buffer []byte) ([]byte, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		logging.Warn("Failed to set read deadline", zap.Error(err))
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Small header allocation is very fast
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	// Extract message length from header
	messageLen := binary.BigEndian.Uint32(header)

	// Validate message size
	if int(messageLen) > maxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds maximum allowed size %d", messageLen, maxMessageSize)
	}

	if messageLen == 0 {
		return []byte{}, nil
	}

	// Check if provided buffer is large enough
	if len(buffer) < int(messageLen) {
		return nil, fmt.Errorf("provided buffer size %d is too small for message size %d", len(buffer), messageLen)
	}

	// Read directly into the provided buffer (zero-copy)
	messageData := buffer[:messageLen]
	if _, err := io.ReadFull(conn, messageData); err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// Return slice pointing to buffer data (caller must process before next call)
	return messageData, nil
}

// ReadFramedMessageWithBuffer reads a length-prefixed message using a provided buffer
// This version reuses buffers to reduce allocations for better performance
// Returns a copy of the data to avoid buffer reuse issues
func ReadFramedMessageWithBuffer(conn net.Conn, maxMessageSize int, timeout time.Duration, buffer []byte) ([]byte, error) {
	// Read directly into buffer
	messageData, err := ReadFramedMessageInPlace(conn, maxMessageSize, timeout, buffer)
	if err != nil {
		return nil, err
	}

	// Return a copy to avoid buffer reuse issues
	result := make([]byte, len(messageData))
	copy(result, messageData)
	return result, nil
}
