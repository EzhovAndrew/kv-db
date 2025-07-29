package network

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestFramedMessaging(t *testing.T) {
	// Create a pipe for testing
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	timeout := 1 * time.Second
	maxMessageSize := 1024

	// Test small message
	t.Run("SmallMessage", func(t *testing.T) {
		originalMsg := []byte("Hello, World!")

		// Send message from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WriteFramedMessage(server, originalMsg, timeout)
			errChan <- err
		}()

		// Read message from client side
		receivedMsg, err := ReadFramedMessage(client, maxMessageSize, timeout)
		if err != nil {
			t.Fatalf("Failed to read framed message: %v", err)
		}

		// Wait for sender to complete and check for errors
		wg.Wait()
		close(errChan)
		if err := <-errChan; err != nil {
			t.Errorf("Failed to write framed message: %v", err)
		}

		if string(receivedMsg) != string(originalMsg) {
			t.Errorf("Message mismatch. Expected: %s, Got: %s", originalMsg, receivedMsg)
		}
	})

	// Test large message
	t.Run("LargeMessage", func(t *testing.T) {
		// Create a 500-byte message
		originalMsg := make([]byte, 500)
		for i := range originalMsg {
			originalMsg[i] = byte(i % 256)
		}

		// Send message from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WriteFramedMessage(server, originalMsg, timeout)
			errChan <- err
		}()

		// Read message from client side
		receivedMsg, err := ReadFramedMessage(client, maxMessageSize, timeout)
		if err != nil {
			t.Fatalf("Failed to read framed message: %v", err)
		}

		// Wait for sender to complete and check for errors
		wg.Wait()
		close(errChan)
		if err := <-errChan; err != nil {
			t.Errorf("Failed to write framed message: %v", err)
		}

		if len(receivedMsg) != len(originalMsg) {
			t.Errorf("Message length mismatch. Expected: %d, Got: %d", len(originalMsg), len(receivedMsg))
		}

		for i := range originalMsg {
			if receivedMsg[i] != originalMsg[i] {
				t.Errorf("Message content mismatch at index %d. Expected: %d, Got: %d", i, originalMsg[i], receivedMsg[i])
				break
			}
		}
	})

	// Test multiple consecutive messages
	t.Run("MultipleMessages", func(t *testing.T) {
		messages := [][]byte{
			[]byte("First message"),
			[]byte("Second message with more content"),
			[]byte("Third"),
		}

		// Send multiple messages from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, msg := range messages {
				if err := WriteFramedMessage(server, msg, timeout); err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}()

		// Read multiple messages from client side
		for i, expectedMsg := range messages {
			receivedMsg, err := ReadFramedMessage(client, maxMessageSize, timeout)
			if err != nil {
				t.Fatalf("Failed to read framed message %d: %v", i, err)
			}

			if string(receivedMsg) != string(expectedMsg) {
				t.Errorf("Message %d mismatch. Expected: %s, Got: %s", i, expectedMsg, receivedMsg)
			}
		}

		// Wait for sender to complete and check for errors
		wg.Wait()
		close(errChan)
		if err := <-errChan; err != nil {
			t.Errorf("Failed to write framed messages: %v", err)
		}
	})

	// Test empty message
	t.Run("EmptyMessage", func(t *testing.T) {
		originalMsg := []byte{}

		// Use longer timeout for this specific test to avoid race conditions
		longerTimeout := 5 * time.Second

		// Send message from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WriteFramedMessage(server, originalMsg, longerTimeout)
			errChan <- err
		}()

		// Read message from client side
		receivedMsg, err := ReadFramedMessage(client, maxMessageSize, longerTimeout)
		if err != nil {
			t.Fatalf("Failed to read framed message: %v", err)
		}

		// Wait for sender to complete and check for errors
		wg.Wait()
		close(errChan)
		if err := <-errChan; err != nil {
			t.Errorf("Failed to write framed message: %v", err)
		}

		if len(receivedMsg) != 0 {
			t.Errorf("Expected empty message, got %d bytes", len(receivedMsg))
		}
	})
}

func TestFramedMessagingWithBuffer(t *testing.T) {
	// Create a pipe for testing
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	timeout := 1 * time.Second
	maxMessageSize := 1024
	buffer := make([]byte, maxMessageSize)

	// Test message with buffer reuse
	t.Run("MessageWithBuffer", func(t *testing.T) {
		originalMsg := []byte("Test message for buffer reuse")

		// Send message from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WriteFramedMessage(server, originalMsg, timeout)
			errChan <- err
		}()

		// Read message from client side using buffer
		receivedMsg, err := ReadFramedMessageWithBuffer(client, maxMessageSize, timeout, buffer)
		if err != nil {
			t.Fatalf("Failed to read framed message with buffer: %v", err)
		}

		// Wait for sender to complete and check for errors
		wg.Wait()
		close(errChan)
		if err := <-errChan; err != nil {
			t.Errorf("Failed to write framed message: %v", err)
		}

		if string(receivedMsg) != string(originalMsg) {
			t.Errorf("Message mismatch. Expected: %s, Got: %s", originalMsg, receivedMsg)
		}
	})

	// Test buffer too small
	t.Run("BufferTooSmall", func(t *testing.T) {
		originalMsg := []byte("This message is longer than the small buffer")
		smallBuffer := make([]byte, 10) // Too small for the message

		// Send message from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WriteFramedMessage(server, originalMsg, timeout)
			errChan <- err
		}()

		// Try to read with too small buffer
		_, err := ReadFramedMessageWithBuffer(client, maxMessageSize, timeout, smallBuffer)
		if err == nil {
			t.Errorf("Expected error due to small buffer, but got none")
		}

		// Wait for sender to complete - we expect it might timeout since reader failed
		wg.Wait()
		close(errChan)
		// Note: We don't check for write errors here since the reader failed first
	})
}

func TestFramedMessagingErrorCases(t *testing.T) {
	// Create a pipe for testing
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	timeout := 1 * time.Second

	// Test message size limit
	t.Run("MessageTooLarge", func(t *testing.T) {
		maxMessageSize := 100
		largeMsg := make([]byte, 200) // Larger than limit

		// Send large message from server side
		errChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WriteFramedMessage(server, largeMsg, timeout)
			errChan <- err
		}()

		// Try to read with size limit
		_, err := ReadFramedMessage(client, maxMessageSize, timeout)
		if err == nil {
			t.Errorf("Expected error due to message size limit, but got none")
		}

		// Wait for sender to complete - we expect it might timeout since reader failed
		wg.Wait()
		close(errChan)
		// Note: We don't check for write errors here since the reader failed due to size limit
	})
}
