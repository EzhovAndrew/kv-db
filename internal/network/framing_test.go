package network

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
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

func TestTCPClientFramedMessaging(t *testing.T) {
	// This test simulates the LSN sync handshake to ensure framing works correctly
	server, clientConn := net.Pipe()
	defer server.Close()
	defer clientConn.Close()

	// Create TCP client with the connection
	client := NewTCPClient(&configuration.NetworkConfig{
		Ip:                      "127.0.0.1",
		Port:                    "3333",
		MaxMessageSize:          1024,
		IdleTimeout:             5,
		MaxConnections:          10,
		GracefulShutdownTimeout: 5,
	})
	client.conn = clientConn // Directly set the connection for testing

	t.Run("SendFramedMessage", func(t *testing.T) {
		testMessage := []byte(`{"type":"lsn_sync","last_lsn":42,"slave_id":"test-slave"}`)

		// Server side that reads framed message and sends framed response
		serverDone := make(chan error, 1)
		go func() {
			defer close(serverDone)

			// Read the framed message
			receivedData, err := ReadFramedMessage(server, 1024, 5*time.Second)
			if err != nil {
				serverDone <- err
				return
			}

			if string(receivedData) != string(testMessage) {
				serverDone <- fmt.Errorf("message mismatch: expected %s, got %s", testMessage, receivedData)
				return
			}

			// Send framed response
			response := []byte(`{"status":"OK","slave_id":"test-slave"}`)
			err = WriteFramedMessage(server, response, 5*time.Second)
			serverDone <- err
		}()

		// Client sends framed message and receives framed response
		ctx := context.Background()
		response, err := client.SendFramedMessage(ctx, testMessage)
		if err != nil {
			t.Fatalf("Failed to send framed message: %v", err)
		}

		expectedResponse := `{"status":"OK","slave_id":"test-slave"}`
		if string(response) != expectedResponse {
			t.Errorf("Response mismatch: expected %s, got %s", expectedResponse, response)
		}

		// Wait for server to complete
		if err := <-serverDone; err != nil {
			t.Errorf("Server error: %v", err)
		}
	})
}
