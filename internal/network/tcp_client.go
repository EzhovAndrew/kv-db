package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/logging"
)

var (
	ErrConnectionClosed = errors.New("connection is closed")
	ErrResponseTimeout  = errors.New("response timeout")
)

// MessageHandler handles incoming messages from master (for push model)
type MessageHandler func(ctx context.Context, message []byte) []byte

type TCPClient struct {
	conn    net.Conn
	cfg     *configuration.NetworkConfig
	timeout time.Duration

	// Push model support for slave connections
	pushMutex      sync.RWMutex
	pushCtx        context.Context
	pushCancel     context.CancelFunc
	pushWg         sync.WaitGroup
	messageHandler MessageHandler
	isPushMode     bool
}

func NewTCPClient(cfg *configuration.NetworkConfig) *TCPClient {
	return &TCPClient{
		cfg:        cfg,
		timeout:    time.Second * time.Duration(cfg.IdleTimeout),
		isPushMode: false,
	}
}

func (c *TCPClient) Connect(ctx context.Context) error {
	if c.conn != nil {
		return nil // Already connected
	}

	address := net.JoinHostPort(c.cfg.Ip, c.cfg.Port)
	conn, err := c.dialWithTimeout(ctx, address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	c.conn = conn
	logging.Info("Connected to server", zap.String("address", address))
	return nil
}

func (c *TCPClient) SendMessage(ctx context.Context, message []byte) ([]byte, error) {
	if err := c.validateConnection(); err != nil {
		return nil, err
	}

	if err := c.validateMessageSize(message); err != nil {
		return nil, err
	}

	if err := c.writeMessage(message); err != nil {
		return nil, err
	}

	return c.readResponse()
}

func (c *TCPClient) SendMessageWithTimeout(ctx context.Context, message []byte, timeout time.Duration) ([]byte, error) {
	if err := c.validateConnection(); err != nil {
		return nil, err
	}

	return c.sendMessageAsync(ctx, message, timeout)
}

func (c *TCPClient) StartPushMode(ctx context.Context, handler MessageHandler) error {
	if err := c.validateConnection(); err != nil {
		return err
	}

	if handler == nil {
		return fmt.Errorf("message handler cannot be nil")
	}

	return concurrency.WithLock(&c.pushMutex, func() error {
		c.stopExistingPushMode()
		c.startNewPushMode(ctx, handler)
		return nil
	})
}

func (c *TCPClient) StopPushMode() {
	concurrency.WithLock(&c.pushMutex, func() error { // nolint:errcheck
		c.stopExistingPushMode()
		return nil
	})
}

func (c *TCPClient) IsPushMode() bool {
	var result bool
	concurrency.WithLock(c.pushMutex.RLocker(), func() error { // nolint:errcheck
		result = c.isPushMode
		return nil
	})
	return result
}

func (c *TCPClient) Close() error {
	c.StopPushMode()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil

	if err != nil {
		logging.Error("Failed to close connection", zap.Error(err))
		return fmt.Errorf("failed to close connection: %w", err)
	}

	logging.Info("Connection closed")
	return nil
}

func (c *TCPClient) IsConnected() bool {
	return c.conn != nil
}

func (c *TCPClient) RemoteAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

func (c *TCPClient) LocalAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr()
}

// Private helper methods for better organization

func (c *TCPClient) dialWithTimeout(ctx context.Context, address string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: c.timeout,
	}
	return dialer.DialContext(ctx, "tcp", address)
}

func (c *TCPClient) validateConnection() error {
	if c.conn == nil {
		return ErrConnectionClosed
	}
	return nil
}

func (c *TCPClient) validateMessageSize(message []byte) error {
	if len(message) > c.cfg.MaxMessageSize {
		return fmt.Errorf("message size %d exceeds limit %d", len(message), c.cfg.MaxMessageSize)
	}
	return nil
}

func (c *TCPClient) writeMessage(message []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		logging.Warn("Failed to set write deadline", zap.Error(err))
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	_, err := c.conn.Write(message)
	if err != nil {
		logging.Error("Failed to send message", zap.Error(err))
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (c *TCPClient) readResponse() ([]byte, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		logging.Warn("Failed to set read deadline", zap.Error(err))
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	buffer := make([]byte, c.cfg.MaxMessageSize+1)
	n, err := c.conn.Read(buffer)
	if err != nil {
		logging.Error("Failed to read response", zap.Error(err))
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if n > c.cfg.MaxMessageSize {
		return nil, fmt.Errorf("response size %d exceeds maximum allowed size %d", n, c.cfg.MaxMessageSize)
	}

	return buffer[:n], nil
}

func (c *TCPClient) sendMessageAsync(ctx context.Context, message []byte, timeout time.Duration) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultChan := make(chan struct {
		response []byte
		err      error
	}, 1)

	go func() {
		response, err := c.SendMessage(timeoutCtx, message)
		resultChan <- struct {
			response []byte
			err      error
		}{response, err}
	}()

	select {
	case result := <-resultChan:
		return result.response, result.err
	case <-timeoutCtx.Done():
		return nil, ErrResponseTimeout
	}
}

func (c *TCPClient) stopExistingPushMode() {
	if c.pushCancel != nil {
		c.pushCancel()
		c.pushWg.Wait()
		c.pushCancel = nil
		c.isPushMode = false
		logging.Info("Push mode stopped")
	}
}

func (c *TCPClient) startNewPushMode(ctx context.Context, handler MessageHandler) {
	c.messageHandler = handler
	c.isPushMode = true
	c.pushCtx, c.pushCancel = context.WithCancel(ctx)

	c.pushWg.Add(1)
	go c.pushModeLoop()

	logging.Info("Push mode started - listening for master messages",
		zap.String("remote_addr", c.getRemoteAddrString()))
}

func (c *TCPClient) pushModeLoop() {
	defer c.pushWg.Done()

	for {
		if c.shouldStopPushMode() {
			logging.Info("Push mode loop stopped due to context cancellation")
			return
		}

		message, err := c.readPushMessage()
		if err != nil {
			if c.handlePushReadError(err) {
				continue // Continue for timeout errors
			}
			return // Exit for serious errors
		}

		if message == nil {
			continue // Skip invalid messages
		}

		c.processPushMessage(message)
	}
}

func (c *TCPClient) shouldStopPushMode() bool {
	select {
	case <-c.pushCtx.Done():
		return true
	default:
		return false
	}
}

func (c *TCPClient) readPushMessage() ([]byte, error) {
	timeout := time.Second * time.Duration(c.cfg.IdleTimeout)
	return ReadFramedMessage(c.conn, c.cfg.MaxMessageSize, timeout)
}

func (c *TCPClient) handlePushReadError(err error) bool {
	// Check if it's a timeout (expected for context checking)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true // Continue loop
	}

	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
		logging.Info("Connection closed in push mode")
		return false // Exit loop
	}

	logging.Error("Failed to read in push mode", zap.Error(err))
	return false // Exit loop
}

func (c *TCPClient) processPushMessage(message []byte) {
	response := c.messageHandler(c.pushCtx, message)

	if err := c.sendResponse(response); err != nil {
		logging.Error("Failed to send response in push mode", zap.Error(err))
		return
	}

	logging.Info("Processed push message and sent response",
		zap.Int("message_size", len(message)),
		zap.Int("response_size", len(response)))
}

func (c *TCPClient) sendResponse(response []byte) error {
	if err := c.validateConnection(); err != nil {
		return err
	}

	if len(response) > c.cfg.MaxMessageSize {
		return fmt.Errorf("response size %d exceeds limit %d", len(response), c.cfg.MaxMessageSize)
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	n, err := c.conn.Write(response)
	if err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	if n != len(response) {
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", n, len(response))
	}

	return nil
}

func (c *TCPClient) getRemoteAddrString() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return "unknown"
}
