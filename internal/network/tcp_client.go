package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
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

	// Health monitoring
	healthMutex      sync.RWMutex
	healthCtx        context.Context
	healthCancel     context.CancelFunc
	healthWg         sync.WaitGroup
	isHealthy        bool
	lastPingTime     time.Time
	consecutiveFails int

	// Push model support
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
		isHealthy:  false,
		isPushMode: false,
	}
}

func (c *TCPClient) Connect(ctx context.Context) error {
	if c.conn != nil {
		return nil // Already connected
	}

	address := net.JoinHostPort(c.cfg.Ip, c.cfg.Port)

	dialer := &net.Dialer{
		Timeout: c.timeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	c.conn = conn
	c.setHealthy(true)
	logging.Info("Connected to server", zap.String("address", address))
	return nil
}

func (c *TCPClient) SendMessage(ctx context.Context, message []byte) ([]byte, error) {
	if c.conn == nil {
		return nil, ErrConnectionClosed
	}

	// Check message size limit
	if len(message) > c.cfg.MaxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds limit %d", len(message), c.cfg.MaxMessageSize)
	}

	// Set write deadline
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		logging.Warn("Failed to set write deadline", zap.Error(err))
		return nil, fmt.Errorf("failed to set write deadline: %w", err)
	}

	// Send message
	_, err := c.conn.Write(message)
	if err != nil {
		logging.Error("Failed to send message", zap.Error(err))
		c.setHealthy(false)
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	// Set read deadline
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		logging.Warn("Failed to set read deadline", zap.Error(err))
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read response
	buffer := make([]byte, c.cfg.MaxMessageSize+1)
	n, err := c.conn.Read(buffer)
	if err != nil {
		logging.Error("Failed to read response", zap.Error(err))
		c.setHealthy(false)
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if n == c.cfg.MaxMessageSize+1 {
		return nil, fmt.Errorf("response size exceeds maximum allowed size")
	}

	return buffer[:n], nil
}

func (c *TCPClient) SendMessageWithTimeout(ctx context.Context, message []byte, timeout time.Duration) ([]byte, error) {
	if c.conn == nil {
		return nil, ErrConnectionClosed
	}

	// Create a context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Channel to receive the result
	resultChan := make(chan struct {
		response []byte
		err      error
	}, 1)

	// Send message in a goroutine
	go func() {
		response, err := c.SendMessage(timeoutCtx, message)
		resultChan <- struct {
			response []byte
			err      error
		}{response, err}
	}()

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		return result.response, result.err
	case <-timeoutCtx.Done():
		c.setHealthy(false)
		return nil, ErrResponseTimeout
	}
}

// StopPushMode stops the push mode listening
func (c *TCPClient) StopPushMode() {
	c.pushMutex.Lock()
	defer c.pushMutex.Unlock()

	if c.pushCancel != nil {
		c.pushCancel()
		c.pushWg.Wait()
		c.pushCancel = nil
		c.isPushMode = false
		logging.Info("Push mode stopped")
	}
}

// IsPushMode returns whether client is in push mode
func (c *TCPClient) IsPushMode() bool {
	c.pushMutex.RLock()
	defer c.pushMutex.RUnlock()
	return c.isPushMode
}

func (c *TCPClient) Close() error {
	// Stop push mode first
	c.StopPushMode()

	// Stop health monitoring
	c.StopHealthMonitoring()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	c.setHealthy(false)

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

// Ping sends a simple ping message to test connectivity
func (c *TCPClient) Ping(ctx context.Context) error {
	_, err := c.SendMessage(ctx, []byte("PING"))
	return err
}

// SendBatch sends multiple messages in sequence
func (c *TCPClient) SendBatch(ctx context.Context, messages [][]byte) ([][]byte, error) {
	if c.conn == nil {
		return nil, ErrConnectionClosed
	}

	responses := make([][]byte, 0, len(messages))

	for i, message := range messages {
		select {
		case <-ctx.Done():
			return responses, ctx.Err()
		default:
		}

		response, err := c.SendMessage(ctx, message)
		if err != nil {
			return responses, fmt.Errorf("failed to send message %d: %w", i, err)
		}
		responses = append(responses, response)
	}

	return responses, nil
}

// StartHealthMonitoring starts sending ping messages every second to monitor connection health
func (c *TCPClient) StartHealthMonitoring(ctx context.Context) {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	// Stop existing monitoring if running
	if c.healthCancel != nil {
		c.healthCancel()
		c.healthWg.Wait()
	}

	// Create new context for health monitoring
	c.healthCtx, c.healthCancel = context.WithCancel(ctx)

	c.healthWg.Add(1)
	go c.healthMonitorLoop()

	logging.Info("Health monitoring started")
}

// StopHealthMonitoring stops the health monitoring
func (c *TCPClient) StopHealthMonitoring() {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	if c.healthCancel != nil {
		c.healthCancel()
		c.healthWg.Wait()
		c.healthCancel = nil
		logging.Info("Health monitoring stopped")
	}
}

// IsHealthy returns the current health status of the connection
func (c *TCPClient) IsHealthy() bool {
	c.healthMutex.RLock()
	defer c.healthMutex.RUnlock()
	return c.isHealthy
}

// GetLastPingTime returns the time of the last successful ping
func (c *TCPClient) GetLastPingTime() time.Time {
	c.healthMutex.RLock()
	defer c.healthMutex.RUnlock()
	return c.lastPingTime
}

// GetConsecutiveFailures returns the number of consecutive ping failures
func (c *TCPClient) GetConsecutiveFailures() int {
	c.healthMutex.RLock()
	defer c.healthMutex.RUnlock()
	return c.consecutiveFails
}

func (c *TCPClient) performHealthCheck() {
	if c.conn == nil {
		c.setHealthy(false)
		return
	}

	// Create a timeout context for the ping
	pingCtx, cancel := context.WithTimeout(c.healthCtx, 5*time.Second)
	defer cancel()

	err := c.Ping(pingCtx)

	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	if err != nil {
		c.consecutiveFails++
		c.isHealthy = false

		logging.Warn("Health check failed",
			zap.Error(err),
			zap.Int("consecutive_failures", c.consecutiveFails),
			zap.String("remote_addr", c.getRemoteAddrString()))

		// If we have too many consecutive failures, consider the connection dead
		if c.consecutiveFails >= 3 {
			logging.Error("Connection considered unhealthy after consecutive failures",
				zap.Int("consecutive_failures", c.consecutiveFails),
				zap.String("remote_addr", c.getRemoteAddrString()))
		}
	} else {
		// Reset failure count on successful ping
		if c.consecutiveFails > 0 {
			logging.Info("Health check succeeded after consecutive failures",
				zap.Int("consecutive_failures", c.consecutiveFails),
				zap.String("remote_addr", c.getRemoteAddrString()))
		}
		c.consecutiveFails = 0
		c.isHealthy = true
		c.lastPingTime = time.Now()
	}
}

// setHealthy sets the health status (thread-safe)
func (c *TCPClient) setHealthy(healthy bool) {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()
	c.isHealthy = healthy
	if healthy {
		c.consecutiveFails = 0
		c.lastPingTime = time.Now()
	}
}

// getRemoteAddrString safely gets the remote address as string
func (c *TCPClient) getRemoteAddrString() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return "unknown"
}

// WaitForHealthy waits until the connection becomes healthy or timeout occurs
func (c *TCPClient) WaitForHealthy(ctx context.Context, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if c.IsHealthy() {
			return nil
		}

		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for healthy connection")
		case <-ticker.C:
			// Continue checking
		}
	}
}

func (c *TCPClient) pushModeLoop() {
	defer c.pushWg.Done()

	buffer := make([]byte, c.cfg.MaxMessageSize+1)

	for {
		select {
		case <-c.pushCtx.Done():
			logging.Info("Push mode loop stopped due to context cancellation")
			return
		default:
		}

		// Set read deadline to allow periodic context checking
		if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			logging.Error("Failed to set read deadline in push mode", zap.Error(err))
			c.setHealthy(false)
			return
		}

		n, err := c.conn.Read(buffer)
		if err != nil {
			// Check if it's a timeout (expected for context checking)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Continue loop to check context
			}

			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				logging.Info("Connection closed in push mode")
				c.setHealthy(false)
				return
			}

			logging.Error("Failed to read in push mode", zap.Error(err))
			c.setHealthy(false)
			return
		}

		if n == c.cfg.MaxMessageSize+1 {
			logging.Warn("Received message exceeds maximum size in push mode, skipping")
			continue
		}

		if n == 0 {
			logging.Info("Received empty message in push mode, skipping")
			continue
		}

		message := make([]byte, n)
		copy(message, buffer[:n])

		response := c.messageHandler(c.pushCtx, message)

		// Send response back to master
		if err := c.sendResponse(response); err != nil {
			logging.Error("Failed to send response in push mode", zap.Error(err))
			c.setHealthy(false)
			return
		}

		logging.Info("Processed push message and sent response",
			zap.Int("message_size", n),
			zap.Int("response_size", len(response)))
	}
}

func (c *TCPClient) sendResponse(response []byte) error {
	if c.conn == nil {
		return ErrConnectionClosed
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

func (c *TCPClient) StartPushMode(ctx context.Context, handler MessageHandler) error {
	if c.conn == nil {
		return ErrConnectionClosed
	}

	if handler == nil {
		return fmt.Errorf("message handler cannot be nil")
	}

	c.pushMutex.Lock()
	defer c.pushMutex.Unlock()

	// Stop existing push mode if running
	if c.pushCancel != nil {
		c.pushCancel()
		c.pushWg.Wait()
	}

	c.messageHandler = handler
	c.isPushMode = true
	c.pushCtx, c.pushCancel = context.WithCancel(ctx)

	c.pushWg.Add(1)
	go c.pushModeLoop()

	logging.Info("Push mode started - listening for master messages",
		zap.String("remote_addr", c.getRemoteAddrString()))
	return nil
}

// Fixed healthMonitorLoop to avoid interference with push mode
func (c *TCPClient) healthMonitorLoop() {
	defer c.healthWg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.healthCtx.Done():
			logging.Info("Health monitor loop stopped due to context cancellation")
			return
		case <-ticker.C:
			// Skip health checks in push mode to avoid interfering with message flow
			if !c.IsPushMode() {
				c.performHealthCheck()
			} else {
				// In push mode, we consider connection healthy if push mode is running
				// and we haven't detected any errors
				c.healthMutex.RLock()
				pushModeRunning := c.isPushMode
				c.healthMutex.RUnlock()

				if pushModeRunning && c.conn != nil {
					c.setHealthy(true)
				}
			}
		}
	}
}
