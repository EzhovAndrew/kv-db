package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/logging"
)

type TCPHandler = func(ctx context.Context, data []byte) []byte

type StreamHandler = func(ctx context.Context, initialData []byte) iter.Seq2[[]byte, error]

type TCPServer struct {
	listener      net.Listener
	semaphore     *concurrency.Semaphore
	connectionsWg sync.WaitGroup
	bufferPool    sync.Pool

	cfg *configuration.NetworkConfig
}

func NewTCPServer(cfg *configuration.NetworkConfig) (*TCPServer, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.Ip, cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("unable to start listener: %w", err)
	}
	return &TCPServer{
		listener:  listener,
		semaphore: concurrency.NewSemaphore(cfg.MaxConnections),
		cfg:       cfg,
		bufferPool: sync.Pool{
			New: func() any {
				slice := make([]byte, cfg.MaxMessageSize+1)
				return &slice
			},
		},
	}, nil
}

func (s *TCPServer) HandleRequests(ctx context.Context, handler TCPHandler) {
	s.handleConnections(ctx, func(ctx context.Context, conn net.Conn) {
		s.handleConnection(ctx, conn, handler)
	})
}

func (s *TCPServer) HandleStreamRequests(ctx context.Context, handler StreamHandler) {
	s.handleConnections(ctx, func(ctx context.Context, conn net.Conn) {
		s.handleStreamConnection(ctx, conn, handler)
	})
}

func (s *TCPServer) handleConnections(ctx context.Context, connectionHandler func(context.Context, net.Conn)) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.acceptConnections(ctx, connectionHandler)
	}()

	s.waitForShutdown(ctx)
	wg.Wait()
	s.gracefulShutdown()
}

func (s *TCPServer) handleConnection(ctx context.Context, conn net.Conn, handler TCPHandler) {
	defer s.recoverAndCloseConnection(conn, "connection")

	buffer := s.bufferPool.Get().(*[]byte)
	defer s.bufferPool.Put(buffer)

	for {
		if s.shouldStopConnection(ctx) {
			return
		}

		data, err := s.readMessage(conn, buffer)
		if err != nil {
			if s.isConnectionClosed(err) {
				return
			}
			logging.Warn("unable to read from connection", zap.Error(err))
			return
		}

		if !s.validateMessageSize(len(data), conn.RemoteAddr().String()) {
			return
		}

		response := handler(ctx, data)
		if err := s.writeWithDeadline(conn, response); err != nil {
			logging.Warn("unable to write response to connection",
				zap.String("address", conn.RemoteAddr().String()),
				zap.Error(err))
			return
		}
	}
}

func (s *TCPServer) handleStreamConnection(ctx context.Context, conn net.Conn, handler StreamHandler) {
	defer s.recoverAndCloseConnection(conn, "stream connection")

	buffer := s.bufferPool.Get().(*[]byte)
	defer s.bufferPool.Put(buffer)

	initialData, err := s.readInitialData(conn, buffer)
	if err != nil {
		return
	}

	if !s.validateMessageSize(len(initialData), conn.RemoteAddr().String()) {
		return
	}

	s.processDataStream(ctx, conn, handler, initialData)
}

func (s *TCPServer) acceptConnections(ctx context.Context, connectionHandler func(context.Context, net.Conn)) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logging.Error("unable to accept connection", zap.Error(err))
			continue
		}

		if !s.semaphore.TryAcquire() {
			s.rejectConnection(conn)
			continue
		}

		s.connectionsWg.Add(1)
		go func(conn net.Conn) {
			defer s.semaphore.Release()
			defer s.connectionsWg.Done()
			connectionHandler(ctx, conn)
		}(conn)
	}
}

func (s *TCPServer) rejectConnection(conn net.Conn) {
	_, err := conn.Write([]byte("ERROR: Connection limit exceeded, try again later"))
	if err != nil {
		logging.Warn("unable to send message about connection limit exceeded", zap.Error(err))
	}
	err = conn.Close()
	if err != nil {
		logging.Warn("error in conn.Close()", zap.Error(err))
	}
}

func (s *TCPServer) waitForShutdown(ctx context.Context) {
	<-ctx.Done()
	err := s.listener.Close()
	if err != nil {
		logging.Warn("error in listener.Close()", zap.Error(err))
	}
}

func (s *TCPServer) gracefulShutdown() {
	done := make(chan struct{})
	go func() {
		s.connectionsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logging.Info("All connections completed gracefully")
	case <-time.After(time.Second * time.Duration(s.cfg.GracefulShutdownTimeout)):
		logging.Warn("Shutdown timeout reached, some connections may have been forcefully closed")
	}
}

func (s *TCPServer) recoverAndCloseConnection(conn net.Conn, connectionType string) {
	if v := recover(); v != nil {
		logging.Error("captured panic in "+connectionType, zap.Any("panic", v))
	}

	if err := conn.Close(); err != nil {
		logging.Error("failed to close "+connectionType, zap.Error(err))
	}
}

func (s *TCPServer) shouldStopConnection(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (s *TCPServer) readMessage(conn net.Conn, buffer *[]byte) ([]byte, error) {
	err := conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.cfg.IdleTimeout)))
	if err != nil {
		logging.Warn("unable to set read deadline", zap.Error(err))
		return nil, err
	}

	count, err := conn.Read(*buffer)
	if err != nil {
		return nil, err
	}

	return (*buffer)[:count], nil
}

func (s *TCPServer) readInitialData(conn net.Conn, buffer *[]byte) ([]byte, error) {
	timeout := time.Second * time.Duration(s.cfg.IdleTimeout)
	data, err := ReadFramedMessageInPlace(conn, s.cfg.MaxMessageSize, timeout, *buffer)
	if err != nil {
		if s.isConnectionClosed(err) {
			return nil, err
		}
		logging.Warn("unable to read initial data from stream connection", zap.Error(err))
		return nil, err
	}

	return data, nil
}

func (s *TCPServer) isConnectionClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF)
}

func (s *TCPServer) validateMessageSize(messageSize int, remoteAddr string) bool {
	if messageSize > s.cfg.MaxMessageSize {
		logging.Warn("message size exceeds limit",
			zap.Int("message_size", messageSize),
			zap.Int("max_size", s.cfg.MaxMessageSize),
			zap.String("remote_addr", remoteAddr))
		return false
	}
	return true
}

func (s *TCPServer) processDataStream(ctx context.Context, conn net.Conn, handler StreamHandler, initialData []byte) {
	dataIterator := handler(ctx, initialData)

	for data, iterErr := range dataIterator {
		if s.shouldStopConnection(ctx) {
			logging.Info("stream connection context cancelled",
				zap.String("address", conn.RemoteAddr().String()))
			return
		}

		if iterErr != nil {
			s.handleStreamError(conn, iterErr)
			return
		}

		if len(data) == 0 {
			continue // Skip empty data chunks
		}

		if err := s.writeFramedWithDeadline(conn, data); err != nil {
			logging.Warn("unable to write stream data to connection",
				zap.String("address", conn.RemoteAddr().String()),
				zap.Error(err))
			return
		}

		logging.Info("sent stream data chunk",
			zap.String("address", conn.RemoteAddr().String()),
			zap.Int("bytes", len(data)))
	}

	logging.Info("stream connection completed",
		zap.String("address", conn.RemoteAddr().String()))
}

func (s *TCPServer) handleStreamError(conn net.Conn, iterErr error) {
	logging.Error("error from stream iterator",
		zap.Error(iterErr),
		zap.String("address", conn.RemoteAddr().String()))

	errorMsg := fmt.Sprintf("ERROR: %s", iterErr.Error())
	if err := s.writeFramedWithDeadline(conn, []byte(errorMsg)); err != nil {
		logging.Warn("unable to send error message to client", zap.Error(err))
	}
}

func (s *TCPServer) writeWithDeadline(conn net.Conn, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(s.cfg.IdleTimeout))); err != nil {
		return fmt.Errorf("unable to set write deadline: %w", err)
	}

	_, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("unable to write data: %w", err)
	}

	return nil
}

// writeFramedWithDeadline writes data with a length prefix for proper message framing
func (s *TCPServer) writeFramedWithDeadline(conn net.Conn, data []byte) error {
	timeout := time.Second * time.Duration(s.cfg.IdleTimeout)
	return WriteFramedMessage(conn, data, timeout)
}
