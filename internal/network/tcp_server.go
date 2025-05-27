package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type TCPHandler = func(ctx context.Context, data []byte) []byte

type TCPServer struct {
	listener      net.Listener
	semaphore     *concurrency.Semaphore
	connectionsWg sync.WaitGroup

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
	}, nil
}

func (s *TCPServer) HandleRequests(ctx context.Context, handler TCPHandler) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
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
				conn.Write([]byte("Connection limit exceeded, try again later"))
				conn.Close()
				continue
			}

			s.connectionsWg.Add(1)
			go func(conn net.Conn) {
				defer s.semaphore.Release()
				defer s.connectionsWg.Done()
				s.handleConnection(ctx, conn, handler)
			}(conn)
		}
	}()

	<-ctx.Done()
	s.listener.Close()
	wg.Wait()

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

func (s *TCPServer) handleConnection(ctx context.Context, conn net.Conn, handler TCPHandler) {
	defer func() {
		if v := recover(); v != nil {
			logging.Error("captured panic", zap.Any("panic", v))
		}

		if err := conn.Close(); err != nil {
			logging.Error("failed to close connection", zap.Error(err))
		}
	}()

	
}
