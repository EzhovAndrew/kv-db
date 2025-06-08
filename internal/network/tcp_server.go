package network

import (
	"context"
	"errors"
	"fmt"
	"io"
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
				_, err := conn.Write([]byte("ERROR: Connection limit exceeded, try again later"))
				if err != nil {
					logging.Error("unable to send message about connection limit exceeded", zap.Error(err))
				}
				err = conn.Close()
				if err != nil {
					logging.Error("error in conn.Close()", zap.Error(err))
				}
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
	err := s.listener.Close()
	if err != nil {
		logging.Warn("error in listener.Close()", zap.Error(err))
	}
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

	buffer := s.bufferPool.Get().(*[]byte)
	defer s.bufferPool.Put(buffer)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(s.cfg.IdleTimeout)))
			if err != nil {
				logging.Warn("unable to set read deadline", zap.Error(err))
				return
			}
		}
		count, err := conn.Read(*buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return
			}
			logging.Warn("unable to read from connection", zap.Error(err))
			return
		}
		if count == s.cfg.MaxMessageSize+1 {
			logging.Warn("message size exceeds limit")
			return
		}

		response := handler(ctx, (*buffer)[:count])
		if err = conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(s.cfg.IdleTimeout))); err != nil {
			logging.Warn("unable to set write deadline", zap.Error(err))
			return
		}
		if _, err = conn.Write(response); err != nil {
			logging.Warn(
				"unable to write to connection",
				zap.String("address", conn.RemoteAddr().String()), zap.Error(err),
			)
			return
		}
	}
}
