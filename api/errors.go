package api

import (
	"errors"
	"fmt"
)

// Common error variables for easy error checking
var (
	// ErrKeyNotFound is returned when a requested key doesn't exist in the database
	ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidArgument is returned when invalid arguments are provided
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrConnectionFailed is returned when connection to the server fails
	ErrConnectionFailed = errors.New("connection failed")

	// ErrNetworkError is returned when a network error occurs
	ErrNetworkError = errors.New("network error")

	// ErrServerError is returned when the server returns an error
	ErrServerError = errors.New("server error")
)

// KeyNotFoundError represents an error when a requested key is not found.
type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key '%s' not found", e.Key)
}

func (e *KeyNotFoundError) Is(target error) bool {
	return target == ErrKeyNotFound
}

func (e *KeyNotFoundError) Unwrap() error {
	return ErrKeyNotFound
}

// InvalidArgumentError represents an error when invalid arguments are provided.
type InvalidArgumentError struct {
	Message string
}

func (e *InvalidArgumentError) Error() string {
	return e.Message
}

func (e *InvalidArgumentError) Is(target error) bool {
	return target == ErrInvalidArgument
}

func (e *InvalidArgumentError) Unwrap() error {
	return ErrInvalidArgument
}

// ConnectionError represents an error when connecting to the server fails.
type ConnectionError struct {
	Host string
	Port int
	Err  error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("failed to connect to %s:%d: %v", e.Host, e.Port, e.Err)
}

func (e *ConnectionError) Is(target error) bool {
	return target == ErrConnectionFailed
}

func (e *ConnectionError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrConnectionFailed
}

// NetworkError represents a network-related error.
type NetworkError struct {
	Err error
}

func (e *NetworkError) Error() string {
	return fmt.Sprintf("network error: %v", e.Err)
}

func (e *NetworkError) Is(target error) bool {
	return target == ErrNetworkError
}

func (e *NetworkError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrNetworkError
}

// ServerError represents an error returned by the server.
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error: %s", e.Message)
}

func (e *ServerError) Is(target error) bool {
	return target == ErrServerError
}

func (e *ServerError) Unwrap() error {
	return ErrServerError
}
