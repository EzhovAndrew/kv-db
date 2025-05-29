package compute

import (
	"errors"
	"strings"
)

type Compute struct{}

var (
	ErrEmptyCommand           = errors.New("empty command")
	ErrUnknownCommand         = errors.New("unknown command")
	ErrInvalidArgumentsNumber = errors.New("invalid arguments number")
)

func NewCompute() *Compute {
	return &Compute{}
}

func (c *Compute) Parse(cmd string) (Query, error) {
	splittedCmd := strings.Fields(cmd)
	if len(splittedCmd) == 0 {
		return Query{}, ErrEmptyCommand
	}
	commandID := commandNameToCommandID(splittedCmd[0])
	if commandID == UnknownCommandID {
		return Query{}, ErrUnknownCommand
	}
	arguments := splittedCmd[1:]
	if len(arguments) != commandArgumentsNumber(commandID) {
		return Query{}, ErrInvalidArgumentsNumber
	}
	return NewQuery(commandID, arguments), nil
}
