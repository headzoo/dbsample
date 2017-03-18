package filters

import (
	"errors"
	"strings"
)

// RepeatFilter replaces column values with a repeating string.
type RepeatFilter struct {
}

// NewRepeatFilter returns a new *RepeatFilter instance.
func NewRepeatFilter() *RepeatFilter {
	return &RepeatFilter{}
}

// Filter...
func (f *RepeatFilter) Filter(value *string, dataType string, maxLength int64, args []string) error {
	*value = strings.Repeat(args[0], int(maxLength))
	return nil
}

// ValidateArgs...
func (f *RepeatFilter) ValidateArgs(args []string) error {
	if len(args) != 1 {
		return errors.New(`Filter "repeat" expects exactly 1 argument.`)
	}
	return nil
}

// Usage...
func (f *RepeatFilter) Usage() string {
	return `repeat users.password <string>`
}
