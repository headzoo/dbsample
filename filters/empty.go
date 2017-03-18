package filters

import "errors"

// EmptyFilter returns an empty column value.
type EmptyFilter struct {
}

// NewEmptyFilter returns a new *EmptyFilter instance.
func NewEmptyFilter() *EmptyFilter {
	return &EmptyFilter{}
}

// Filter...
func (f *EmptyFilter) Filter(value *string, dataType string, maxLength int64, args []string) error {
	*value = ""
	return nil
}

// ValidateArgs...
func (f *EmptyFilter) ValidateArgs(args []string) error {
	if len(args) != 1 {
		return errors.New(`Filter "empty" expects exactly 0 arguments.`)
	}
	return nil
}

// Usage...
func (f *EmptyFilter) Usage() string {
	return `empty users.password`
}
