package dbsample

import (
	"fmt"
	"io"
)

// Dumper...
type Dumper interface {
	Dump(w io.Writer, db Database) error
}

// NewDumper returns a Dumper instance.
func NewDumper(s *Server) (Dumper, error) {
	switch s.conn.Driver {
	case DriverMySQL:
		switch s.major {
		case "5":
			return NewMySQL5Dumper(s.args), nil
		}
	}
	return nil, fmt.Errorf("Dumper not available for %s %s", s.conn.Driver, s.major)
}
