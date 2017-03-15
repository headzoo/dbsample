package dbsampler

// Database queries a database.
type Database interface {
	Name() string
	CharSet() string
	Collation() string
	Tables() (TableGraph, error)
	Views() (ViewGraph, error)
	Routines() (RoutineGraph, error)
	Server() *Server
}
