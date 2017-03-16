package dbsampler

// Database queries a database.
type Database interface {
	Name() string
	SetName(string)
	CharSet() string
	Collation() string
	CreateSQL() (string, error)
	SetCreateSQL(string)
	Tables() (TableGraph, error)
	Views() (ViewGraph, error)
	Routines() (RoutineGraph, error)
	Server() *Server
}
