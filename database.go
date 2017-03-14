package dbsampler

type Database interface {
	Server() *Server
	Name() string
	CharSet() string
	Collation() string
	Tables(limit int) (TableGraph, error)
	Generator() (Generator, error)
}
