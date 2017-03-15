package dbsampler

import (
	"os"
	"strings"
)

const (
	Name    = "DBSampler"
	Version = "1.0"
)

var IsDebugBuild = false

// init...
func init() {
	if strings.Contains(os.Args[0], "go-build") {
		IsDebugBuild = true
	}
}

// Dump...
func Dump() error {
	conn, args, err := ParseFlags()
	if err != nil {
		return err
	}
	server := NewServer(conn, args)
	if err := server.Open(); err != nil {
		return err
	}
	defer server.Close()
	db, err := server.Database(conn.Name)
	if err != nil {
		return err
	}
	dumper, err := NewDumper(server)
	if err != nil {
		return err
	}
	return dumper.Dump(os.Stdout, db)
}
