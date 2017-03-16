package dbsampler

import (
	"os"
	"strings"
	"github.com/davecgh/go-spew/spew"
)

const (
	Name    = "DBSampler"
	Version = "0.1"
)

var IsDebugBuild = false
var IsDebugging = false

// init...
func init() {
	if strings.Contains(os.Args[0], "go-build") {
		IsDebugBuild = true
	}
}

// dump...
func dump(v ...interface{}) {
	spew.Fdump(os.Stderr, v...)
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
