package dbsampler

import (
	"os"
	"strings"
	"github.com/davecgh/go-spew/spew"
	"fmt"
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

// warning...
func warning(str string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, str+"\n", v...)
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
	//db.Tables()
	//return nil
	dumper, err := NewDumper(server)
	if err != nil {
		return err
	}
	return dumper.Dump(os.Stdout, db)
}
