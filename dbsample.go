package dbsample

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/headzoo/dbsample/filters"
	"os"
	"strings"
)

const (
	Name    = "DBSample"
	Version = "0.1"
)

var IsDebugBuild = false
var IsDebugging = false
var Filters = filters.NewFilterController()

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
	if err := Filters.Load(); err != nil {
		return err
	}
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
