package main

import (
	"fmt"
	"github.com/headzoo/dbsampler"
	"github.com/howeyc/gopass"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"strings"
)

// main...
func main() {
	if strings.Contains(os.Args[0], "go-build") {
		dbsampler.IsDebugBuild = true
	}

	conn, args, err := parseFlags()
	handleError(err)

	server := dbsampler.NewServer(conn, args)
	handleError(server.Open())
	defer server.Close()

	db, err := server.Database(conn.Name)
	tables, err := db.Tables(args.Limit)
	handleError(err)

	gen, err := db.Generator()
	handleError(err)
	sql, err := gen.GenerateSQL(db, tables)
	handleError(err)
	stdout(sql)
}

// parseFlags parses the command line flags.
func parseFlags() (*dbsampler.Connection, *dbsampler.Args, error) {
	conn := &dbsampler.Connection{
		Driver: dbsampler.DriverMySQL,
	}
	args := &dbsampler.Args{}

	kingpin.Version(dbsampler.Version)
	kingpin.Flag("host", "The database host.").Default("127.0.0.1").Short('h').StringVar(&conn.Host)
	kingpin.Flag("port", "The database port.").Default("3306").StringVar(&conn.Port)
	kingpin.Flag("user", "The database user.").Default("").Short('u').StringVar(&conn.User)
	kingpin.Flag("pass", "The database password.").Default("").Short('p').StringVar(&conn.Pass)
	prompt := kingpin.Flag("prompt", "Prompt for the database password.").Bool()

	kingpin.Flag("routines", "Dump store procedures and functions.").BoolVar(&args.Routines)
	kingpin.Flag("triggers", "Dump triggers.").BoolVar(&args.Triggers)
	kingpin.Flag("limit", "Max number of rows from each table to dump.").Default("100").Short('l').IntVar(&args.Limit)
	kingpin.Flag("skip-lock-tables", "Skip lock tables.").BoolVar(&args.SkipLockTables)
	kingpin.Flag("skip-add-drop-table", "Disable adding DROP TABLE.").BoolVar(&args.SkipAddDropTable)
	kingpin.Arg("database", "Name of the database to dump.").Required().StringVar(&conn.Name)
	kingpin.Parse()

	if *prompt {
		fmt.Print("Enter password: ")
		pass, _ := gopass.GetPasswd()
		conn.Pass = string(pass)
	}

	return conn, args, nil
}

// stdout writes the given string to stdout.
func stdout(s string) {
	fmt.Fprintln(os.Stdout, s)
}

// stderr writes the given string to stderr.
func stderr(s string) {
	fmt.Fprintln(os.Stderr, s)
}

// handleError prints the error and exits if not nil.
func handleError(err error) {
	if err != nil {
		stderr(err.Error())
		os.Exit(1)
	}
}
