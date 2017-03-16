package dbsampler

import (
	"fmt"
	"github.com/howeyc/gopass"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	DriverMySQL = "mysql"
)

// ConnectionArgs...
type ConnectionArgs struct {
	Driver   string
	Name     string
	Host     string
	Port     string
	User     string
	Pass     string
	Protocol string
}

func (c *ConnectionArgs) dsn() string {
	return fmt.Sprintf(
		"%s:%s@%s(%s:%s)/%s",
		c.User,
		c.Pass,
		c.Protocol,
		c.Host,
		c.Port,
		c.Name,
	)
}

// DumpArgs...
type DumpArgs struct {
	Limit              int
	Routines           bool
	Triggers           bool
	SkipCreateDatabase bool
	SkipLockTables     bool
	SkipAddDropTable   bool
	ExtendedInsert     bool
}

// ParseFlags parses the command line flags.
func ParseFlags() (*ConnectionArgs, *DumpArgs, error) {
	conn := &ConnectionArgs{
		Driver: DriverMySQL,
	}
	args := &DumpArgs{}

	kingpin.Version(Version)
	kingpin.Flag("host", "The database host.").Default("127.0.0.1").Short('h').StringVar(&conn.Host)
	kingpin.Flag("port", "The database port.").Default("3306").Short('P').StringVar(&conn.Port)
	kingpin.Flag("protocol", "The protocol to use for the connection (tcp, socket, pip, memory).").Default("tcp").StringVar(&conn.Protocol)
	kingpin.Flag("user", "The database user.").Default("").Short('u').StringVar(&conn.User)
	kingpin.Flag("pass", "The database password.").Default("").Short('p').StringVar(&conn.Pass)
	prompt := kingpin.Flag("prompt", "Prompt for the database password.").Bool()

	kingpin.Flag("routines", "Dump procedures and functions.").BoolVar(&args.Routines)
	kingpin.Flag("triggers", "Dump triggers.").BoolVar(&args.Triggers)
	kingpin.Flag("limit", "Max number of rows from each table to dump.").Default("100").Short('l').IntVar(&args.Limit)
	kingpin.Flag("skip-create-database", "Disable adding CREATE DATABASE statement.").BoolVar(&args.SkipCreateDatabase)
	kingpin.Flag("skip-lock-tables", "Disable locking tables on read.").BoolVar(&args.SkipLockTables)
	kingpin.Flag("skip-add-drop-table", "Disable adding DROP TABLE statements.").BoolVar(&args.SkipAddDropTable)
	kingpin.Flag("extended-insert", "Use multiple-row INSERT syntax that include several VALUES lists.").BoolVar(&args.ExtendedInsert)
	kingpin.Arg("database", "Name of the database to dump.").Required().StringVar(&conn.Name)
	kingpin.Parse()

	if *prompt {
		fmt.Print("Enter password: ")
		pass, _ := gopass.GetPasswd()
		conn.Pass = string(pass)
	}

	return conn, args, nil
}
