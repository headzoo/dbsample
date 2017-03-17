package dbsampler

import (
	"fmt"
	"github.com/howeyc/gopass"
	"gopkg.in/alecthomas/kingpin.v2"
	"strings"
	"os"
	"os/user"
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
	RenameDatabase     string
	SkipCreateDatabase bool
	SkipLockTables     bool
	SkipAddDropTable   bool
	ExtendedInsert     bool
	Dependencies       map[string][]*Dependency
}

// ParseFlags parses the command line flags.
func ParseFlags() (*ConnectionArgs, *DumpArgs, error) {
	conn := &ConnectionArgs{
		Driver: DriverMySQL,
	}
	args := &DumpArgs{
		Dependencies: map[string][]*Dependency{},
	}
	
	for i, a := range os.Args {
		if a == "-p" || a == "--password" {
			os.Args[i] = "--password=\000"
		}
	}
	
	kingpin.Version(Version)
	kingpin.Flag("host", "The database host.").Default("127.0.0.1").Short('h').StringVar(&conn.Host)
	kingpin.Flag("port", "The database port.").Default("3306").Short('P').StringVar(&conn.Port)
	kingpin.Flag("protocol", "The protocol to use for the connection (tcp, socket, pip, memory).").Default("tcp").StringVar(&conn.Protocol)
	kingpin.Flag("user", "User for login if not current user.").Short('u').StringVar(&conn.User)
	kingpin.Flag("password", "Password to use when connecting to server. If password is not given it's asked from stderr.").Short('p').StringVar(&conn.Pass)

	kingpin.Flag("debug", "").Hidden().BoolVar(&IsDebugging)
	kingpin.Flag("routines", "Dump procedures and functions.").BoolVar(&args.Routines)
	kingpin.Flag("triggers", "Dump triggers.").BoolVar(&args.Triggers)
	kingpin.Flag("limit", "Max number of rows from each table to dump.").Default("100").Short('l').IntVar(&args.Limit)
	kingpin.Flag("skip-create-database", "Disable adding CREATE DATABASE statement.").BoolVar(&args.SkipCreateDatabase)
	kingpin.Flag("skip-lock-tables", "Disable locking tables on read.").BoolVar(&args.SkipLockTables)
	kingpin.Flag("skip-add-drop-table", "Disable adding DROP TABLE statements.").BoolVar(&args.SkipAddDropTable)
	kingpin.Flag("extended-insert", "Use multiple-row INSERT syntax that include several VALUES lists.").BoolVar(&args.ExtendedInsert)
	kingpin.Flag("rename-database", "Use this database name in the dump.").PlaceHolder("DUMP-NAME").StringVar(&args.RenameDatabase)
	fks := kingpin.Flag("foreign-key", "Assigns one or more mock foreign keys.").Short('f').Strings()
	kingpin.Arg("database", "Name of the database to dump.").Required().StringVar(&conn.Name)
	kingpin.Parse()
	
	if conn.User == "" {
		u, err := user.Current()
		if err != nil {
			return nil, nil, err
		}
		conn.User = u.Username
	}
	if conn.Pass == "\000" {
		pass, _ := gopass.GetPasswdPrompt("Enter password: ", false, os.Stdin, os.Stderr)
		conn.Pass = string(pass)
	} else {
		warning("Warning: Using a password on the command line interface can be insecure.")
	}
	
	for _, fk := range *fks {
		halves := strings.Split(fk, " ")
		if len(halves) != 2 {
			return nil, nil, fmt.Errorf("Invalid foreign key specification %s", fk)
		}
		parts := strings.Split(halves[0], ".")
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("Invalid foreign key specification %s", fk)
		}
		table := parts[0]
		refCol := parts[1]
		parts = strings.Split(halves[1], ".")
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("Invalid foreign key specification %s", fk)
		}
		tblName := parts[0]
		colName := parts[1]
		
		if _, ok := args.Dependencies[table]; !ok {
			args.Dependencies[table] = []*Dependency{}
		}
		args.Dependencies[table] = append(args.Dependencies[table], &Dependency{
			TableName: tblName,
			ColumnName: colName,
			ReferencedColumnName: refCol,
		})
	}

	return conn, args, nil
}
