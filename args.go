package dbsample

import (
	"bytes"
	"fmt"
	"github.com/howeyc/gopass"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/user"
	"regexp"
	"strings"
	"text/template"
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
	Limit            int
	Routines         bool
	Triggers         bool
	RenameDatabase   string
	NoCreateDatabase bool
	SkipLockTables   bool
	SkipAddDropTable bool
	ExtendedInsert   bool
	Filters          []string
	Constraints      map[string][]*Constraint
}

// ParseFlags parses the command line flags.
func ParseFlags() (*ConnectionArgs, *DumpArgs, error) {
	if err := argsSetupUsageTemplate(); err != nil {
		return nil, nil, err
	}
	conn := &ConnectionArgs{
		Driver: DriverMySQL,
	}
	args := &DumpArgs{
		Constraints: map[string][]*Constraint{},
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
	kingpin.Flag("no-create-database", "Disable adding CREATE DATABASE statement.").Short('n').BoolVar(&args.NoCreateDatabase)
	kingpin.Flag("skip-lock-tables", "Disable locking tables on read.").BoolVar(&args.SkipLockTables)
	kingpin.Flag("skip-add-drop-table", "Disable adding DROP TABLE statements.").BoolVar(&args.SkipAddDropTable)
	kingpin.Flag("extended-insert", "Use multiple-row INSERT syntax that include several VALUES lists.").BoolVar(&args.ExtendedInsert)
	kingpin.Flag("rename-database", "Use this database name in the dump.").PlaceHolder("DUMP-NAME").StringVar(&args.RenameDatabase)
	fks := kingpin.Flag("constraint", "Assigns one or more foreign key constraints.").Short('c').Strings()
	kingpin.Flag("filter", "Apply a filter to the output.").Short('f').StringsVar(&args.Filters)
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

	r := regexp.MustCompile(`([\w]+)\.([\w]+)\s+([\w]+)\.([\w]+)`)
	for _, fk := range *fks {
		m := r.FindStringSubmatch(fk)
		if len(m) != 5 {
			return nil, nil, fmt.Errorf(`Invalid constraint "%s". Must be "table.column references.column"`, fk)
		}
		if _, ok := args.Constraints[m[1]]; !ok {
			args.Constraints[m[1]] = []*Constraint{}
		}
		args.Constraints[m[1]] = append(args.Constraints[m[1]], &Constraint{
			TableName:            m[3],
			ColumnName:           m[4],
			ReferencedColumnName: m[2],
		})
	}
	if err := Filters.SetCommands(args.Filters); err != nil {
		return nil, nil, err
	}

	return conn, args, nil
}

// argsSetupUsage...
func argsSetupUsageTemplate() error {
	t, err := template.New("dbsample").Parse(argsUsageDBSample)
	if err != nil {
		return err
	}

	ctx := argsUsageTemplateContext{
		FilterUsages: []string{},
	}
	for _, f := range Filters.Usage() {
		ctx.FilterUsages = append(ctx.FilterUsages, fmt.Sprintf(`--filter="%s"`, f))
	}

	buff := &bytes.Buffer{}
	if err := t.Execute(buff, ctx); err != nil {
		return err
	}
	usage := strings.Replace(argsUsageTemplate, "{{dbsample}}", buff.String(), 1)
	kingpin.UsageTemplate(usage)
	return nil
}

type argsUsageTemplateContext struct {
	FilterUsages []string
}

var argsUsageTemplate = `{{define "FormatCommand"}}\
{{if .FlagSummary}} {{.FlagSummary}}{{end}}\
{{range .Args}} {{if not .Required}}[{{end}}<{{.Name}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end}}\
{{end}}\

{{define "FormatCommands"}}\
{{range .FlattenedCommands}}\
{{if not .Hidden}}\
  {{.FullCommand}}{{if .Default}}*{{end}}{{template "FormatCommand" .}}
{{.Help|Wrap 4}}
{{end}}\
{{end}}\
{{end}}\

{{define "FormatUsage"}}\
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end}}
{{if .Help}}
{{.Help|Wrap 0}}\
{{end}}\

{{end}}\

{{if .Context.SelectedCommand}}\
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{else}}\
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end}}\
{{if .Context.Flags}}\
Flags:
{{.Context.Flags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.Args}}\
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.SelectedCommand}}\
{{if len .Context.SelectedCommand.Commands}}\
Subcommands:
{{template "FormatCommands" .Context.SelectedCommand}}
{{end}}\
{{else if .App.Commands}}\
Commands:
{{template "FormatCommands" .App}}
{{end}}\

{{dbsample}}
`

var argsUsageDBSample = `
Filters:
Filters alter column values in the dump. For example they can remove passwords or
other sensitive information. Each --filter flag should be passed the name of the
filter, e.g. "empty", the name of a table.column, e.g. "users.passwords", and one
or more arguments.

{{range .FilterUsages}}  {{.}}
{{end}}

Examples:
dbsample --limit=100 blog > dump.sql
dbsample --limit=100 -h db1 -u admin -p blog > dump.sql
dbsample --limit=100 --rename-database=blog_dev blog > dump.sql
dbsample --limit=100 --filter="empty users.password" --filter="repeat users.email X" blog > dump.sql
dbsample --limit=100 -c "posts.user_id users.id" -c "posts.cat_id categories.id" blog > dump.sql
`
