package dbsampler

import (
	"bytes"
	"fmt"
	"github.com/headzoo/dbsampler/templates"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"text/template"
	"time"
)

const MySQL5DumperTemplatesPath = "./templates/mysql"

// MySQL5DumperTemplateValues...
type MySQL5DumperTemplateValues struct {
	ShouldDumpTables   bool
	ShouldDumpViews    bool
	ShouldDumpRoutines bool
	ShouldDumpTriggers bool
	AppName            string
	AppVersion         string
	DumpDate           string
	CharSet            string
	Collation          string
	Database           Database
	Args               *DumpArgs
	Connection         *ConnectionArgs
	Server             *Server
	Tables             TableGraph
	Views              ViewGraph
	Routines           RoutineGraph
}

// MySQL5Dumper...
type MySQL5Dumper struct {
	args      *DumpArgs
	buff      bytes.Buffer
	templates *template.Template
}

// NewMySQL5Dumper returns a new *MySQL5Dumper instance.
func NewMySQL5Dumper(args *DumpArgs) *MySQL5Dumper {
	return &MySQL5Dumper{
		args:      args,
		templates: template.New(""),
	}
}

// Dump...
func (g *MySQL5Dumper) Dump(w io.Writer, db Database) error {
	tables, err := db.Tables()
	if err != nil {
		return err
	}
	views, err := db.Views()
	if err != nil {
		return err
	}
	routines, err := db.Routines()
	if err != nil {
		return err
	}

	if err := g.parseTemplates(); err != nil {
		return err
	}
	vals := MySQL5DumperTemplateValues{
		ShouldDumpTables:   true,
		ShouldDumpViews:    true,
		ShouldDumpRoutines: true,
		ShouldDumpTriggers: true,
		AppName:            Name,
		AppVersion:         Version,
		Database:           db,
		Args:               g.args,
		CharSet:            db.CharSet(),
		Collation:          db.Collation(),
		DumpDate:           time.Now().Format("2006-01-02 15:04:05"),
		Connection:         db.Server().conn,
		Server:             db.Server(),
		Tables:             tables,
		Views:              views,
		Routines:           routines,
	}
	if err := g.templates.ExecuteTemplate(w, "templates/mysql/dump.sql.tmpl", vals); err != nil {
		return err
	}
	return nil
}

// parseTemplates...
func (g *MySQL5Dumper) parseTemplates() error {
	g.templates.Funcs(template.FuncMap{
		"TableInserts": g.tableInserts,
	})

	files, err := ioutil.ReadDir(MySQL5DumperTemplatesPath)
	if err != nil {
		return err
	}
	for _, f := range files {
		g.parseTemplateFile(path.Join(MySQL5DumperTemplatesPath, f.Name()))
	}
	return nil
}

// parseTemplate...
func (g *MySQL5Dumper) parseTemplateFile(file string) error {
	var err error
	var sql []byte
	if IsDebugBuild {
		sql, err = ioutil.ReadFile(file)
	} else {
		sql, err = templates.ReadFile(file)
	}
	if err != nil {
		return err
	}
	g.templates, err = g.templates.New(file).Parse(string(sql))
	if err != nil {
		return err
	}
	return nil
}

// tableInserts...
func (g *MySQL5Dumper) tableInserts(table *Table) string {
	if len(table.Rows) == 0 {
		return ""
	}

	cols := []string{}
	for col, _ := range table.Rows[0] {
		cols = append(cols, col)
	}
	columns := MySQLJoinColumns(cols)

	if g.args.ExtendedInsert {
		inserts := []string{}
		for _, row := range table.Rows {
			vals := []string{}
			for _, v := range row {
				vals = append(vals, v)
			}
			inserts = append(inserts, fmt.Sprintf(
				"INSERT INTO `%s` (%s) VALUES(%s);",
				table.Name,
				columns,
				MySQLJoinValues(vals),
			))
		}
		return strings.Join(inserts, "\n")
	} else {
		values := []string{}
		for _, row := range table.Rows {
			vals := []string{}
			for _, v := range row {
				vals = append(vals, v)
			}
			values = append(values, fmt.Sprintf("(%s)", MySQLJoinValues(vals)))
		}
		return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s;\n", table.Name, columns, strings.Join(values, ","))
	}
}
