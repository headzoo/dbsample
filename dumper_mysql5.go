package dbsample

import (
	"bytes"
	"fmt"
	"github.com/headzoo/dbsample/templates"
	"io"
	"io/ioutil"
	"path"
	"regexp"
	"strings"
	"text/template"
	"time"
)

const MySQL5DumperTemplatesPath = "./templates/mysql"

var dbname = regexp.MustCompile("CREATE DATABASE `[^`]+`")

// MySQL5DumperTemplateValues...
type MySQL5DumperTemplateValues struct {
	ShouldDumpDatabase   bool
	ShouldDumpTables     bool
	ShouldDumpViews      bool
	ShouldDumpRoutines   bool
	ShouldDumpTriggers   bool
	Debug                bool
	AppName              string
	AppVersion           string
	DumpDate             string
	DumpDuration         string
	CharSet              string
	Collation            string
	OriginalDatabaseName string
	Database             Database
	Args                 *DumpArgs
	Connection           *ConnectionArgs
	Server               *Server
	Tables               TableGraph
	Views                ViewGraph
	Routines             RoutineGraph
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
	start := time.Now()
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

	origDatabaseName := db.Name()
	if g.args.RenameDatabase != "" {
		sql, err := db.CreateSQL()
		if err != nil {
			return err
		}
		sql = dbname.ReplaceAllString(
			sql,
			fmt.Sprintf("CREATE DATABASE `%s`", g.args.RenameDatabase),
		)
		db.SetCreateSQL(sql)
		db.SetName(g.args.RenameDatabase)
	}

	if err := g.parseTemplates(); err != nil {
		return err
	}
	vals := MySQL5DumperTemplateValues{
		ShouldDumpDatabase:   !g.args.NoCreateDatabase,
		ShouldDumpTables:     true,
		ShouldDumpViews:      false,
		ShouldDumpRoutines:   g.args.Routines,
		ShouldDumpTriggers:   g.args.Triggers,
		Debug:                IsDebugging,
		AppName:              Name,
		AppVersion:           Version,
		Database:             db,
		OriginalDatabaseName: origDatabaseName,
		Args:                 g.args,
		CharSet:              db.CharSet(),
		Collation:            db.Collation(),
		DumpDate:             time.Now().Format("2006-01-02 15:04:05"),
		DumpDuration:         fmt.Sprintf("%s", time.Since(start)),
		Connection:           db.Server().conn,
		Server:               db.Server(),
		Tables:               tables,
		Views:                views,
		Routines:             routines,
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

	var err error
	var sql []byte
	if IsDebugBuild {
		files, err := ioutil.ReadDir(MySQL5DumperTemplatesPath)
		if err != nil {
			return err
		}
		for _, file := range files {
			filename := path.Join(MySQL5DumperTemplatesPath, file.Name())
			sql, err = ioutil.ReadFile(filename)
			g.templates, err = g.templates.New(filename).Parse(string(sql))
			if err != nil {
				return err
			}
		}
	} else {
		for _, file := range templates.FileNames {
			sql, err = templates.ReadFile(file)
			g.templates, err = g.templates.New(file).Parse(string(sql))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// tableInserts...
func (g *MySQL5Dumper) tableInserts(table *Table) string {
	if len(table.Rows) == 0 {
		return ""
	}

	types := []string{}
	cols := []string{}
	for _, row := range table.Rows[0] {
		cols = append(cols, row.Column)
		types = append(types, table.Columns[row.Column].DataType)
	}
	columns := MySQL5JoinColumns(cols)
	sep := ""
	if IsDebugging {
		sep = "\n"
	}
	if g.args.ExtendedInsert {
		inserts := []string{}
		for _, row := range table.Rows {
			vals := []string{}
			for _, v := range row {
				vals = append(vals, v.Value)
			}
			inserts = append(inserts, fmt.Sprintf(
				"INSERT INTO `%s` (%s)%sVALUES(%s);",
				table.Name,
				columns,
				sep,
				g.joinValues(vals, types),
			))
		}
		return strings.Join(inserts, "\n")
	} else {
		values := []string{}
		for _, row := range table.Rows {
			vals := []string{}
			for _, v := range row {
				vals = append(vals, v.Value)
			}
			values = append(values, fmt.Sprintf("(%s)", g.joinValues(vals, types)))
		}
		return fmt.Sprintf("INSERT INTO `%s` (%s)%sVALUES %s;\n", table.Name, columns, sep, strings.Join(values, ","))
	}
}

// joinValues...
func (g *MySQL5Dumper) joinValues(vals []string, types []string) string {
	for i, val := range vals {
		if strings.Contains(types[i], "int") {
			if val == "" {
				val = "0"
			}
			vals[i] = val
		} else {
			vals[i] = MySQL5Quote(val)
		}
	}
	return strings.Join(vals, ", ")
}
