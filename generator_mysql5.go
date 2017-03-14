package dbsampler

import (
	"bytes"
	"fmt"
	"github.com/headzoo/dbsampler/templates"
	"io/ioutil"
	"path"
	"text/template"
	"time"
)

const MySQL5TemplatesPath = "./templates/mysql"

// MySQL5GeneratorTemplateTable...
type MySQL5GeneratorTemplateTable struct {
	*Table
	Inserts   []string
	CharSet   string
	Collation string
}

// MySQL5GeneratorTemplateView...
type MySQL5GeneratorTemplateView struct {
	*Table
	CharSet   string
	Collation string
}

// MySQL5GeneratorTemplateRoutine...
type MySQL5GeneratorTemplateRoutine struct {
	*Table
	CharSet   string
	Collation string
}

// MySQL5GeneratorTemplateTrigger...
type MySQL5GeneratorTemplateTrigger struct {
	*Table
	CharSet   string
	Collation string
}

// MySQL5GeneratorTemplateValues...
type MySQL5GeneratorTemplateValues struct {
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
	Args               *Args
	Connection         *Connection
	Server             *Server
	Tables             []MySQL5GeneratorTemplateTable
	Views              []MySQL5GeneratorTemplateView
	Routines           []MySQL5GeneratorTemplateRoutine
	Triggers           []MySQL5GeneratorTemplateTrigger
}

// MySQL5Generator...
type MySQL5Generator struct {
	Delimiter string
	db        Database
	args      *Args
	buff      bytes.Buffer
	templates *template.Template
}

// NewMySQL5Generator returns a new *MySQL5Generator instance.
func NewMySQL5Generator(args *Args) *MySQL5Generator {
	return &MySQL5Generator{
		Delimiter: sqlDelimiter,
		args:      args,
		templates: template.New(""),
	}
}

// SetDelimiter...
func (g *MySQL5Generator) SetDelimiter(d string) {
	g.Delimiter = d
}

// GenerateSQL...
func (g *MySQL5Generator) GenerateSQL(db Database, tables TableGraph) (string, error) {
	g.db = db
	if err := g.parseTemplates(); err != nil {
		return "", err
	}

	vals := MySQL5GeneratorTemplateValues{
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
		Tables:             g.createTemplateTables(tables),
		Views:              g.createTemplateViews(tables),
		Routines:           g.createTemplateRoutines(tables),
		Triggers:           g.createTemplateTriggers(tables),
	}

	b := &bytes.Buffer{}
	if err := g.templates.ExecuteTemplate(b, "templates/mysql/dump.sql.tmpl", vals); err != nil {
		return "", err
	}
	return b.String(), nil
}

// createTemplateTables...
func (g *MySQL5Generator) createTemplateTables(tables TableGraph) []MySQL5GeneratorTemplateTable {
	tmplTables := []MySQL5GeneratorTemplateTable{}
	for _, table := range tables {
		if table.Type == schemaTypeBaseTable {
			t := MySQL5GeneratorTemplateTable{
				Table:     table,
				Inserts:   []string{},
				CharSet:   g.db.CharSet(),
				Collation: g.db.Collation(),
			}
			for _, row := range table.Rows {
				vals := []string{}
				cols := []string{}
				for c, v := range row {
					vals = append(vals, v)
					cols = append(cols, c)
				}
				t.Inserts = append(t.Inserts, fmt.Sprintf(
					"INSERT INTO %s (%s) VALUES(%s)",
					backtick(t.Name),
					joinColumns(cols),
					joinValues(vals),
				))
			}
			tmplTables = append(tmplTables, t)
		}
	}

	return tmplTables
}

// createTemplateViews...
func (g *MySQL5Generator) createTemplateViews(tables TableGraph) []MySQL5GeneratorTemplateView {
	tmplTables := []MySQL5GeneratorTemplateView{}
	for _, table := range tables {
		if table.Type == schemaTypeView {
			t := MySQL5GeneratorTemplateView{
				Table:     table,
				CharSet:   g.db.CharSet(),
				Collation: g.db.Collation(),
			}
			tmplTables = append(tmplTables, t)
		}
	}

	return tmplTables
}

// createTemplateRoutines...
func (g *MySQL5Generator) createTemplateRoutines(tables TableGraph) []MySQL5GeneratorTemplateRoutine {
	tmplTables := []MySQL5GeneratorTemplateRoutine{}
	for _, table := range tables {
		if table.Type == schemaTypeProcedure || table.Type == schemaTypeFunction {
			t := MySQL5GeneratorTemplateRoutine{
				Table:     table,
				CharSet:   g.db.CharSet(),
				Collation: g.db.Collation(),
			}
			tmplTables = append(tmplTables, t)
		}
	}

	return tmplTables
}

// createTemplateTriggers...
func (g *MySQL5Generator) createTemplateTriggers(tables TableGraph) []MySQL5GeneratorTemplateTrigger {
	tmplTables := []MySQL5GeneratorTemplateTrigger{}
	for _, table := range tables {
		if table.Type == schemaTypeTrigger {
			t := MySQL5GeneratorTemplateTrigger{
				Table:     table,
				CharSet:   g.db.CharSet(),
				Collation: g.db.Collation(),
			}
			tmplTables = append(tmplTables, t)
		}
	}

	return tmplTables
}

// parseTemplates...
func (g *MySQL5Generator) parseTemplates() error {
	files, err := ioutil.ReadDir(MySQL5TemplatesPath)
	if err != nil {
		return err
	}
	for _, f := range files {
		g.parseTemplateFile(path.Join(MySQL5TemplatesPath, f.Name()))
	}
	return nil
}

// parseTemplate...
func (g *MySQL5Generator) parseTemplateFile(file string) error {
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
		println(err.Error())
		return err
	}
	return nil
}
