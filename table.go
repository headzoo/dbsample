package dbsampler

import "fmt"

type (
	Row          []Field
	Rows         []Row
	TableGraph   []*Table
	TriggerGraph []*Trigger
	RoutineGraph []*Routine
	ViewGraph    []*View
	ColumnMap    map[string]*Column
)

// Field...
type Field struct {
	Column string
	Value  string
}

// Column...
type Column struct {
	Name            string
	OrdinalPosition int
	Type            string
	DataType        string
}

// Trigger...
type Trigger struct {
	Name              string
	CreateSQL         string
	ActionTiming      string
	EventManipulation string
	EventObjectTable  string
	ActionOrientation string
	Definer           string
	SQLMode           string
	CharSet           string
	Collation         string
}

// Routine...
type Routine struct {
	Name            string
	Type            string
	CreateSQL       string
	SecurityType    string
	Definer         string
	ParamList       string
	Returns         string
	IsDeterministic string
	SQLMode         string
	CharSet         string
	Collation       string
}

// View...
type View struct {
	Name         string
	Columns      ColumnMap
	CreateSQL    string
	CharSet      string
	Collation    string
	SecurityType string
	Definer      string
}

// Table stores the details of a single database table.
type Table struct {
	Name         string
	CreateSQL    string
	CharSet      string
	Collation    string
	DebugMsgs    []string
	Columns      ColumnMap
	Constraints  []*Constraint
	Triggers     TriggerGraph
	Rows         Rows
}

// NewTable returns a new *Table instance.
func NewTable() *Table {
	return &Table{
		DebugMsgs:    []string{},
		Columns:      ColumnMap{},
		Constraints: []*Constraint{},
		Triggers:     TriggerGraph{},
		Rows:         Rows{},
	}
}

// AppendDebugMsg appends a message to the debug messages.
func (t *Table) AppendDebugMsg(s string, v ...interface{}) {
	if IsDebugging {
		t.DebugMsgs = append(t.DebugMsgs, fmt.Sprintf(s, v...))
	}
}

// Constraint...
type Constraint struct {
	TableName            string
	ColumnName           string
	ReferencedColumnName string
}
