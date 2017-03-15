package dbsampler

type (
	Row          map[string]string
	Rows         []Row
	TableGraph   []*Table
	TriggerGraph []*Trigger
	RoutineGraph []*Routine
	ViewGraph    []*View
)

// Trigger...
type Trigger struct {
	Name      string
	CreateSQL string
	SQLMode   string
	CharSet   string
	Collation string
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
	CharSet         string
	Collation       string
}

// View...
type View struct {
	Name      string
	CreateSQL string
	CharSet   string
	Collation string
}

// Dependency...
type Dependency struct {
	TableName            string
	ColumnName           string
	ReferencedColumnName string
}

// Table stores the details of a single database table.
type Table struct {
	Name         string
	CreateSQL    string
	CharSet      string
	Collation    string
	Dependencies []*Dependency
	Triggers     TriggerGraph
	Rows         Rows
}
