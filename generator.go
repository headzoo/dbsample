package dbsampler

// Generator...
type Generator interface {
	GenerateSQL(db Database, tables TableGraph) (string, error)
	SetDelimiter(d string)
}
