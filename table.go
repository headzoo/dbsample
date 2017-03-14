package dbsampler

import (
	"errors"
	"github.com/deckarep/golang-set"
)

type Row map[string]string
type Rows []Row

// Dependency...
type Dependency struct {
	TableName            string
	ColumnName           string
	ReferencedColumnName string
}

// Schema...
type Schema struct {
	Name string
	Type string
}

// Table stores the details of a single database table.
type Table struct {
	Name         string
	Type         string
	CreateSQL    string
	Dependencies []Dependency
	Rows         Rows
}

// NewTable returns a new *Table instance.
func NewTable(name string, sql string, deps []Dependency) *Table {
	return &Table{
		Name:         name,
		Type:         schemaTypeBaseTable,
		CreateSQL:    sql,
		Dependencies: deps,
	}
}

// NewView returns a new *Table instance.
func NewView(name string, sql string) *Table {
	return &Table{
		Name:      name,
		Type:      schemaTypeView,
		CreateSQL: sql,
	}
}

// NewProcedure returns a new *Table instance.
func NewProcedure(name string, sql string) *Table {
	return &Table{
		Name:      name,
		Type:      schemaTypeProcedure,
		CreateSQL: sql,
	}
}

// NewFunction returns a new *Table instance.
func NewFunction(name string, sql string) *Table {
	return &Table{
		Name:      name,
		Type:      schemaTypeFunction,
		CreateSQL: sql,
	}
}

// NewTrigger returns a new *Table instance.
func NewTrigger(name string, sql string) *Table {
	return &Table{
		Name:      name,
		Type:      schemaTypeTrigger,
		CreateSQL: sql,
	}
}

// TableGraph is a slice of tables.
type TableGraph []*Table

// ResolveTableGraph resolves table dependencies.
func ResolveTableGraph(graph TableGraph) (TableGraph, error) {
	tableNames := make(map[string]*Table)
	tableDeps := make(map[string]mapset.Set)
	for _, table := range graph {
		depSet := mapset.NewSet()
		for _, dep := range table.Dependencies {
			depSet.Add(dep.TableName)
		}
		tableNames[table.Name] = table
		tableDeps[table.Name] = depSet
	}

	// Iteratively find and remove nodes from the graph which have no dependencies.
	// If at some point there are still nodes in the graph and we cannot find
	// nodes without dependencies, that means we have a circular dependency
	var resolved TableGraph
	for len(tableDeps) != 0 {
		// Get all nodes from the graph which have no dependencies
		readySet := mapset.NewSet()
		for tableName, deps := range tableDeps {
			if deps.Cardinality() == 0 {
				readySet.Add(tableName)
			}
		}

		// If there aren't any ready nodes, then we have a cicular dependency
		if readySet.Cardinality() == 0 {
			var g TableGraph
			for tableName := range tableDeps {
				g = append(g, tableNames[tableName])
			}
			return g, errors.New("Circular dependency found")
		}

		// Remove the ready nodes and add them to the resolved graph
		for tableName := range readySet.Iter() {
			delete(tableDeps, tableName.(string))
			resolved = append(resolved, tableNames[tableName.(string)])
		}

		// Also make sure to remove the ready nodes from the
		// remaining node dependencies as well
		for tableName, deps := range tableDeps {
			diff := deps.Difference(readySet)
			tableDeps[tableName] = diff
		}
	}

	return resolved, nil
}
