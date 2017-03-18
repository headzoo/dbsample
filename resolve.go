package dbsampler

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"os"
	"strings"
)

// resolveTableGraph resolves table constraints.
func resolveTableConstraints(graph TableGraph) (TableGraph, error) {
	tableNames := make(map[string]*Table)
	tableFKs := make(map[string]mapset.Set)
	for _, table := range graph {
		fkSet := mapset.NewSet()
		for _, fk := range table.Constraints {
			fkSet.Add(fk.TableName)
		}
		tableNames[table.Name] = table
		tableFKs[table.Name] = fkSet
	}

	var resolved TableGraph
	for len(tableFKs) != 0 {
		readySet := mapset.NewSet()
		for tableName, fks := range tableFKs {
			if fks.Cardinality() == 0 {
				readySet.Add(tableName)
			}
		}
		if readySet.Cardinality() == 0 {
			s := []string{}
			for tableName, fks := range tableFKs {
				f := []string{}
				for fk := range fks.Iter() {
					f = append(f, fk.(string))
				}
				s = append(s, fmt.Sprintf("`%s` = %s", tableName, strings.Join(f, ", ")))
			}
			return resolved, fmt.Errorf("Circular dependency found -> %s", strings.Join(s, ", "))
		}
		for tableName := range readySet.Iter() {
			delete(tableFKs, tableName.(string))
			resolved = append(resolved, tableNames[tableName.(string)])
		}
		for tableName, fks := range tableFKs {
			diff := fks.Difference(readySet)
			tableFKs[tableName] = diff
		}
	}

	return resolved, nil
}

type resolveTableRowsFunc func(table *Table, cond map[string]mapset.Set) (Rows, error)

// resolveTableRows...
func resolveTableRows(tables TableGraph, fn resolveTableRowsFunc) (err error) {
	fkRows := make(map[string]map[string]mapset.Set)
	skipTables := make(map[string]bool)
	for _, table := range tables {
		skip := false
		if _, ok := skipTables[table.Name]; ok {
			continue
		}
		for _, fk := range table.Constraints {
			if _, ok := skipTables[fk.TableName]; ok {
				skip = true
			}
		}
		if skip {
			continue
		}

		cond := map[string]mapset.Set{}
		if _, ok := fkRows[table.Name]; ok {
			cond = fkRows[table.Name]
		}
		if table.Rows, err = fn(table, cond); err != nil {
			return
		}

		// Should we save these rows because another table depends on them?
		for _, t := range tables {
			for _, fk := range t.Constraints {
				if fk.TableName == table.Name {
					if len(table.Rows) == 0 {
						warning("Skipping `%s`, references empty table `%s`.", t.Name, fk.TableName)
						skipTables[t.Name] = true
						continue
					}
					if _, ok := fkRows[t.Name]; !ok {
						fkRows[t.Name] = make(map[string]mapset.Set)
					}
					if _, ok := fkRows[t.Name][fk.ReferencedColumnName]; !ok {
						fkRows[t.Name][fk.ReferencedColumnName] = mapset.NewSet()
					}
					for _, row := range table.Rows {
						for _, field := range row {
							if field.Column == fk.ColumnName {
								fkRows[t.Name][fk.ReferencedColumnName].Add(field.Value)
							}
						}
					}
				}
			}
		}
	}

	return
}

var displayTables map[string]*Table

// displayGraph...
func displayGraph(graph TableGraph) {
	displayTables = map[string]*Table{}
	for _, table := range graph {
		displayTables[table.Name] = table
	}
	for _, table := range graph {
		fmt.Fprintf(os.Stderr, "%s\n", table.Name)
		displayConstraints(table.Constraints, 1)
		fmt.Fprint(os.Stderr, "\n")
	}
}

// displayConstraints...
func displayConstraints(fks []*Constraint, indent int) {
	tabs := strings.Repeat("--", indent)
	for _, fk := range fks {
		fmt.Fprintf(os.Stderr, "%s %d. %s\n", tabs, indent, fk.TableName)
		if len(displayTables[fk.TableName].Constraints) > 0 {
			indent++
			displayConstraints(displayTables[fk.TableName].Constraints, indent)
			indent--
		}
	}
}
