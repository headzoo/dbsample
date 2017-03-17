package dbsampler

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"os"
	"strings"
)

// resolveTableGraph resolves table dependencies.
func resolveTableGraph(graph TableGraph) (TableGraph, error) {
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

	var resolved TableGraph
	for len(tableDeps) != 0 {
		readySet := mapset.NewSet()
		for tableName, deps := range tableDeps {
			if deps.Cardinality() == 0 {
				readySet.Add(tableName)
			}
		}
		if readySet.Cardinality() == 0 {
			s := []string{}
			for tableName, deps := range tableDeps {
				f := []string{}
				for d := range deps.Iter() {
					f = append(f, d.(string))
				}
				s = append(s, fmt.Sprintf("`%s` = %s", tableName, strings.Join(f, ", ")))
			}
			return resolved, fmt.Errorf("Circular dependency found -> %s", strings.Join(s, ", "))
		}
		for tableName := range readySet.Iter() {
			delete(tableDeps, tableName.(string))
			resolved = append(resolved, tableNames[tableName.(string)])
		}
		for tableName, deps := range tableDeps {
			diff := deps.Difference(readySet)
			tableDeps[tableName] = diff
		}
	}

	return resolved, nil
}

type resolveTableRowsFunc func(table *Table, cond map[string]mapset.Set) (Rows, error)

// resolveTableRows...
func resolveTableRows(tables TableGraph, fn resolveTableRowsFunc) (err error) {
	depRows := make(map[string]map[string]mapset.Set)
	skipTables := make(map[string]bool)
	for _, table := range tables {
		skip := false
		if _, ok := skipTables[table.Name]; ok {
			continue
		}
		for _, dep := range table.Dependencies {
			if _, ok := skipTables[dep.TableName]; ok {
				skip = true
			}
		}
		if skip {
			continue
		}

		cond := map[string]mapset.Set{}
		if _, ok := depRows[table.Name]; ok {
			cond = depRows[table.Name]
		}
		if table.Rows, err = fn(table, cond); err != nil {
			return
		}

		// Should we save these rows because another table depends on them?
		for _, t := range tables {
			for _, dep := range t.Dependencies {
				if dep.TableName == table.Name {
					if len(table.Rows) == 0 {
						warning("Skipping `%s`, references empty table `%s`.", t.Name, dep.TableName)
						skipTables[t.Name] = true
						continue
					}
					if _, ok := depRows[t.Name]; !ok {
						depRows[t.Name] = make(map[string]mapset.Set)
					}
					if _, ok := depRows[t.Name][dep.ReferencedColumnName]; !ok {
						depRows[t.Name][dep.ReferencedColumnName] = mapset.NewSet()
					}
					for _, row := range table.Rows {
						for _, field := range row {
							if field.Column == dep.ColumnName {
								depRows[t.Name][dep.ReferencedColumnName].Add(field.Value)
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
		displayDependencies(table.Dependencies, 1)
		fmt.Fprint(os.Stderr, "\n")
	}
}

// displayDependencies...
func displayDependencies(deps []*Dependency, indent int) {
	tabs := strings.Repeat("--", indent)
	for _, dep := range deps {
		fmt.Fprintf(os.Stderr, "%s %d. %s\n", tabs, indent, dep.TableName)
		if len(displayTables[dep.TableName].Dependencies) > 0 {
			indent++
			displayDependencies(displayTables[dep.TableName].Dependencies, indent)
			indent--
		}
	}
}
