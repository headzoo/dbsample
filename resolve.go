package dbsampler

import (
	"github.com/deckarep/golang-set"
	"fmt"
	"os"
	"strings"
)

type queryConditionsFunc func(*Table, map[string][]string) (Rows, error)

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
			for tableName := range tableDeps {
				s = append(s, "`"+tableName+"`")
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

// resolveTableConditions...
func resolveTableConditions(tg TableGraph, fn queryConditionsFunc) error {
	tableResults := map[string]Rows{}
	for _, table := range tg {
		conditions := map[string][]string{}
		if len(table.Dependencies) > 0 {
			for _, dep := range table.Dependencies {
				refResults := tableResults[dep.TableName]
				values := []string{}
				for _, row := range refResults {
					for _, field := range row {
						if field.Column == dep.ColumnName {
							values = append(values, field.Value)
						}
					}
				}
				conditions[dep.ReferencedColumnName] = values
			}
		}
		
		res, err := fn(table, conditions)
		if err != nil {
			return err
		}
		tableResults[table.Name] = res
	}
	
	return nil
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