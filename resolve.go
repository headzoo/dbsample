package dbsampler

import (
	"github.com/deckarep/golang-set"
	"errors"
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

		// If there aren't any ready nodes, then we have a circular dependency
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