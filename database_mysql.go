package dbsampler

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"github.com/deckarep/golang-set"
	"regexp"
)

var air = regexp.MustCompile(`AUTO_INCREMENT=[\d]+ `)

// MySQLDatabase implements Database for MySQL.
type MySQLDatabase struct {
	name      string
	server    *Server
	charSet   string
	collation string
}

// NewMySQLDatabase returns a new *MySQLDatabase instance.
func NewMySQLDatabase(server *Server, name, charSet, collation string) *MySQLDatabase {
	return &MySQLDatabase{
		server:    server,
		name:      name,
		charSet:   charSet,
		collation: collation,
	}
}

// Server...
func (db *MySQLDatabase) Server() *Server {
	return db.server
}

// Name...
func (db *MySQLDatabase) Name() string {
	return db.name
}

// CharSet...
func (db *MySQLDatabase) CharSet() string {
	return db.charSet
}

// Collation...
func (db *MySQLDatabase) Collation() string {
	return db.collation
}

// Tables...
func (db *MySQLDatabase) Tables() (tables TableGraph, err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query(
		"SELECT `TABLE_NAME`, `TABLE_COLLATION` "+
			"FROM `INFORMATION_SCHEMA`.`TABLES` "+
			"WHERE `TABLE_SCHEMA` = %s "+
			"AND `TABLE_TYPE` = 'BASE TABLE'",
		quote(db.name),
	); err != nil {
		return
	}
	defer rows.Close()

	tables = make(TableGraph, 0)
	for rows.Next() {
		table := &Table{}
		if err = rows.Scan(&table.Name, &table.Collation); err != nil {
			return
		}
		if err = db.setTableDependencies(table); err != nil {
			return
		}
		if err = db.setTableCreateSQL(table); err != nil {
			return
		}
		table.CharSet = db.charSet
		tables = append(tables, table)
	}
	if err = db.setTableGraphRows(tables); err != nil {
		return
	}
	if tables, err = db.resolveTableGraph(tables); err != nil {
		return
	}
	if db.server.args.Triggers {
		for _, table := range tables {
			if err = db.setTableTriggers(table); err != nil {
				return
			}
		}
	}
	return
}

// Views...
func (db *MySQLDatabase) Views() (views ViewGraph, err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query(
		"SELECT `TABLE_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`TABLES` "+
			"WHERE `TABLE_SCHEMA` = %s "+
			"AND `TABLE_TYPE` = 'VIEW'",
		quote(db.name),
	); err != nil {
		return
	}
	defer rows.Close()

	views = make(ViewGraph, 0)
	for rows.Next() {
		view := &View{}
		if err = rows.Scan(&view.Name); err != nil {
			return
		}
		if err = db.setViewCreateSQL(view); err != nil {
			return
		}
		view.CharSet = db.charSet
		view.Collation = db.collation
		views = append(views, view)
	}
	return
}

// Routines...
func (db *MySQLDatabase) Routines() (routines RoutineGraph, err error) {
	if !db.server.args.Routines {
		return
	}

	var rows *gosql.Rows
	rows, err = db.server.query(
		"SELECT `name`, `type`, `character_set_client`, `collation_connection` FROM `mysql`.`proc` WHERE `db` = %s",
		quote(db.name),
	)
	if err != nil {
		return
	}
	defer rows.Close()

	routines = make(RoutineGraph, 0)
	for rows.Next() {
		routine := &Routine{}
		if err = rows.Scan(&routine.Name, &routine.Type, &routine.CharSet, &routine.Collation); err != nil {
			return
		}
		if err = db.setRoutineCreateSQL(routine); err != nil {
			return
		}
		routines = append(routines, routine)
	}
	return
}

// setTableDependencies...
func (db *MySQLDatabase) setTableDependencies(table *Table) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query(
		"SELECT `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`, `COLUMN_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` "+
			"WHERE `REFERENCED_TABLE_SCHEMA` = %s "+
			"AND `TABLE_NAME` = %s",
		quote(db.name),
		quote(table.Name),
	); err != nil {
		return
	}
	defer rows.Close()

	table.Dependencies = []*Dependency{}
	for rows.Next() {
		dep := &Dependency{}
		if err = rows.Scan(&dep.TableName, &dep.ColumnName, &dep.ReferencedColumnName); err != nil {
			return
		}
		table.Dependencies = append(table.Dependencies, dep)
	}
	return
}

// setTableGraphRows...
func (db *MySQLDatabase) setTableGraphRows(tg TableGraph) (err error) {
	defer func() {
		if !db.server.args.SkipLockTables {
			err = db.server.exec("UNLOCK TABLES")
		}
	}()

	tableRows := map[string]Rows{}
	for _, table := range tg {
		b := NewSelectBuilder(table)
		b.Limit = db.server.args.Limit
		if len(table.Dependencies) > 0 {
			for _, dep := range table.Dependencies {
				refRows := tableRows[dep.TableName]
				values := []string{}
				for _, rows := range refRows {
					values = append(values, rows[dep.ColumnName])
				}
				b.Conditions[dep.ReferencedColumnName] = values
			}
		}

		if !db.server.args.SkipLockTables {
			if err = db.server.exec("LOCK TABLES %s READ LOCAL", backtick(table.Name)); err != nil {
				return
			}
		}
		var rows Rows
		rows, err = db.server.selectRows(b.SQL())
		if err != nil {
			return
		}
		if !db.server.args.SkipLockTables {
			if err = db.server.exec("UNLOCK TABLES"); err != nil {
				return
			}
		}
		table.Rows = rows
		tableRows[table.Name] = rows
	}
	return
}

// setTableTriggers...
func (db *MySQLDatabase) setTableTriggers(table *Table) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query(
		"SELECT `TRIGGER_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`TRIGGERS` "+
			"WHERE `TRIGGER_SCHEMA` LIKE %s "+
			"AND `EVENT_OBJECT_TABLE` = %s",
		quote(db.name+"%"),
		quote(table.Name),
	); err != nil {
		return
	}
	defer rows.Close()

	table.Triggers = TriggerGraph{}
	for rows.Next() {
		trigger := &Trigger{}
		if err = rows.Scan(&trigger.Name); err != nil {
			return
		}
		if err = db.setTriggerCreateSQL(trigger); err != nil {
			return
		}
		table.Triggers = append(table.Triggers, trigger)
	}
	return
}

// showCreateTable...
func (db *MySQLDatabase) setTableCreateSQL(table *Table) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query("SHOW CREATE TABLE %s", backtick(table.Name)); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE TABLE %s returned 0 rows", backtick(table.Name))
		return
	}
	var a string
	if err = rows.Scan(&a, &table.CreateSQL); err != nil {
		return
	}
	table.CreateSQL = air.ReplaceAllString(table.CreateSQL, "")
	return
}

// setViewCreateSQL...
func (db *MySQLDatabase) setViewCreateSQL(view *View) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query("SHOW CREATE VIEW %s", backtick(view.Name)); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE VIEW %s returned 0 rows", backtick(view.Name))
		return
	}
	var a string
	var b string
	var c string
	if err = rows.Scan(&a, &view.CreateSQL, &b, &c); err != nil {
		return
	}
	return
}

// setRoutineCreateSQL...
func (db *MySQLDatabase) setRoutineCreateSQL(r *Routine) (err error) {
	var rows *gosql.Rows
	rows, err = db.server.query(
		"SELECT `type`, `body_utf8`, `security_type`, `definer`, `param_list`, `returns`, `is_deterministic` "+
			"FROM `mysql`.`proc` "+
			"WHERE `name` = %s "+
			"AND `db` = %s",
		quote(r.Name),
		quote(db.name),
	)
	if err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE ROUTINE %s returned 0 rows", backtick(r.Name))
		return
	}
	if err = rows.Scan(&r.Type, &r.CreateSQL, &r.SecurityType, &r.Definer, &r.ParamList, &r.Returns, &r.IsDeterministic); err != nil {
		return
	}
	return
}

// setTriggerCreateSQL...
func (db *MySQLDatabase) setTriggerCreateSQL(t *Trigger) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query("SHOW CREATE TRIGGER %s", backtick(t.Name)); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE TRIGGER %s returned 0 rows", backtick(t.Name))
		return
	}
	var a string
	var b string
	if err = rows.Scan(&a, &t.SQLMode, &t.CreateSQL, &t.CharSet, &t.Collation, &b); err != nil {
		return
	}
	return
}

// resolveTableGraph resolves table dependencies.
func (db *MySQLDatabase) resolveTableGraph(graph TableGraph) (TableGraph, error) {
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
