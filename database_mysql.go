package dbsampler

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"github.com/deckarep/golang-set"
	"regexp"
	"strings"
	"bytes"
)

var air = regexp.MustCompile(`AUTO_INCREMENT=[\d]+ `)

// MySQLDatabase implements Database for MySQL.
type MySQLDatabase struct {
	name      string
	server    *Server
	charSet   string
	collation string
	createSQL string
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

// CreateSQL...
func (db *MySQLDatabase) CreateSQL() (string, error) {
	if db.createSQL == "" {
		rows, err := db.server.query("SHOW CREATE DATABASE %s", MySQLBacktick(db.name))
		if err != nil {
			return "", err
		}
		defer rows.Close()

		if !rows.Next() {
			return "", fmt.Errorf("SHOW CREATE DATABASE %s returned 0 rows", MySQLBacktick(db.name))
		}
		var a string
		if err = rows.Scan(&a, &db.createSQL); err != nil {
			return "", err
		}
	}
	return db.createSQL, nil
}

// Tables...
func (db *MySQLDatabase) Tables() (tables TableGraph, err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query(
		"SELECT `TABLE_NAME`, `TABLE_COLLATION` "+
			"FROM `INFORMATION_SCHEMA`.`TABLES` "+
			"WHERE `TABLE_SCHEMA` = %s "+
			"AND `TABLE_TYPE` = 'BASE TABLE'",
		MySQLQuote(db.name),
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
		MySQLQuote(db.name),
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
		MySQLQuote(db.name),
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
		MySQLQuote(db.name),
		MySQLQuote(table.Name),
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
		conditions := map[string][]string{}
		if len(table.Dependencies) > 0 {
			for _, dep := range table.Dependencies {
				refRows := tableRows[dep.TableName]
				values := []string{}
				for _, rows := range refRows {
					values = append(values, rows[dep.ColumnName])
				}
				conditions[dep.ReferencedColumnName] = values
			}
		}

		if !db.server.args.SkipLockTables {
			if err = db.server.exec("LOCK TABLES %s READ LOCAL", MySQLBacktick(table.Name)); err != nil {
				return
			}
		}
		var rows Rows
		rows, err = db.server.selectRows(db.buildSelectRowsSQL(table.Name, conditions))
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
		MySQLQuote(db.name+"%"),
		MySQLQuote(table.Name),
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
	if rows, err = db.server.query("SHOW CREATE TABLE %s", MySQLBacktick(table.Name)); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE TABLE %s returned 0 rows", MySQLBacktick(table.Name))
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
	if rows, err = db.server.query(
		"SELECT `VIEW_DEFINITION`, `DEFINER`, `SECURITY_TYPE`, `CHARACTER_SET_CLIENT`, `COLLATION_CONNECTION`"+
		"FROM `INFORMATION_SCHEMA`.`VIEWS` "+
		"WHERE `TABLE_SCHEMA` = %s "+
		"AND `TABLE_NAME` = %s "+
		"LIMIT 1",
		MySQLQuote(db.Name()),
		MySQLQuote(view.Name),
	); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE VIEW %s returned 0 rows", MySQLBacktick(view.Name))
		return
	}
	if err = rows.Scan(&view.CreateSQL, &view.Definer, &view.SecurityType, &view.CharSet, &view.Collation); err != nil {
		return
	}
	view.CreateSQL = fmt.Sprintf("VIEW %s AS %s", MySQLBacktick(view.Name), view.CreateSQL)
	view.Definer = MySQLBacktickUser(view.Definer)
	var cols ColumnGraph
	if cols, err = db.tableColumns(view.Name); err != nil {
		return
	}
	view.Columns = cols
	return
}

// setRoutineCreateSQL...
func (db *MySQLDatabase) setRoutineCreateSQL(r *Routine) (err error) {
	var rows *gosql.Rows
	rows, err = db.server.query(
		"SELECT `type`, `body_utf8`, `security_type`, `definer`, `param_list`, `returns`, `is_deterministic`, `sql_mode` "+
			"FROM `mysql`.`proc` "+
			"WHERE `name` = %s "+
			"AND `db` = %s "+
			"LIMIT 1",
		MySQLQuote(r.Name),
		MySQLQuote(db.name),
	)
	if err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE ROUTINE %s returned 0 rows", MySQLBacktick(r.Name))
		return
	}
	if err = rows.Scan(&r.Type, &r.CreateSQL, &r.SecurityType, &r.Definer, &r.ParamList, &r.Returns, &r.IsDeterministic, &r.SQLMode); err != nil {
		return
	}
	r.Definer = MySQLBacktickUser(r.Definer)
	return
}

// setTriggerCreateSQL...
func (db *MySQLDatabase) setTriggerCreateSQL(t *Trigger) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query("SHOW CREATE TRIGGER %s", MySQLBacktick(t.Name)); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE TRIGGER %s returned 0 rows", MySQLBacktick(t.Name))
		return
	}
	var a string
	var b string
	if err = rows.Scan(&a, &t.SQLMode, &t.CreateSQL, &t.CharSet, &t.Collation, &b); err != nil {
		return
	}
	return
}

// tableColumns...
func (db *MySQLDatabase) tableColumns(tableName string) (cols ColumnGraph, err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query(
		"SELECT `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_TYPE` "+
		"FROM `INFORMATION_SCHEMA`.`COLUMNS` "+
		"WHERE `TABLE_SCHEMA` = %s "+
		"AND `TABLE_NAME` = %s",
		MySQLQuote(db.Name()),
		MySQLQuote(tableName),
	); err != nil {
		return
	}
	defer rows.Close()
	
	cols = ColumnGraph{}
	for rows.Next() {
		col := &Column{}
		if err = rows.Scan(&col.Name, &col.OrdinalPosition, &col.Type); err != nil {
			return
		}
		cols = append(cols, col)
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

// buildSelectRowsSQL...
func (db *MySQLDatabase) buildSelectRowsSQL(tableName string, conditions map[string][]string) string {
	where := db.buildWhereIn(conditions)
	var limit string
	if db.server.args.Limit != 0 {
		limit = fmt.Sprintf("LIMIT %d", db.server.args.Limit)
	}
	return fmt.Sprintf(
		"SELECT * FROM %s %s %s",
		MySQLBacktick(tableName),
		where,
		limit,
	)
}

// buildWhereIn...
func (db *MySQLDatabase) buildWhereIn(conditions map[string][]string) string {
	values := []string{}
	for col, vals := range conditions {
		if len(vals) > 0 {
			cond := fmt.Sprintf("%s IN(%s)", MySQLBacktick(col), MySQLJoinValues(vals))
			values = append(values, cond)
		}
	}
	if len(values) == 0 {
		return ""
	}
	return fmt.Sprintf("WHERE %s", strings.Join(values, " AND "))
}

// MySQLEscape...
// @see https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
func MySQLEscape(val string) string {
	b := bytes.Buffer{}
	for _, c := range val {
		switch c {
		case '\000':
			b.WriteString(`\0`)
		case '\033':
			b.WriteString(`\Z`)
		case '\'':
			b.WriteString(`\'`)
		case '\b':
			b.WriteString(`\b`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(c)
		}
	}
 	return b.String()
}

// MySQLBacktick...
func MySQLBacktick(col string) string {
	return fmt.Sprintf("`%s`", col)
}

// MySQLBacktickUser...
func MySQLBacktickUser(user string) string {
	parts := strings.SplitN(user, "@", 2)
	return fmt.Sprintf("`%s`@`%s`", parts[0], parts[1])
}

// MySQLQuote...
func MySQLQuote(val string) string {
	return fmt.Sprintf("'%s'", MySQLEscape(val))
}

// MySQLJoinValues...
func MySQLJoinValues(vals []string) string {
	for i, val := range vals {
		vals[i] = MySQLQuote(val)
	}
	return strings.Join(vals, ", ")
}

// MySQLJoinColumns...
func MySQLJoinColumns(cols []string) string {
	for i, col := range cols {
		cols[i] = MySQLBacktick(col)
	}
	return strings.Join(cols, ", ")
}
