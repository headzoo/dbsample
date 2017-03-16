package dbsampler

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strings"
)

var mysql5RegexpAI = regexp.MustCompile(`AUTO_INCREMENT=[\d]+ `)
var mysql5Stmts *MySQL5PreparedStatements

// MySQL5PreparedStatements...
type MySQL5PreparedStatements struct {
	db    *gosql.DB
	stmts map[string]*gosql.Stmt
	err   error
}

// NewMySQL5PreparedStatements returns a new *MySQL5PreparedStatements instance.
func NewMySQL5PreparedStatements(db *gosql.DB) *MySQL5PreparedStatements {
	return &MySQL5PreparedStatements{
		db:    db,
		stmts: map[string]*gosql.Stmt{},
	}
}

// Prepare...
func (p *MySQL5PreparedStatements) Prepare(name, sql string) error {
	if p.err != nil {
		return fmt.Errorf("Cannot prepare when last error is not nil: %s", p.err.Error())
	}
	if _, ok := p.stmts[name]; !ok {
		stmt, err := p.db.Prepare(sql)
		if err != nil {
			p.err = err
			return err
		}
		p.stmts[name] = stmt
	}
	return nil
}

// Query...
func (p *MySQL5PreparedStatements) Query(name string, args ...interface{}) (*gosql.Rows, error) {
	if p.err != nil {
		return nil, fmt.Errorf("Cannot query when last error is not nil: %s", p.err.Error())
	}
	if _, ok := p.stmts[name]; !ok {
		return nil, fmt.Errorf("Prepared statement %s not created", name)
	}
	return p.stmts[name].Query(args...)
}

// Err returns the last error.
func (p *MySQL5PreparedStatements) Err() error {
	return p.err
}

// MySQL5Database implements Database for MySQL5.
type MySQL5Database struct {
	name      string
	server    *Server
	charSet   string
	collation string
	createSQL string
}

// NewMySQL5Database returns a new *MySQL5Database instance.
func NewMySQL5Database(server *Server, name, charSet, collation string) *MySQL5Database {
	if mysql5Stmts == nil {
		mysql5Stmts = NewMySQL5PreparedStatements(server.db)
	}
	return &MySQL5Database{
		server:    server,
		name:      name,
		charSet:   charSet,
		collation: collation,
	}
}

// Server...
func (db *MySQL5Database) Server() *Server {
	return db.server
}

// Name...
func (db *MySQL5Database) Name() string {
	return db.name
}

// SetName...
func (db *MySQL5Database) SetName(name string) {
	db.name = name
}

// CharSet...
func (db *MySQL5Database) CharSet() string {
	return db.charSet
}

// Collation...
func (db *MySQL5Database) Collation() string {
	return db.collation
}

// CreateSQL...
func (db *MySQL5Database) CreateSQL() (string, error) {
	if db.createSQL == "" {
		rows, err := db.server.query("SHOW CREATE DATABASE %s", MySQL5Backtick(db.name))
		if err != nil {
			return "", err
		}
		defer rows.Close()

		if !rows.Next() {
			return "", fmt.Errorf("SHOW CREATE DATABASE %s returned 0 rows", MySQL5Backtick(db.name))
		}
		if err = rows.Err(); err != nil {
			return "", err
		}
		var a string
		if err = rows.Scan(&a, &db.createSQL); err != nil {
			return "", err
		}
	}
	return db.createSQL, nil
}

// SetCreateSQL...
func (db *MySQL5Database) SetCreateSQL(sql string) {
	db.createSQL = sql
}

// Tables...
func (db *MySQL5Database) Tables() (tables TableGraph, err error) {
	mysql5Stmts.Prepare(
		"Tables",
		"SELECT `TABLE_NAME`, `TABLE_COLLATION` "+
		"FROM `INFORMATION_SCHEMA`.`TABLES` "+
		"WHERE `TABLE_SCHEMA` = ? "+
		"AND `TABLE_TYPE` = 'BASE TABLE'",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("Tables", db.Name()); err != nil {
		return
	}
	defer rows.Close()

	tables = make(TableGraph, 0)
	for rows.Next() {
		table := NewTable()
		if err = rows.Scan(&table.Name, &table.Collation); err != nil {
			return
		}
		if err = db.setTableDependencies(table); err != nil {
			return
		}
		if err = db.setTableCreateSQL(table); err != nil {
			return
		}
		var cols ColumnMap
		if cols, err = db.tableColumns(table.Name); err != nil {
			return
		}
		table.Columns = cols
		table.CharSet = db.charSet
		tables = append(tables, table)
	}
	if err = rows.Err(); err != nil {
		return
	}
	if tables, err = resolveTableGraph(tables); err != nil {
		return
	}
	if err = db.setTableGraphRows(tables); err != nil {
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
func (db *MySQL5Database) Views() (views ViewGraph, err error) {
	mysql5Stmts.Prepare(
		"Views",
		"SELECT `TABLE_NAME` "+
		"FROM `INFORMATION_SCHEMA`.`TABLES` "+
		"WHERE `TABLE_SCHEMA` = ? "+
		"AND `TABLE_TYPE` = 'VIEW'",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("Views", db.Name()); err != nil {
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
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// Routines...
func (db *MySQL5Database) Routines() (routines RoutineGraph, err error) {
	if !db.server.args.Routines {
		return
	}
	mysql5Stmts.Prepare(
		"Routines",
		"SELECT `name`, `type`, `character_set_client`, `collation_connection` "+
			"FROM `mysql`.`proc` "+
			"WHERE `db` = ?",
	)
	var rows *gosql.Rows
	rows, err = mysql5Stmts.Query("Routines", db.name)
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
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// setTableDependencies...
func (db *MySQL5Database) setTableDependencies(table *Table) (err error) {
	mysql5Stmts.Prepare(
		"setTableDependencies",
		"SELECT "+
			"`REFERENCED_TABLE_NAME`, "+
			"`REFERENCED_COLUMN_NAME`, "+
			"`COLUMN_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` "+
			"WHERE `REFERENCED_TABLE_SCHEMA` = ? "+
			"AND `TABLE_NAME` = ?",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("setTableDependencies", db.name, table.Name); err != nil {
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
		table.AppendDebugMsg("Dependency: %s.%s", dep.TableName, dep.ColumnName)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// setTableGraphRows...
func (db *MySQL5Database) setTableGraphRows(tables TableGraph) (err error) {
	defer func() {
		err = db.unlockTables()
	}()

	err = resolveTableConditions(tables, func(t *Table, conditions map[string][]string) (rows Rows, err error) {
		if err = db.lockTableRead(t.Name); err != nil {
			return
		}
		sql := db.buildSelectRowsSQL(t.Name, conditions)
		t.AppendDebugMsg(sql)
		if rows, err = db.server.querySelect(sql); err != nil {
			return
		}
		if err = db.unlockTables(); err != nil {
			return
		}
		t.Rows = rows
		return
	})
	return
}

// setTableTriggers...
func (db *MySQL5Database) setTableTriggers(table *Table) (err error) {
	mysql5Stmts.Prepare(
		"setTableTriggers",
		"SELECT `TRIGGER_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`TRIGGERS` "+
			"WHERE `TRIGGER_SCHEMA` = ? "+
			"AND `EVENT_OBJECT_TABLE` = ?",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("setTableTriggers", db.name, table.Name); err != nil {
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
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// showCreateTable...
func (db *MySQL5Database) setTableCreateSQL(table *Table) (err error) {
	var rows *gosql.Rows
	if rows, err = db.server.query("SHOW CREATE TABLE %s", MySQL5Backtick(table.Name)); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE TABLE %s returned 0 rows", MySQL5Backtick(table.Name))
		return
	}
	if err = rows.Err(); err != nil {
		return
	}
	var a string
	if err = rows.Scan(&a, &table.CreateSQL); err != nil {
		return
	}
	table.CreateSQL = mysql5RegexpAI.ReplaceAllString(table.CreateSQL, "")
	return
}

// setViewCreateSQL...
func (db *MySQL5Database) setViewCreateSQL(view *View) (err error) {
	mysql5Stmts.Prepare(
		"setViewCreateSQL",
		"SELECT `VIEW_DEFINITION`, `DEFINER`, `SECURITY_TYPE`, `CHARACTER_SET_CLIENT`, `COLLATION_CONNECTION`"+
		"FROM `INFORMATION_SCHEMA`.`VIEWS` "+
		"WHERE `TABLE_SCHEMA` = ? "+
		"AND `TABLE_NAME` = ? "+
		"LIMIT 1",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("setViewCreateSQL", db.Name(), view.Name); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE VIEW %s returned 0 rows", MySQL5Backtick(view.Name))
		return
	}
	if err = rows.Err(); err != nil {
		return
	}
	if err = rows.Scan(&view.CreateSQL, &view.Definer, &view.SecurityType, &view.CharSet, &view.Collation); err != nil {
		return
	}
	view.CreateSQL = fmt.Sprintf("VIEW %s AS %s", MySQL5Backtick(view.Name), view.CreateSQL)
	view.Definer = MySQL5BacktickUser(view.Definer)
	var cols ColumnMap
	if cols, err = db.tableColumns(view.Name); err != nil {
		return
	}
	view.Columns = cols
	return
}

// setRoutineCreateSQL...
func (db *MySQL5Database) setRoutineCreateSQL(r *Routine) (err error) {
	mysql5Stmts.Prepare(
		"setRoutineCreateSQL",
		"SELECT `type`, `body_utf8`, `security_type`, `definer`, `param_list`, `returns`, `is_deterministic`, `sql_mode` "+
		"FROM `mysql`.`proc` "+
		"WHERE `name` = ? "+
		"AND `db` = ? "+
		"LIMIT 1",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("setRoutineCreateSQL", r.Name, db.Name()); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE ROUTINE %s returned 0 rows", MySQL5Backtick(r.Name))
		return
	}
	if err = rows.Err(); err != nil {
		return
	}
	if err = rows.Scan(&r.Type, &r.CreateSQL, &r.SecurityType, &r.Definer, &r.ParamList, &r.Returns, &r.IsDeterministic, &r.SQLMode); err != nil {
		return
	}
	r.Definer = MySQL5BacktickUser(r.Definer)
	return
}

// setTriggerCreateSQL...
func (db *MySQL5Database) setTriggerCreateSQL(t *Trigger) (err error) {
	mysql5Stmts.Prepare(
		"setTriggerCreateSQL",
		"SELECT "+
		"`ACTION_STATEMENT`, "+
		"`ACTION_TIMING`, "+
		"`EVENT_MANIPULATION`, "+
		"`EVENT_OBJECT_TABLE`, "+
		"`ACTION_ORIENTATION`, "+
		"`DEFINER`, "+
		"`SQL_MODE`, "+
		"`CHARACTER_SET_CLIENT`, "+
		"`COLLATION_CONNECTION`"+
		"FROM `INFORMATION_SCHEMA`.`TRIGGERS` "+
		"WHERE `TRIGGER_SCHEMA` = ? "+
		"AND `TRIGGER_NAME` = ? "+
		"LIMIT 1",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("setTriggerCreateSQL", db.Name(), t.Name); err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		err = fmt.Errorf("SHOW CREATE TRIGGER %s returned 0 rows", MySQL5Backtick(t.Name))
		return
	}
	if err = rows.Err(); err != nil {
		return
	}
	if err = rows.Scan(
		&t.CreateSQL,
		&t.ActionTiming,
		&t.EventManipulation,
		&t.EventObjectTable,
		&t.ActionOrientation,
		&t.Definer,
		&t.SQLMode,
		&t.CharSet,
		&t.Collation); err != nil {
		return
	}
	t.Definer = MySQL5BacktickUser(t.Definer)
	return
}

// tableColumns...
func (db *MySQL5Database) tableColumns(tableName string) (cols ColumnMap, err error) {
	mysql5Stmts.Prepare(
		"tableColumns",
		"SELECT `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_TYPE`, `DATA_TYPE` "+
		"FROM `INFORMATION_SCHEMA`.`COLUMNS` "+
		"WHERE `TABLE_SCHEMA` = ? "+
		"AND `TABLE_NAME` = ?",
	)
	var rows *gosql.Rows
	if rows, err = mysql5Stmts.Query("tableColumns", db.Name(), tableName); err != nil {
		return
	}
	defer rows.Close()

	cols = ColumnMap{}
	for rows.Next() {
		col := &Column{}
		if err = rows.Scan(&col.Name, &col.OrdinalPosition, &col.Type, &col.DataType); err != nil {
			return
		}
		cols[col.Name] = col
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// buildSelectRowsSQL...
func (db *MySQL5Database) buildSelectRowsSQL(tableName string, conditions map[string][]string) string {
	where := db.buildWhereIn(conditions)
	var limit string
	if db.server.args.Limit != 0 {
		limit = fmt.Sprintf("LIMIT %d", db.server.args.Limit)
	}
	return fmt.Sprintf(
		"SELECT * FROM %s %s %s",
		MySQL5Backtick(tableName),
		where,
		limit,
	)
}

// buildWhereIn...
func (db *MySQL5Database) buildWhereIn(conditions map[string][]string) string {
	values := []string{}
	for col, vals := range conditions {
		if len(vals) > 0 {
			cond := fmt.Sprintf("%s IN(%s)", MySQL5Backtick(col), MySQL5JoinValues(vals))
			values = append(values, cond)
		}
	}
	if len(values) == 0 {
		return ""
	}
	return fmt.Sprintf("WHERE %s", strings.Join(values, " AND "))
}

// lockTableRead...
func (db *MySQL5Database) lockTableRead(tableName string) error {
	if !db.server.args.SkipLockTables {
		if err := db.server.exec("LOCK TABLES %s READ LOCAL", MySQL5Backtick(tableName)); err != nil {
			return err
		}
	}
	return nil
}

// unlockTables...
func (db *MySQL5Database) unlockTables() error {
	if !db.server.args.SkipLockTables {
		if err := db.server.exec("UNLOCK TABLES"); err != nil {
			return err
		}
	}
	return nil
}

// MySQL5Escape...
// @see https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
func MySQL5Escape(val string) string {
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

// MySQL5Backtick...
func MySQL5Backtick(col string) string {
	return fmt.Sprintf("`%s`", col)
}

// MySQL5BacktickUser...
func MySQL5BacktickUser(user string) string {
	parts := strings.SplitN(user, "@", 2)
	return fmt.Sprintf("`%s`@`%s`", parts[0], parts[1])
}

// MySQL5Quote...
func MySQL5Quote(val string) string {
	return fmt.Sprintf("'%s'", MySQL5Escape(val))
}

// MySQL5JoinValues...
func MySQL5JoinValues(vals []string) string {
	for i, val := range vals {
		vals[i] = MySQL5Quote(val)
	}
	return strings.Join(vals, ", ")
}

// MySQL5JoinColumns...
func MySQL5JoinColumns(cols []string) string {
	for i, col := range cols {
		cols[i] = MySQL5Backtick(col)
	}
	return strings.Join(cols, ", ")
}
