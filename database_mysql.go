package dbsampler

import (
	"fmt"
	"regexp"
)

var air = regexp.MustCompile(`AUTO_INCREMENT=[\d]+ `)

// MySQLDatabase...
type MySQLDatabase struct {
	name      string
	server    *Server
	charSet   string
	collation string
	args      *Args
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
func (db *MySQLDatabase) Tables(limit int) (TableGraph, error) {
	var tables TableGraph
	var others TableGraph
	var err error
	var sql string

	schemas, err := db.schemas()
	if err != nil {
		return tables, err
	}
	for _, schema := range schemas {
		switch schema.Type {
		case schemaTypeBaseTable:
			if sql, err = db.showCreateTable(schema.Name); err != nil {
				return tables, err
			}
			deps, err := db.tableDependencies(schema.Name)
			if err != nil {
				return tables, err
			}
			tables = append(tables, NewTable(schema.Name, sql, deps))
		case schemaTypeView:
			if sql, err = db.showCreateView(schema.Name); err != nil {
				return tables, err
			}
			others = append(others, NewView(schema.Name, sql))
		case schemaTypeProcedure:
			if db.args.Routines {
				if sql, err = db.showCreateRoutine(schema.Name); err != nil {
					return tables, err
				}
				others = append(others, NewProcedure(schema.Name, sql))
			}
		case schemaTypeFunction:
			if db.args.Routines {
				if sql, err = db.showCreateRoutine(schema.Name); err != nil {
					return tables, err
				}
				others = append(others, NewFunction(schema.Name, sql))
			}
		case schemaTypeTrigger:
			if db.args.Triggers {
				if sql, err = db.showCreateTrigger(schema.Name); err != nil {
					return tables, err
				}
				others = append(others, NewTrigger(schema.Name, sql))
			}
		default:
			return tables, fmt.Errorf("Unknown schema type '%s'", schema.Type)
		}
	}

	if tables, err = ResolveTableGraph(tables); err != nil {
		return tables, err
	}
	if err := db.tableRows(tables, limit); err != nil {
		return tables, err
	}
	tables = append(tables, others...)

	return tables, nil
}

// Generator...
func (db *MySQLDatabase) Generator() (Generator, error) {
	switch db.server.major {
	case "5":
		return NewMySQL5Generator(db.args), nil
	}
	return nil, fmt.Errorf("Generator not available for %s %s", db.server.conn.Driver, db.server.major)
}

// schemas...
func (db *MySQLDatabase) schemas() ([]Schema, error) {
	var schemas []Schema
	rows, err := db.server.Query(
		"SELECT `TABLE_NAME`, `TABLE_TYPE` "+
			"FROM `INFORMATION_SCHEMA`.`TABLES` "+
			"WHERE `TABLE_SCHEMA` = '%s'",
		db.name)
	if err != nil {
		return schemas, err
	}
	defer rows.Close()
	for rows.Next() {
		var s Schema
		if err := rows.Scan(&s.Name, &s.Type); err != nil {
			return schemas, err
		}
		schemas = append(schemas, s)
	}
	rows.Close()

	if db.args.Routines {
		rows, err = db.server.Query("SELECT `name`, `type` FROM `mysql`.`proc` WHERE `db` = '%s'", db.name)
		if err != nil {
			return schemas, err
		}
		defer rows.Close()
		for rows.Next() {
			var s Schema
			if err := rows.Scan(&s.Name, &s.Type); err != nil {
				return schemas, err
			}
			schemas = append(schemas, s)
		}
		rows.Close()
	}

	if db.args.Triggers {
		rows, err := db.server.Query(
			"SELECT `TRIGGER_NAME` "+
				"FROM `INFORMATION_SCHEMA`.`TRIGGERS` "+
				"WHERE `TRIGGER_SCHEMA` LIKE '%s%%'",
			db.name)
		if err != nil {
			return schemas, err
		}
		defer rows.Close()
		for rows.Next() {
			var s Schema
			if err := rows.Scan(&s.Name); err != nil {
				return schemas, err
			}
			s.Type = schemaTypeTrigger
			schemas = append(schemas, s)
		}
		rows.Close()
	}

	return schemas, err
}

// tableDependencies...
func (db *MySQLDatabase) tableDependencies(tableName string) ([]Dependency, error) {
	deps := []Dependency{}
	rows, err := db.server.Query(
		"SELECT `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`, `COLUMN_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` "+
			"WHERE `REFERENCED_TABLE_SCHEMA` = '%s' "+
			"AND `TABLE_NAME` = '%s'",
		db.name,
		tableName)
	if err != nil {
		return deps, err
	}
	defer rows.Close()
	for rows.Next() {
		var dep Dependency
		if err := rows.Scan(&dep.TableName, &dep.ColumnName, &dep.ReferencedColumnName); err != nil {
			return deps, err
		}
		deps = append(deps, dep)
	}
	return deps, nil
}

// tableRows...
func (db *MySQLDatabase) tableRows(tg TableGraph, limit int) (err error) {
	defer func() {
		if !db.args.SkipLockTables {
			err = db.server.Exec(sqlUnlockTables)
		}
	}()

	tableRows := map[string]Rows{}
	for _, table := range tg {
		b := NewSelectBuilder(table)
		b.Limit = limit
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

		if !db.args.SkipLockTables {
			if err = db.server.Exec(sqlLockTablesReadLocal, backtick(table.Name)); err != nil {
				return
			}
		}
		var rows Rows
		rows, err = db.server.Select(b)
		if err != nil {
			return
		}
		if !db.args.SkipLockTables {
			if err = db.server.Exec(sqlUnlockTables); err != nil {
				return
			}
		}
		table.Rows = rows
		tableRows[table.Name] = rows
	}
	return
}

// showCreateTable...
func (db *MySQLDatabase) showCreateTable(tableName string) (string, error) {
	var row string
	rows, err := db.server.Query("SHOW CREATE TABLE %s", backtick(tableName))
	if err != nil {
		return row, err
	}
	defer rows.Close()
	for rows.Next() {
		var a string
		if err := rows.Scan(&a, &row); err != nil {
			return row, err
		}
	}
	return air.ReplaceAllString(row, ""), nil
}

// showCreateView...
func (db *MySQLDatabase) showCreateView(viewName string) (string, error) {
	var row string
	rows, err := db.server.Query("SHOW CREATE VIEW %s", backtick(viewName))
	if err != nil {
		return row, err
	}
	defer rows.Close()
	for rows.Next() {
		var a string
		var b string
		var c string
		if err := rows.Scan(&a, &row, &b, &c); err != nil {
			return row, err
		}
	}
	return row, nil
}

// showCreateRoutine...
func (db *MySQLDatabase) showCreateRoutine(name string) (string, error) {
	rows, err := db.server.Query(
		"SELECT `type`, `body_utf8`, `security_type`, `definer`, `param_list`, `returns`, `is_deterministic` "+
			"FROM `mysql`.`proc` "+
			"WHERE `name` = %s "+
			"AND `db` = %s",
		quote(name),
		quote(db.name),
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	rows.Next()

	var body string
	var typ string
	var securityType string
	var definer string
	var paramList string
	var returns string
	var isDet string
	if err := rows.Scan(&typ, &body, &securityType, &definer, &paramList, &returns, &isDet); err != nil {
		return "", err
	}

	var row string
	if typ == schemaTypeProcedure {
		row = fmt.Sprintf(
			"CREATE %s=%s PROCEDURE %s (%s)\n%s",
			securityType,
			backtickUser(definer),
			backtick(name),
			paramList,
			body,
		)
	} else {
		det := ""
		if isDet == "YES" {
			det = "\tDETERMINISTIC\n"
		}
		row = fmt.Sprintf(
			"CREATE %s=%s FUNCTION %s (%s) RETURNS %s\n%s%s",
			securityType,
			backtickUser(definer),
			backtick(name),
			paramList,
			returns,
			det,
			body,
		)
	}

	return row, nil
}

// showCreateTrigger...
func (db *MySQLDatabase) showCreateTrigger(name string) (string, error) {
	var row string
	rows, err := db.server.Query("SHOW CREATE TRIGGER %s", backtick(name))
	if err != nil {
		return row, err
	}
	defer rows.Close()
	rows.Next()
	var a string
	var b string
	var c string
	var d string
	var e string
	if err := rows.Scan(&a, &b, &row, &c, &d, &e); err != nil {
		return row, err
	}
	return row, nil
}
