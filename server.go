package dbsampler

import (
	gosql "database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

// Server...
type Server struct {
	conn    *ConnectionArgs
	args    *DumpArgs
	db      *gosql.DB
	version string
	major   string
	minor   string
	rev     string
}

// NewServer returns a new *Server instance.
func NewServer(conn *ConnectionArgs, args *DumpArgs) *Server {
	return &Server{
		conn: conn,
		args: args,
	}
}

// Open...
func (s *Server) Open() error {
	conn, err := gosql.Open(s.conn.Driver, s.conn.dsn())
	if err != nil {
		return err
	}
	s.db = conn
	if err := s.setVersion(); err != nil {
		return err
	}
	return nil
}

// Close...
func (s *Server) Close() error {
	return s.db.Close()
}

// Database returns a new Database instance.
func (s *Server) Database(name string) (Database, error) {
	charSet, collation, err := s.selectDatabaseCharSet(name)
	if err != nil {
		return nil, err
	}

	switch s.conn.Driver {
	case DriverMySQL:
		switch s.major {
		case "5":
			return NewMySQL5Database(s, name, charSet, collation), nil
		}
	}
	return nil, fmt.Errorf("Database not available for %s %s", s.conn.Driver, s.major)
}

// Variable...
func (s *Server) Variable(v string) (string, error) {
	var row string
	rows, err := s.query("SELECT @@%s", v)
	if err != nil {
		return row, err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", nil
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if err := rows.Scan(&row); err != nil {
		return row, err
	}
	return row, nil
}

// Version...
func (s *Server) Version() string {
	return s.version
}

// VersionNumber...
func (s *Server) VersionNumber() string {
	return fmt.Sprintf("%s%02s%02s", s.major, s.minor, s.rev)
}

// query...
func (s *Server) query(sql string, args ...interface{}) (*gosql.Rows, error) {
	sql = fmt.Sprintf(sql, args...)
	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// exec...
func (s *Server) exec(sql string, args ...interface{}) error {
	sql = fmt.Sprintf(sql, args...)
	_, err := s.db.Exec(sql)
	return err
}

// buildQueryRows...
func (s *Server) buildQueryRows(rows *gosql.Rows) (results Rows, err error) {
	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return
	}
	colNum := len(columns)

	for rows.Next() {
		rawValues := make([]interface{}, colNum)
		rawBytes := make([][]byte, colNum)
		for i, _ := range rawValues {
			rawValues[i] = &rawBytes[i]
		}
		rows.Scan(rawValues...)

		strValues := make([]string, colNum)
		for i, v := range rawBytes {
			if v == nil {
				strValues[i] = ""
			} else {
				strValues[i] = string(v)
			}
		}

		fields := make(Row, colNum)
		for i, c := range columns {
			fields[i] = Field{
				Column: c,
				Value:  strValues[i],
			}
		}
		results = append(results, fields)
	}
	err = rows.Err()
	return
}

// selectDatabaseCharSet...
func (s *Server) selectDatabaseCharSet(name string) (charSet string, collation string, err error) {
	var rows *gosql.Rows
	if rows, err = s.query(
		"SELECT `DEFAULT_CHARACTER_SET_NAME`, `DEFAULT_COLLATION_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`SCHEMATA` "+
			"WHERE `SCHEMA_NAME` = '%s' "+
			"LIMIT 1",
		name,
	); err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		err = fmt.Errorf("SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME returned 0 rows")
		return
	}
	if err = rows.Err(); err != nil {
		return
	}
	if err = rows.Scan(&charSet, &collation); err != nil {
		return
	}
	return
}

// setVersion...
func (s *Server) setVersion() error {
	ver, err := s.Variable("version")
	if err != nil {
		return err
	}

	v := strings.SplitN(ver, "-", 2)
	vp := strings.Split(v[0], ".")
	s.version = ver
	s.major = vp[0]
	s.minor = vp[1]
	s.rev = vp[2]
	return nil
}
