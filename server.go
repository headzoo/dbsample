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
			return NewMySQLDatabase(s, name, charSet, collation), nil
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
	rows.Next()
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

// selectRows...
func (s *Server) selectRows(sql string) (Rows, error) {
	res := make(Rows, 0)
	rows, err := s.query(sql)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return res, err
	}
	clen := len(cols)

	raw := make([][]byte, clen)
	result := make([]string, clen)
	dest := make([]interface{}, clen)
	for i, _ := range raw {
		dest[i] = &raw[i]
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return res, err
		}
		for i, raw := range raw {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		row := make(Row, clen)
		for i, c := range cols {
			row[c] = result[i]
		}
		res = append(res, row)
	}

	return res, nil
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
