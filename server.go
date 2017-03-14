package dbsampler

import (
	gosql "database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

// Server...
type Server struct {
	conn    *Connection
	args    *Args
	db      *gosql.DB
	version string
	major   string
	minor   string
	rev     string
}

// NewServer returns a new *Server instance.
func NewServer(conn *Connection, args *Args) *Server {
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
	db := &MySQLDatabase{
		name:   name,
		server: s,
		args:   s.args,
	}
	meta, err := s.selectDatabaseMeta(name)
	if err != nil {
		return nil, err
	}
	db.charSet = meta.CharacterSet
	db.collation = meta.Collation
	return db, nil
}

// Variable...
func (s *Server) Variable(v string) (string, error) {
	var row string
	rows, err := s.Query("SELECT @@%s", v)
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

// Select...
func (s *Server) Select(b *SelectBuilder) (Rows, error) {
	res := make(Rows, 0)
	rows, err := s.Query(b.SQL())
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

// Query...
func (s *Server) Query(sql string, args ...interface{}) (*gosql.Rows, error) {
	sql = fmt.Sprintf(sql, args...)
	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// Exec...
func (s *Server) Exec(sql string, args ...interface{}) error {
	sql = fmt.Sprintf(sql, args...)
	_, err := s.db.Exec(sql)
	return err
}

// Version...
func (s *Server) Version() string {
	return s.version
}

// VersionNumber...
func (s *Server) VersionNumber() string {
	return fmt.Sprintf("%s%02s%02s", s.major, s.minor, s.rev)
}

// selectDatabaseMeta...
func (s *Server) selectDatabaseMeta(name string) (meta, error) {
	var m meta
	rows, err := s.Query(
		"SELECT `DEFAULT_CHARACTER_SET_NAME`, `DEFAULT_COLLATION_NAME` "+
			"FROM `INFORMATION_SCHEMA`.`SCHEMATA` "+
			"WHERE `SCHEMA_NAME` = '%s' "+
			"LIMIT 1",
		name,
	)
	if err != nil {
		return m, err
	}
	defer rows.Close()
	rows.Next()
	if err := rows.Scan(&m.CharacterSet, &m.Collation); err != nil {
		return m, err
	}
	return m, err
}

// setVersion...
func (s *Server) setVersion() error {
	ver, err := s.Variable(variableVersion)
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

type meta struct {
	CharacterSet string
	Collation    string
}
