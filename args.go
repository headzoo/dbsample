package dbsampler

import "fmt"

const (
	DriverMySQL = "mysql"
)

type Connection struct {
	Driver string
	Name   string
	Host   string
	Port   string
	User   string
	Pass   string
}

func (a Connection) dsn() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		a.User,
		a.Pass,
		a.Host,
		a.Port,
		a.Name,
	)
}

type Args struct {
	Limit            int
	Routines         bool
	Triggers         bool
	SkipLockTables   bool
	SkipAddDropTable bool
}
