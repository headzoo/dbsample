package dbsampler

import (
	"fmt"
	"strings"
)

const (
	variableVersion = "version"
)

const (
	conditionVersion40000 = "40000"
	conditionVersion40101 = "40101"
	conditionVersion40103 = "40103"
	conditionVersion40111 = "40111"
	conditionVersion50001 = "50001"
	conditionVersion50003 = "50003"
	conditionVersion50013 = "50013"
)

const (
	sqlDelimiter           = ";"
	sqlRoutineDelimiter    = ";;"
	sqlComment             = "--"
	sqlConditionOpen       = "/*!"
	sqlConditionClose      = "*/"
	sqlDropTable           = "DROP TABLE IF EXISTS %s"
	sqlDropView            = "DROP VIEW IF EXISTS %s"
	sqlDropRoutine         = "DROP %s IF EXISTS %s"
	sqlLockTablesWrite     = "LOCK TABLES %s WRITE"
	sqlLockTablesReadLocal = "LOCK TABLES %s READ LOCAL"
	sqlUnlockTables        = "UNLOCK TABLES"
)

const (
	schemaTypeBaseTable = "BASE TABLE"
	schemaTypeView      = "VIEW"
	schemaTypeProcedure = "PROCEDURE"
	schemaTypeFunction  = "FUNCTION"
	schemaTypeTrigger   = "TRIGGER"
)

type SelectBuilder struct {
	Table      *Table
	Conditions map[string][]string
	Limit      int
}

func NewSelectBuilder(t *Table) *SelectBuilder {
	return &SelectBuilder{
		Table:      t,
		Conditions: make(map[string][]string),
	}
}

func (b *SelectBuilder) SQL() string {
	where := b.buildWhere()
	var limit string
	if b.Limit != 0 {
		limit = fmt.Sprintf("LIMIT %d", b.Limit)
	}
	return fmt.Sprintf(
		"SELECT * FROM %s %s %s",
		backtick(b.Table.Name),
		where,
		limit,
	)
}

func (b *SelectBuilder) buildWhere() string {
	conditions := []string{}
	for col, vals := range b.Conditions {
		if len(vals) > 0 {
			cond := fmt.Sprintf("%s IN(%s)", backtick(col), joinValues(vals))
			conditions = append(conditions, cond)
		}
	}
	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf("WHERE %s", strings.Join(conditions, " AND "))
}

func joinValues(vals []string) string {
	for i, val := range vals {
		vals[i] = quote(val)
	}
	return strings.Join(vals, ", ")
}

func joinColumns(cols []string) string {
	for i, col := range cols {
		cols[i] = backtick(col)
	}
	return strings.Join(cols, ", ")
}

func quote(val string) string {
	val = strings.Replace(val, "\n", "\\n", -1)
	val = strings.Replace(val, "\r", "\\r", -1)
	return fmt.Sprintf("'%s'", strings.Replace(val, "'", "\\'", -1))
}

func backtick(col string) string {
	return fmt.Sprintf("`%s`", col)
}

func backtickUser(user string) string {
	parts := strings.SplitN(user, "@", 2)
	return fmt.Sprintf("`%s`@`%s`", parts[0], parts[1])
}
