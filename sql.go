package dbsampler

import (
	"fmt"
	"strings"
)

// SelectBuilder...
type SelectBuilder struct {
	Table      *Table
	Conditions map[string][]string
	Limit      int
}

// NewSelectBuilder returns a *SelectBuilder instance.
func NewSelectBuilder(t *Table) *SelectBuilder {
	return &SelectBuilder{
		Table:      t,
		Conditions: make(map[string][]string),
	}
}

// SQL...
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

// buildWhere...
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

// joinValues...
func joinValues(vals []string) string {
	for i, val := range vals {
		vals[i] = quote(val)
	}
	return strings.Join(vals, ", ")
}

// joinColumns...
func joinColumns(cols []string) string {
	for i, col := range cols {
		cols[i] = backtick(col)
	}
	return strings.Join(cols, ", ")
}

// quote...
func quote(val string) string {
	val = strings.Replace(val, "\n", "\\n", -1)
	val = strings.Replace(val, "\r", "\\r", -1)
	return fmt.Sprintf("'%s'", strings.Replace(val, "'", "\\'", -1))
}

// backtick...
func backtick(col string) string {
	return fmt.Sprintf("`%s`", col)
}
