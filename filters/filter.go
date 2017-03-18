package filters

import (
	"fmt"
	"strings"
)

// Filter represents an object which filters table column values.
type Filter interface {
	Filter(value *string, dataType string, maxLength int64, args []string) error
	ValidateArgs(args []string) error
	Usage() string
}

// FilterCommand stores the details of a filter command.
type FilterCommand struct {
	FilterName string
	TableName  string
	ColumnName string
	Args       []string
}

// FilterController...
type FilterController struct {
	cmds   []*FilterCommand
	loaded map[string]Filter
}

// NewFilterController returns a new *FilterController instance.
func NewFilterController() *FilterController {
	return &FilterController{
		cmds: []*FilterCommand{},
	}
}

// Load the filters.
func (c *FilterController) Load() error {
	c.loaded = map[string]Filter{
		"empty":  NewEmptyFilter(),
		"repeat": NewRepeatFilter(),
	}
	return nil
}

// Usage returns the usage information for each loaded filter.
func (c *FilterController) Usage() []string {
	u := []string{}
	for _, f := range c.loaded {
		u = append(u, f.Usage())
	}
	return u
}

// SetCommands...
func (c *FilterController) SetCommands(cmds []string) (err error) {
	for _, cmd := range cmds {
		parts := strings.Split(cmd, " ")
		if len(parts) < 2 {
			err = fmt.Errorf(`Invalid filter "%s"`, cmd)
			return
		}

		filterName := parts[0]
		tableColumn := strings.Split(parts[1], ".")
		args := parts[2:]
		if len(tableColumn) != 2 {
			err = fmt.Errorf(`Invalid table.column in filter "%s"`, cmd)
			return
		}

		if _, ok := c.loaded[filterName]; !ok {
			err = fmt.Errorf(`Invalid filter "%s"`, filterName)
			return
		}
		if err = c.loaded[filterName].ValidateArgs(args); err != nil {
			return
		}
		c.cmds = append(c.cmds, &FilterCommand{
			FilterName: filterName,
			TableName:  tableColumn[0],
			ColumnName: tableColumn[1],
			Args:       args,
		})
	}
	return
}

// Filter...
func (c *FilterController) Filter(value *string, tableName, columnName, dataType string, maxLength int64) (err error) {
	for _, cmd := range c.cmds {
		if cmd.TableName == tableName && cmd.ColumnName == columnName {
			if err = c.loaded[cmd.FilterName].Filter(value, dataType, maxLength, cmd.Args); err != nil {
				return
			}
		}
	}
	return
}
