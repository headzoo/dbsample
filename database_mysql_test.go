package dbsampler

import "testing"

func TestMySQLEscape(t *testing.T) {
	tests := map[string]string{
		`Hello world`: `Hello world`,
		"Hello\x00world": `Hello\0world`,
		"Hello\033world": `Hello\Zworld`,
		"Hello\bworld": `Hello\bworld`,
		"Hello\rworld": `Hello\rworld`,
		"Hello\nworld": `Hello\nworld`,
		"Hello\tworld": `Hello\tworld`,
		`Hello\world`: `Hello\\world`,
		"\x00\033\b\r\n\t\\": `\0\Z\b\r\n\t\\`,
	}
	for s, ex := range tests {
		ac := MySQLEscape(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQLQuote(t *testing.T) {
	tests := map[string]string{
		`Hello world`: `'Hello world'`,
		"Hello\rworld": `'Hello\rworld'`,
		"Hello\nworld": `'Hello\nworld'`,
	}
	for s, ex := range tests {
		ac := MySQLQuote(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQLBacktick(t *testing.T) {
	tests := map[string]string{
		`Hello world`: "`Hello world`",
	}
	for s, ex := range tests {
		ac := MySQLBacktick(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQLJoinValues(t *testing.T) {
	tests := map[string][]string{
		`'Hello world'`: []string{`Hello world`},
		`'Hello world', 'Goodbye'`: []string{`Hello world`, `Goodbye`},
	}
	for ex, s := range tests {
		ac := MySQLJoinValues(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQLJoinColumns(t *testing.T) {
	tests := map[string][]string{
		"`Hello world`": []string{`Hello world`},
		"`Hello world`, `Goodbye`": []string{`Hello world`, `Goodbye`},
	}
	for ex, s := range tests {
		ac := MySQLJoinColumns(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}