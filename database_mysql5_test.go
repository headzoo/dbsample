package dbsampler

import "testing"

func TestMySQL5Escape(t *testing.T) {
	tests := map[string]string{
		`Hello world`:        `Hello world`,
		"Hello\x00world":     `Hello\0world`,
		"Hello\033world":     `Hello\Zworld`,
		"Hello\bworld":       `Hello\bworld`,
		"Hello\rworld":       `Hello\rworld`,
		"Hello\nworld":       `Hello\nworld`,
		"Hello\tworld":       `Hello\tworld`,
		`Hello\world`:        `Hello\\world`,
		"\x00\033\b\r\n\t\\": `\0\Z\b\r\n\t\\`,
	}
	for s, ex := range tests {
		ac := MySQL5Escape(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQL5Quote(t *testing.T) {
	tests := map[string]string{
		`Hello world`:  `'Hello world'`,
		"Hello\rworld": `'Hello\rworld'`,
		"Hello\nworld": `'Hello\nworld'`,
	}
	for s, ex := range tests {
		ac := MySQL5Quote(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQL5Backtick(t *testing.T) {
	tests := map[string]string{
		`Hello world`: "`Hello world`",
	}
	for s, ex := range tests {
		ac := MySQL5Backtick(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQL5JoinValues(t *testing.T) {
	tests := map[string][]string{
		`'Hello world'`:            []string{`Hello world`},
		`'Hello world', 'Goodbye'`: []string{`Hello world`, `Goodbye`},
	}
	for ex, s := range tests {
		ac := MySQL5JoinValues(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}

func TestMySQL5JoinColumns(t *testing.T) {
	tests := map[string][]string{
		"`Hello world`":            []string{`Hello world`},
		"`Hello world`, `Goodbye`": []string{`Hello world`, `Goodbye`},
	}
	for ex, s := range tests {
		ac := MySQL5JoinColumns(s)
		if ex != ac {
			t.Errorf(`Expected '%s', got '%s'`, ex, ac)
		}
	}
}
