package main

import "testing"

func TestDeserializeSimpleString(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should deserialize simple 'OK'", "+OK\r\n", "OK"},
		{"Should deserialize simple 'Hello World'", "+Hello World\r\n", "Hello World"},
		{"Should deserialize string with special chars 'Hello\\\tWorld'", "+Hello\\\tWorld\r\n", "Hello\\	World"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := deserializeSimpleString(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestSerializeSimpleString(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should serialize simple 'OK'", "OK", "+OK\r\n"},
		{"Should serialize simple 'Hello World'", "Hello World", "+Hello World\r\n"},
		{"Should serialize string with special chars 'Hello\\\tWorld'", "Hello\\	World", "+Hello\\\tWorld\r\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := serializeSimpleString(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestSerializeSimpleStringInvalid(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should fail to serialize string with CLRF chars 'O\nK\r'", "O\nK\r", ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := serializeSimpleString(test.input)
			if err == nil {
				t.Errorf("Got no error but expected failure.")
			}
		})
	}
}

func TestDeserializeSimpleError(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should deserialize simple error 'ERR'", "-ERR unknown command 'asdf'\r\n", "ERR unknown command 'asdf'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := deserializeSimpleError(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestSerializeSimpleError(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should serialize simple error 'ERR'", "ERR unknown command 'asdf'", "-ERR unknown command 'asdf'\r\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := serializeSimpleError(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestSerializeSimpleErrorInvalid(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should fail to serialize error with CLRF chars 'ERR\\n'", "ERR\n", ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := serializeSimpleError(test.input)
			if err == nil {
				t.Errorf("Got no error but expected failure.")
			}
		})
	}
}

func TestDeserializeInteger(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  int
	}{
		// the table itself
		{"Should deserialize unsigned integer", ":1000\r\n", 1000},
		{"Should deserialize negative integer", ":-1000\r\n", -1000},
		{"Should deserialize positive integer", ":+1000\r\n", 1000},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := deserializeInteger(test.input)
			if ans != test.want {
				t.Errorf("Got '%d' but expected '%d'.", ans, test.want)
			}
		})
	}
}

func TestSerializeInteger(t *testing.T) {
	var tests = []struct {
		name  string
		input int
		want  string
	}{
		// the table itself
		{"Should serialize unsigned integer", 1000, ":1000\r\n"},
		{"Should serialize negative integer", -1000, ":-1000\r\n"},
		{"Should serialize positive integer", +1000, ":1000\r\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := serializeInteger(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestDeserializeBulkString(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should deserialize bulk string 'OK'", "$2\r\nOK\r\n", "OK"},
		{"Should deserialize bulk string 'Hello World'", "$11\r\nHello World\r\n", "Hello World"},
		{"Should deserialize long bulk string", "$72\r\nThis is a long bulk string with multiple words and \n special characters.\r\n", "This is a long bulk string with multiple words and \n special characters."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := deserializeBulkString(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestSerializeBulkString(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		// the table itself
		{"Should serialize 'OK'", "OK", "$2\r\nOK\r\n"},
		{"Should serialize empty string", "", "$0\r\n\r\n"},
		{"Should serialize 'Hello World'", "Hello World", "$11\r\nHello World\r\n"},
		{"Should serialize string with special chars", "Hello\nWorld", "$11\r\nHello\nWorld\r\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, _ := serializeBulkString(test.input)
			if ans != test.want {
				t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
			}
		})
	}
}

func TestIsNullBulkString(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  bool
	}{
		// the table itself
		{"Should detect null string", "$-1\r\n", true},
		{"Should detect normal string", "$11\r\nHello World\r\n", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _ := isNullBulkString(test.input)
			if ans != test.want {
				t.Errorf("Got '%v' but expected '%v'.", ans, test.want)
			}
		})
	}
}
