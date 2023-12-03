package main

import (
	"fmt"
	"strconv"
	"strings"
)

// +OK\r\n
func deserializeSimpleString(message string) (string, int, error) {
	if message[0] != '+' {
		return "", 0, fmt.Errorf("Expected a simple string startin with '+' but got '%c'", message[0])
	}

	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}

	return message[start:i], i + 2, nil
}

func serializeSimpleString(message string) (string, error) {
	if strings.ContainsAny(message, "\r\n") {
		return "", fmt.Errorf("CRLF characters are not allowed in simple strings")
	}
	return fmt.Sprintf("+%s\r\n", message), nil
}

func deserializeSimpleError(message string) (string, int, error) {
	if message[0] != '-' {
		return "", 0, fmt.Errorf("Expected a simple error startin with '-' but got '%c'", message[0])
	}

	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}

	return message[start:i], i + 2, nil
}

func serializeSimpleError(message string) (string, error) {
	if strings.ContainsAny(message, "\r\n") {
		return "", fmt.Errorf("CRLF characters are not allowed in simple errors")
	}
	return fmt.Sprintf("-%s\r\n", message), nil
}

func deserializeInteger(message string) (int, int, error) {
	if message[0] != ':' {
		return 0, 0, fmt.Errorf("Expected a integer to begin with ':' but got '%c'", message[0])
	}

	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}

	num, err := strconv.ParseInt(message[start:i], 10, 32)
	if err != nil {
		return 0, 0, err
	}

	return int(num), i + 2, nil
}

func serializeInteger(num int64) (string, int, error) {
	message := fmt.Sprintf(":%d\r\n", num)
	return message, len(message) - 2, nil
}

func deserializeNullOrBulkString(message string) (interface{}, int, error) {
	if message[0] != '$' {
		return "", 0, fmt.Errorf("Expected bulk string to begin with '$' but got '%c'", message[0])
	}

	isNull, err := isNullBulkString(message)
	if err != nil {
		return "", 0, err
	}
	if isNull == true {
		return nil, 5, nil
	}

	return deserializeBulkString(message)
}

func deserializeBulkString(message string) (string, int, error) {
	// decode string length
	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}
	length, err := strconv.ParseInt(message[start:i], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("Failed to decode bulk string length: %v", err)
	}

	// Skip over CLRF after string length
	i += 2
	return message[i : i+int(length)], i + int(length) + 2, nil
}

func serializeBulkString(message string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)
}

func isNullBulkString(message string) (bool, error) {
	if message[0] != '$' {
		return false, fmt.Errorf("Expected bulk string to begin with '$' but got '%c'", message[0])
	}

	if message[1] == '-' && message[2] == '1' {
		return true, nil
	}

	if message[1] == '-' {
		return false, fmt.Errorf("Only negative 1 is allowed in bulk strings to indicate null string.")
	}
	return false, nil
}

func deserializeNullOrArray(message string) (interface{}, int, error) {
	if message[0] != '*' {
		return "", 0, fmt.Errorf("Expected array to begin with '*' but got '%c'", message[0])
	}

	if message[1] == '-' && message[2] == '1' {
		if message[3] != '\r' || message[4] != '\n' {
			return "", 0, fmt.Errorf("Null array not terminated with CRLF.")
		}
		return nil, 5, nil
	}

	return deserializeArray(message)
}

func serializeNullArray() (string, int, error) {
	return "*-1\r\n", 5, nil
}

func serializeNullBulkString() (string, int, error) {
	return "$-1\r\n", 5, nil
}

func deserializeArray(message string) ([]interface{}, int, error) {
	if message[0] != '*' {
		return []interface{}{}, 0, fmt.Errorf("Expected array to begin with '*' but got '%c'", message[0])
	}

	// determine array length
	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}
	length, err := strconv.ParseInt(message[start:i], 10, 32)
	if err != nil {
		return []interface{}{}, 0, fmt.Errorf("Failed to decode bulk string length: %v", err)
	}
	if length < 0 {
		return []interface{}{}, 0, fmt.Errorf("Array langth can't be negative %v", err)
	}

	// step over CRLF
	i += 2

	arr := make([]interface{}, length)
	for idx := 0; idx < int(length); idx++ {
		val, s, err := deserializePrimitive(message[i:])
		if err != nil {
			return nil, 0, err
		}
		i += s
		arr[idx] = val
	}
	return arr, i - start, nil
}

func serializeStringArray(arr []string) (string, int, error) {
	res := fmt.Sprintf("*%d\r\n", len(arr))
	for _, item := range arr {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
	}
	return res, len(res), nil
}

func deserializePrimitive(message string) (interface{}, int, error) {
	switch message[0] {
	case '+':
		return deserializeSimpleString(message)
	case '-':
		return deserializeSimpleError(message)
	case ':':
		return deserializeInteger(message)
	case '$':
		return deserializeNullOrBulkString(message)
	}
	return nil, 0, fmt.Errorf("Expected a primitive to deserialize, but received unsupported type: '%c'", message[0])
}
