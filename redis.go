package main

import (
	"fmt"
	"strconv"
)

func deserialize() {
	return
}

// +OK\r\n
func deserializeSimpleString(message string) (string, int, error) {
	if message[0] != '+' {
		return "", 0, fmt.Errorf("Expected a simple string startin with '+' but got '%c'\n", message[0])
	}

	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}

	return message[start:i], i - start, nil
}

func serializeSimpleString(message string) (string, int, error) {
	for _, c := range message {
		if c == '\n' || c == '\r' {
			return "", 0, fmt.Errorf("CLRF characters are not allowed in simple strings.")
		}
	}
	return fmt.Sprintf("+%s\r\n", message), len(message), nil
}

func deserializeSimpleError(message string) (string, int, error) {
	if message[0] != '-' {
		return "", 0, fmt.Errorf("Expected a simple error startin with '-' but got '%c'\n", message[0])
	}

	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}

	return message[start:i], i - start, nil
}

func serializeSimpleError(message string) (string, int, error) {
	for _, c := range message {
		if c == '\n' || c == '\r' {
			return "", 0, fmt.Errorf("CLRF characters are not allowed in simple errors.")
		}
	}
	return fmt.Sprintf("-%s\r\n", message), len(message), nil
}

func deserializeInteger(message string) (int, int, error) {
	if message[0] != ':' {
		return 0, 0, fmt.Errorf("Expected a integer to begin with ':' but got '%c'\n", message[0])
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

	return int(num), i - start, nil
}

func serializeInteger(num int) (string, int, error) {
	message := fmt.Sprintf(":%d\r\n", num)
	return message, len(message) - 2, nil
}

func serialize() {
	return
}

func main() {
	res, l, err := serializeSimpleString("hel\nlo\r")
	fmt.Println(res)
	fmt.Println(l)
	fmt.Println(err)
}
