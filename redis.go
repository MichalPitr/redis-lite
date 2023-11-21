package main

import "fmt"

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

func serialize() {
	return
}

func main() {
	return
}
