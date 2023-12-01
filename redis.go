package main

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Record struct {
	value           string
	expiryTimestamp int64
}

type Store struct {
	mu   sync.Mutex
	dict map[string]Record
}

func newStore() *Store {
	store := &Store{
		dict: map[string]Record{},
	}
	return store
}

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
		return "", 0, fmt.Errorf("Expected a simple error startin with '-' but got '%c'", message[0])
	}

	start := 1
	i := start
	for message[i] != '\r' && message[i+1] != '\n' {
		i++
	}

	return message[start:i], i + 2, nil
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

func serializeBulkString(message string) (string, int, error) {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message), message), len(message), nil
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

func handleRequest(conn net.Conn, store *Store) {
	defer conn.Close()

	for {
		buf := make([]byte, 2048)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		// deserialize command
		input, _, err := deserializeNullOrArray(string(buf[:n]))
		if err != nil {
			msg, _, _ := serializeSimpleError("ERR - failed while deserializing input.")
			conn.Write([]byte(msg))
		}

		if input == nil {
			fmt.Println("Received nil array.")
		} else {
			if arr, ok := input.([]interface{}); ok {
				switch arr[0] {
				case "PING", "ping":
					if len(arr) == 1 {
						msg, _, _ := serializeBulkString("PONG")
						conn.Write([]byte(msg))
					}
					if len(arr) == 2 {
						msg, _, _ := serializeBulkString(arr[1].(string))
						conn.Write([]byte(msg))
					} else {
						msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'ping' command")
						conn.Write([]byte(msg))
					}
				case "ECHO", "echo":
					msg, _, _ := serializeBulkString(arr[1].(string))
					conn.Write(([]byte(msg)))
				case "SET", "set":
					// Handle access to shared store used by other go-routines.
					handleSet(arr, conn, store)
				case "GET", "get":
					// Handle access to shared store used by other go-routines.
					handleGet(arr, conn, store)
				case "EXISTS", "exists":
					if len(arr) < 2 {
						msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'exists' command")
						conn.Write([]byte(msg))
						continue
					}
					count := 0
					for _, key := range arr[1:] {
						if _, ok := (*store).dict[key.(string)]; ok {
							count++
						}
					}
					msg, _, _ := serializeInteger(int64(count))
					conn.Write([]byte(msg))
				case "DEL", "del":
					if len(arr) < 2 {
						msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'del' command")
						conn.Write([]byte(msg))
						continue
					}
					count := 0
					(*store).mu.Lock()
					for _, key := range arr[1:] {
						if _, ok := (*store).dict[key.(string)]; ok {
							count++
							delete((*store).dict, key.(string))
						}
					}
					(*store).mu.Unlock()
					msg, _, _ := serializeInteger(int64(count))
					conn.Write([]byte(msg))
				case "INCR", "incr":
					handleIncr(arr, conn, store)
				case "DECR", "decr":
					handleDecr(arr, conn, store)
				default:
					msg, _, _ := serializeSimpleError(fmt.Sprintf("-ERR unknown command '%s'", arr[0].(string)))
					conn.Write(([]byte(msg)))
					continue
				}
			}
		}
	}
}

func handleDecr(arr []interface{}, conn net.Conn, store *Store) {
	if len(arr) != 2 {
		msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'decr' command")
		conn.Write([]byte(msg))
		return
	}
	(*store).mu.Lock()
	defer (*store).mu.Unlock()
	val, ok := (*store).dict[arr[1].(string)]

	if ok != true {
		val = Record{"0", -1}
		(*store).dict[arr[1].(string)] = val
	}
	num, err := strconv.ParseInt(val.value, 10, 64)
	if err != nil || num <= math.MinInt64 {
		msg, _, _ := serializeSimpleError("ERR value is not an integer or out of range")
		conn.Write([]byte(msg))
		return
	}
	num--
	(*store).dict[arr[1].(string)] = Record{fmt.Sprint(num), val.expiryTimestamp}
	(*store).mu.Unlock()
	msg, _, _ := serializeInteger(num)
	conn.Write([]byte(msg))
	return
}

func handleIncr(arr []interface{}, conn net.Conn, store *Store) {
	if len(arr) != 2 {
		msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'INCR' command")
		conn.Write([]byte(msg))
		return
	}
	(*store).mu.Lock()
	defer (*store).mu.Unlock()
	val, ok := (*store).dict[arr[1].(string)]

	if ok != true {
		val = Record{"0", -1}
		(*store).dict[arr[1].(string)] = val
	}
	num, err := strconv.ParseInt(val.value, 10, 64)
	if err != nil || num >= math.MaxInt64 {
		msg, _, _ := serializeSimpleError("ERR value is not an integer or out of range")
		conn.Write([]byte(msg))
		return
	}
	num++
	(*store).dict[arr[1].(string)] = Record{fmt.Sprint(num), val.expiryTimestamp}
	msg, _, _ := serializeInteger(num)
	conn.Write([]byte(msg))
	return
}

func handleGet(arr []interface{}, conn net.Conn, store *Store) {
	if len(arr) != 2 {
		msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'get' command")
		conn.Write(([]byte(msg)))
		return
	}
	fmt.Println("In handleGet")

	(*store).mu.Lock()
	defer (*store).mu.Unlock()
	record, ok := (*store).dict[arr[1].(string)]

	if ok == false {
		fmt.Println("Returning nil")

		msg, _, _ := serializeNullArray()
		conn.Write(([]byte(msg)))
		return
	}

	// delete expired key and return nil, since the key doesn't exist anymore.
	if recordExpired(record.expiryTimestamp) {
		fmt.Println("expired record")
		delete(*&store.dict, arr[1].(string))

		msg, _, _ := serializeNullArray()
		conn.Write(([]byte(msg)))
		return
	}

	fmt.Println("exiting")
	msg, _, _ := serializeBulkString(record.value)
	conn.Write(([]byte(msg)))
	return
}

func recordExpired(recordExpiration int64) bool {
	if recordExpiration == -1 {
		return false
	}
	return recordExpiration < time.Now().UnixMilli()
}

func handleSet(arr []interface{}, conn net.Conn, store *Store) {
	// -1 denotes no expiration is set.
	var expiryTimestamp int64 = -1

	if len(arr) == 5 {
		switch arr[3].(string) {
		case "EX", "ex":
			durationSeconds, err := strconv.ParseInt(arr[4].(string), 10, 64)
			if err != nil {
				msg, _, _ := serializeSimpleError("ERR 'EX' arguments has to be integer")
				conn.Write(([]byte(msg)))
				return
			}
			if durationSeconds <= 0 {
				msg, _, _ := serializeSimpleError("ERR 'EX' option has to be positive")
				conn.Write(([]byte(msg)))
				return
			}
			expiryTimestamp = time.Now().UnixMilli() + durationSeconds*1000
		case "PX", "px":
			durationMilli, err := strconv.ParseInt(arr[4].(string), 10, 64)
			if err != nil {
				msg, _, _ := serializeSimpleError("ERR 'PX' arguments has to be integer")
				conn.Write(([]byte(msg)))
				return
			}
			if durationMilli <= 0 {
				msg, _, _ := serializeSimpleError("ERR 'PX' option has to be positive")
				conn.Write(([]byte(msg)))
				return
			}
			expiryTimestamp = time.Now().UnixMilli() + durationMilli
		case "EXAT", "exat":
			unixSeconds, err := strconv.ParseInt(arr[4].(string), 10, 64)
			if err != nil {
				msg, _, _ := serializeSimpleError("ERR 'EXAT' arguments has to be integer")
				conn.Write(([]byte(msg)))
				return
			}
			if unixSeconds <= 0 {
				msg, _, _ := serializeSimpleError("ERR 'EXAT' option has to be positive")
				conn.Write(([]byte(msg)))
				return
			}
			expiryTimestamp = unixSeconds * 1000
		case "PXAT", "pxat":
			unixMilli, err := strconv.ParseInt(arr[4].(string), 10, 64)
			if err != nil {
				msg, _, _ := serializeSimpleError("ERR 'PXAT' arguments has to be integer")
				conn.Write(([]byte(msg)))
				return
			}
			if unixMilli <= 0 {
				msg, _, _ := serializeSimpleError("ERR 'PXAT' option has to be positive")
				conn.Write(([]byte(msg)))
				return
			}
			expiryTimestamp = unixMilli
		default:
			msg, _, _ := serializeSimpleError("ERR unknown option for SET")
			conn.Write(([]byte(msg)))
			return
		}
	} else if len(arr) != 3 {
		msg, _, _ := serializeSimpleError("ERR wrong number of arguments for 'set' command")
		conn.Write(([]byte(msg)))
		return
	}

	(*store).mu.Lock()
	(*store).dict[arr[1].(string)] = Record{value: arr[2].(string), expiryTimestamp: expiryTimestamp}
	(*store).mu.Unlock()

	msg, _, _ := serializeSimpleString("OK")
	conn.Write(([]byte(msg)))
}

// TODO: Maybe have to use mutex, but then we block the store for quite a long time.
func activeKeyExpirer(store *Store) {
	// get keys
	for {
		expired := 0
		total := 0
		keys := make([]string, 0)
		(*store).mu.Lock()
		for k, v := range (*store).dict {
			if v.expiryTimestamp == -1 {
				continue
			}
			if recordExpired(v.expiryTimestamp) {
				expired++
				keys = append(keys, k)
			}
			total++

			if total >= 20 {
				break
			}
		}

		for _, k := range keys {
			delete((*store).dict, k)
		}

		(*store).mu.Unlock()

		if float64(expired)/float64(total) < 0.25 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes
	defer listener.Close()
	fmt.Println("Listening on 0.0.0.0:6379")

	store := newStore()

	// start active key expirer
	go activeKeyExpirer(store)

	for {
		// Accept a connection\\
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn, store)
	}
}
