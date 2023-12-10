package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const port = 6379
const activeExpireKeyLimit = 20

func handleRequest(conn net.Conn, store *dictionary) {
	defer conn.Close()
	// TODO: Find a way to support long messages without allocating 512MB. Most messages are short.
	var buffer []byte
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from client:", err)
			return
		}

		buffer = append(buffer, buf[:n]...)

		if len(buffer) >= 2 && buffer[len(buffer)-2] == '\r' && buffer[len(buffer)-1] == '\n' {
			input, _, err := deserializeNullOrArray(string(buf[:n]))
			if err != nil {
				sendErrorToClient(conn, "ERR - failed while deserializing input")
				return
			}

			if input == nil {
				log.Println("Received nil array.")
			} else {
				if arr, ok := input.([]interface{}); ok {
					cmd := strings.ToLower(arr[0].(string))
					switch cmd {
					case "ping":
						handlePing(arr, conn)
					case "echo":
						handleEcho(arr, conn)
					case "set":
						handleSet(arr, conn, store)
					case "get":
						handleGet(arr, conn, store)
					case "exists":
						handleExists(arr, conn, store)
					case "del":
						handleDel(arr, conn, store)
					case "incr":
						handleIncr(arr, conn, store)
					case "decr":
						handleDecr(arr, conn, store)
					case "lpush":
						handleLPush(arr, conn, store)
					case "lpop":
						handleLPop(arr, conn, store)
					default:
						msg, _ := serializeSimpleError(fmt.Sprintf("-ERR unknown command '%s'", cmd))
						sendMsgToClient(conn, msg)
					}
				}
			}
			// Clear buffer for the next message
			buffer = nil
		}
	}
}

func handleEcho(arr []interface{}, conn net.Conn) {
	msg := serializeBulkString(arr[1].(string))
	sendMsgToClient(conn, msg)
}

func handleDel(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) < 2 {
		sendErrorToClient(conn, "ERR wrong number of arguments for 'del' command")
		return
	}
	count := 0
	store.mu.Lock()
	for _, key := range arr[1:] {
		if _, ok := store.dict[key.(string)]; ok {
			count++
			delete(store.dict, key.(string))
		}
	}
	store.mu.Unlock()
	msg, _, _ := serializeInteger(int64(count))
	sendMsgToClient(conn, msg)
}

func handleExists(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) < 2 {
		sendErrorToClient(conn, "ERR wrong number of arguments for 'exists' command")
		return
	}
	count := 0
	for _, key := range arr[1:] {
		if _, ok := store.dict[key.(string)]; ok {
			count++
		}
	}
	msg, _, _ := serializeInteger(int64(count))
	sendMsgToClient(conn, msg)
}

func handlePing(arr []interface{}, conn net.Conn) {
	var msg string
	var err error

	switch len(arr) {
	case 1:
		msg, err = serializeSimpleString("PONG")
	case 2:
		msg = serializeBulkString(arr[1].(string))
	default:
		msg, err = serializeSimpleError("ERR wrong number of arguments for 'ping' command")
	}

	if err != nil {
		log.Println(err)
		return
	}

	sendMsgToClient(conn, msg)
}

func handleLPush(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) < 2 {
		msg, _ := serializeSimpleError("ERR wrong number of arguments for 'lpush' command")
		sendMsgToClient(conn, msg)
		return
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	// Add logic to initialize LL if it doesn't exist
	rec, ok := store.dict[arr[1].(string)]
	if !ok {
		// initialize Linked List
		rec = record{value: linkedList{
			length: 0},
			expiryTimestamp: -1}
		store.dict[arr[1].(string)] = rec
	}

	ll, ok := rec.value.(linkedList)
	if !ok {
		msg, _ := serializeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
		sendMsgToClient(conn, msg)
		return
	}

	// Insert one by one at the front
	// Inserting A, B, C results in LL of C -> B -> A
	for _, val := range arr[2:] {
		node := node{value: val.(string), next: ll.head, prev: nil}
		if ll.head != nil {
			ll.head.prev = &node
		}
		ll.head = &node
		ll.length++

		// The only time LPUSH affects the tail is when 1st item is inserted.
		if ll.length == 1 {
			ll.tail = &node
		}
		rec.value = ll
		store.dict[arr[1].(string)] = rec
	}

	msg, _, _ := serializeInteger(int64(ll.length))
	sendMsgToClient(conn, msg)
}

func handleLPop(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) < 2 || len(arr) > 3 {
		msg, _ := serializeSimpleError("ERR wrong number of arguments for 'lpop' command")
		sendMsgToClient(conn, msg)
		return
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	rec, ok := store.dict[arr[1].(string)]
	if !ok {
		msg := serializeNullArray()
		sendMsgToClient(conn, msg)
		return
	}

	ll, ok := rec.value.(linkedList)
	if !ok {
		msg, _ := serializeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
		sendMsgToClient(conn, msg)
		return
	}
	if ll.length == 0 {
		msg := serializeNullArray()
		sendMsgToClient(conn, msg)
		return
	}

	count := 1
	if len(arr) == 3 {
		parsedCount, err := strconv.ParseInt(arr[2].(string), 10, 64)
		if err != nil {
			msg, _ := serializeSimpleError("ERR internal error")
			sendMsgToClient(conn, msg)
			return
		}
		count = int(parsedCount)
	}

	resultArr := make([]string, count)
	for i := 0; i < int(count); i++ {
		val := ll.head.value
		ll.head = ll.head.next
		if ll.head != nil {
			ll.head.prev = nil
		}
		ll.length--

		// If there's only one node, it's both the head and tail
		if ll.length == 1 {
			ll.tail = ll.head
		}

		// Write back ll to map
		rec.value = ll
		store.dict[arr[1].(string)] = rec

		resultArr[i] = val
	}

	if len(resultArr) == 1 {
		msg := serializeBulkString(resultArr[0])
		sendMsgToClient(conn, msg)
		return
	}

	msg, _, _ := serializeStringArray(resultArr)
	sendMsgToClient(conn, msg)
}

func handleDecr(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) != 2 {
		msg, _ := serializeSimpleError("ERR wrong number of arguments for 'decr' command")
		sendMsgToClient(conn, msg)
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	rec, ok := store.dict[arr[1].(string)]
	if !ok {
		rec = record{"0", -1}
		store.dict[arr[1].(string)] = rec
	}
	val, ok := rec.value.(string)
	if !ok {
		msg, _ := serializeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
		sendMsgToClient(conn, msg)
		return
	}

	num, err := strconv.ParseInt(val, 10, 64)
	if err != nil || num <= math.MinInt64 {
		msg, _ := serializeSimpleError("ERR value is not an integer or out of range")
		sendMsgToClient(conn, msg)
		return
	}
	num--
	store.dict[arr[1].(string)] = record{fmt.Sprint(num), rec.expiryTimestamp}
	msg, _, _ := serializeInteger(num)
	sendMsgToClient(conn, msg)
}

func handleIncr(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) != 2 {
		msg, _ := serializeSimpleError("ERR wrong number of arguments for 'INCR' command")
		sendMsgToClient(conn, msg)
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	rec, ok := store.dict[arr[1].(string)]
	if !ok {
		rec = record{"0", -1}
		store.dict[arr[1].(string)] = rec
	}
	val, ok := rec.value.(string)
	if !ok {
		msg, _ := serializeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
		sendMsgToClient(conn, msg)
		return
	}

	num, err := strconv.ParseInt(val, 10, 64)
	if err != nil || num >= math.MaxInt64 {
		msg, _ := serializeSimpleError("ERR value is not an integer or out of range")
		sendMsgToClient(conn, msg)
		return
	}
	num++
	store.dict[arr[1].(string)] = record{fmt.Sprint(num), rec.expiryTimestamp}
	msg, _, _ := serializeInteger(num)
	sendMsgToClient(conn, msg)
}

func handleGet(arr []interface{}, conn net.Conn, store *dictionary) {
	if len(arr) != 2 {
		sendErrorToClient(conn, "ERR wrong number of arguments for 'get' command")
		return
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	key, ok := arr[1].(string)
	if !ok {
		sendErrorToClient(conn, "ERR wrong argument type")
		return
	}

	rec, ok := store.dict[key]
	if !ok {
		msg := serializeNullArray()
		sendMsgToClient(conn, msg)
		return
	}

	val, ok := rec.value.(string)
	if !ok {
		sendErrorToClient(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	// delete expired key and return nil, since the key doesn't exist anymore.
	if recordExpired(rec.expiryTimestamp) {
		delete(store.dict, key)

		msg := serializeNullArray()
		sendMsgToClient(conn, msg)
		return
	}

	msg := serializeBulkString(val)
	sendMsgToClient(conn, msg)
}

func sendMsgToClient(conn net.Conn, msg string) {
	if _, err := conn.Write(([]byte(msg))); err != nil {
		log.Println("Error writing to connection:", err)
	}
}

func sendErrorToClient(conn net.Conn, errMsg string) {
	msg, _ := serializeSimpleError(errMsg)
	sendMsgToClient(conn, msg)
}

func recordExpired(recordExpiration int64) bool {
	if recordExpiration == -1 {
		return false
	}
	return recordExpiration < time.Now().UnixMilli()
}

func parseExpiryTimestamp(exCmd string, exTime string) (int64, error) {
	var expiryTimestamp int64
	duration, err := strconv.ParseInt(exTime, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR expiration argument has to be an integer")
	}
	if duration <= 0 {
		return 0, fmt.Errorf("ERR expiration argument has to be positive")
	}

	switch cmd := strings.ToLower(exCmd); cmd {
	case "ex":
		expiryTimestamp = time.Now().UnixMilli() + duration*1000
	case "px":
		expiryTimestamp = time.Now().UnixMilli() + duration
	case "exat":
		expiryTimestamp = duration * 1000
	case "pxat":
		expiryTimestamp = duration
	default:
		return 0, fmt.Errorf("ERR unknown option for SET")
	}
	return expiryTimestamp, nil
}

func handleSet(arr []interface{}, conn net.Conn, store *dictionary) {
	// input validations
	if len(arr) != 3 && len(arr) != 5 {
		sendErrorToClient(conn, "ERR wrong number of arguments for 'set' command")
		return
	}

	// -1 denotes no expiration is set.
	var expiryTimestamp int64 = -1

	if len(arr) == 5 {
		exCmd, ok1 := arr[3].(string)
		exTime, ok2 := arr[4].(string)
		if !ok1 || !ok2 {
			log.Println("Invalid argument type")
			sendErrorToClient(conn, "ERR invalid argument type")
			return
		}

		var err error
		expiryTimestamp, err = parseExpiryTimestamp(exCmd, exTime)
		if err != nil {
			sendErrorToClient(conn, err.Error())
		}
	}

	store.mu.Lock()
	store.dict[arr[1].(string)] = record{value: arr[2].(string), expiryTimestamp: expiryTimestamp}
	store.mu.Unlock()

	msg, _ := serializeSimpleString("OK")
	sendMsgToClient(conn, msg)
}

// Locks the store while cleaning up - Not sure about the perf impact.
func activeKeyExpirer(store *dictionary) {
	for {
		expired := 0
		total := 0
		keys := make([]string, 0)
		store.mu.Lock()
		for k, v := range store.dict {
			if v.expiryTimestamp == -1 {
				continue
			}
			if recordExpired(v.expiryTimestamp) {
				expired++
				keys = append(keys, k)
			}
			total++

			if total >= activeExpireKeyLimit {
				break
			}
		}

		for _, k := range keys {
			delete(store.dict, k)
		}

		store.mu.Unlock()

		if float64(expired)/float64(total) < 0.25 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {
	// Sets up logging
	file, err := os.OpenFile("redis.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed to open a file, exiting.")
		os.Exit(1)
	}
	multiWriter := io.MultiWriter(file, os.Stdout)
	log.SetOutput(multiWriter)

	address := fmt.Sprintf("0.0.0.0:%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("Error listening:", err.Error())
	}
	defer listener.Close()

	log.Printf("Listening on %s...", address)

	store := newStore()

	go activeKeyExpirer(store)

	for {
		// Accept a connection
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Error accepting:", err.Error())
		}
		go handleRequest(conn, store)
	}
}
