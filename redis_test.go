package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	// Setup the server
	go main()

	// Run the tests
	code := m.Run()

	// Exit
	os.Exit(code)
}

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
		input int64
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

func TestDeserializeNullOrArray(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  []interface{}
	}{
		// the table itself
		{"Should deserialize array", "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n", []interface{}{"echo", "hello world"}},
		{"Should deserialize multi-type array", "*4\r\n:+123\r\n-ERR\r\n+OK\r\n$-1\r\n", []interface{}{123, "ERR", "OK", nil}},
		{"Should deserialize empty array", "*0\r\n", []interface{}{}},
		{"Should deserialize nil array", "*-1\r\n", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ans, _, err := deserializeNullOrArray(test.input)
			if err != nil {
				t.Errorf("%v", err)
			}

			if test.want == nil && ans == nil {
				return
			}

			if ans == nil {
				t.Errorf("Unexpected null array.")
			}

			if len(ans.([]interface{})) != len(test.want) {
				t.Errorf("Got array of len '%d' but expected '%d'.", len(ans.([]interface{})), len(test.want))
			}
			for i := 0; i < len(ans.([]interface{})); i++ {
				if ans.([]interface{})[i] != test.want[i] {
					t.Errorf("Got '%s' but expected '%s'.", ans, test.want)
				}
			}
		})
	}
}

func TestSetGet(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		t.Error(err)
	}

	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if val != "value" {
		t.Errorf("Got '%s' but expected '%s'", val, "value")
	}
}

func TestSetGetEx(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	// Set expriration of 2 seconds
	err := rdb.Set(ctx, "key", "value", 2*time.Second).Err()
	if err != nil {
		t.Error(err)
	}

	// Confirm that value was set
	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if val != "value" {
		t.Errorf("Got '%s' but expected '%s'", val, "value")
	}

	// Wait for 3 seconds to confirm that value was reset.
	time.Sleep(3 * time.Second)
	// Confirm that value was set
	val, err = rdb.Get(ctx, "key").Result()
	if err == nil {
		t.Error("Expected error to signal key expired.")
		return
	}
	if err.Error() != "redis: nil" {
		t.Error("Expected nil but got ")
	}
}

func TestSetGetPx(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Set expriration of 500ms
	err := rdb.Set(ctx, "key", "value", 500*time.Millisecond).Err()
	if err != nil {
		t.Error(err)
	}

	// Confirm that value was set
	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if val != "value" {
		t.Errorf("Got '%s' but expected '%s'", val, "value")
	}

	// Wait for 500ms + buffer to confirm that value was reset.
	time.Sleep(700 * time.Millisecond)
	// Confirm that value was set
	val, err = rdb.Get(ctx, "key").Result()
	if err == nil {
		t.Error("Expected error to signal key expired.")
		return
	}
	if err.Error() != "redis: nil" {
		t.Error("Expected nil")
	}
}

func TestSetGetExat(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Set expriration 2 seconds from now.
	expireUnixTime := int64(time.Now().Unix() + 2)

	// Using EXAT, Go client does not have helper for EXAT.
	_, err := rdb.Do(ctx, "SET", "key", "value", "EXAT", expireUnixTime).Result()
	if err != nil {
		t.Error(err)
	}

	// Confirm that value was set
	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if val != "value" {
		t.Errorf("Got '%s' but expected '%s'", val, "value")
	}

	// Confirm that value was reset
	time.Sleep(2 * time.Second)
	val, err = rdb.Get(ctx, "key").Result()
	if err == nil {
		t.Error("Expected error to signal key expired.")
		return
	}
	if err.Error() != "redis: nil" {
		t.Error("Expected nil")
	}
}

func TestSetGetPxat(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Set expriration 500ms from now.
	expireUnixTime := int64(time.Now().UnixMilli() + 500)

	// Go client does not have helper for PXAT.
	_, err := rdb.Do(ctx, "SET", "key", "value", "PXAT", expireUnixTime).Result()
	if err != nil {
		t.Error(err)
	}

	// Confirm that value was set.
	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if val != "value" {
		t.Errorf("Got '%s' but expected '%s'", val, "value")
	}

	// Confirm that value was reset
	time.Sleep(500 * time.Millisecond)
	val, err = rdb.Get(ctx, "key").Result()
	if err == nil {
		t.Error("Expected error to signal key expired.")
		return
	}
	if err.Error() != "redis: nil" {
		t.Errorf("Expected nil but got '%s'", err)
	}
}

func TestGetNonExistant(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := rdb.Get(ctx, "keyDoesNotExist").Result()
	if err != nil {
		if err != redis.Nil {
			t.Error("Key exists when it shouldn't.")
		}
	} else {
		t.Error("Expected an error, but didn't get any.")
	}
}

func TestPing(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	res, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Error(err)
	}

	if res != "PONG" {
		t.Errorf("Expected 'PONG' but got '%s'", res)
	}
}

func TestEcho(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	res, err := rdb.Echo(ctx, "Hello World!").Result()
	if err != nil {
		t.Error(err)
	}

	if res != "Hello World!" {
		t.Errorf("Expected 'PONG' but got '%s'", res)
	}
}

func TestExistsNoSuchKey(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	res, err := rdb.Exists(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if res != 0 {
		t.Errorf("Expected 0 keys but got '%d'", res)
	}
}

func TestExists(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		t.Error(err)
	}
	err = rdb.Set(ctx, "anotherKey", "anotherValue", 0).Err()
	if err != nil {
		t.Error(err)
	}

	res, err := rdb.Exists(ctx, "key", "anotherKey").Result()
	if err != nil {
		t.Error(err)
	}
	if res != 2 {
		t.Errorf("Expected 2 keys but got '%d'", res)
	}
}

func TestDel(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		t.Error(err)
	}

	res, err := rdb.Del(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if res != 1 {
		t.Errorf("Expected 1 but got '%d'", res)
	}
}

func TestIncr(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "key", "1", 0).Err()
	if err != nil {
		t.Error(err)
	}

	res, err := rdb.Incr(ctx, "key").Result()
	if err != nil {
		t.Error(err)
	}
	if res != 2 {
		t.Errorf("Expected 2 but got '%d'", res)
	}
}

func TestIncrOutOfRange(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Start with maximum value that is allowed.
	err := rdb.Set(ctx, "key", fmt.Sprint(math.MaxInt64), 0).Err()
	if err != nil {
		t.Error(err)
	}

	// Increment would overflow - we want to prevent that
	_, err = rdb.Incr(ctx, "key").Result()
	if err == nil {
		t.Error("Expected an error bur didn't get any")
	}
}

func TestIncrKeyDoesNotExist(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Using new key, since we are sharing the store among multiple tests, so key might exist.
	res, err := rdb.Incr(ctx, "thisKeyDoesNotExist").Result()
	if err != nil {
		t.Error(err)
	}
	if res != 1 {
		t.Errorf("Expected 1 but got '%d'", res)
	}
}

func TestIncrNonNumeric(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "nonnumeric", "someString", 0).Err()
	if err != nil {
		t.Error(err)
	}

	_, err = rdb.Incr(ctx, "nonnumeric").Result()
	if err == nil {
		t.Error("Expected an error")
		return
	}
	if err.Error() != "ERR value is not an integer or out of range" {
		t.Errorf("Expected 'must be integer' but got '%s'", err.Error())
	}
}

func TestDecrKeyDoesNotExist(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Using new key, since we are sharing the store among multiple tests, so key might exist.
	res, err := rdb.Decr(ctx, "thisKeyDecrDoesNotExist").Result()
	if err != nil {
		t.Error(err)
	}
	if res != -1 {
		t.Errorf("Expected -1 but got '%d'", res)
	}
}

func TestDecrNonNumeric(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "nonnumericdecr", "someString", 0).Err()
	if err != nil {
		t.Error(err)
	}

	_, err = rdb.Decr(ctx, "nonnumericdecr").Result()
	if err == nil {
		t.Error("Expected an error")
		return
	}
	if err.Error() != "ERR value is not an integer or out of range" {
		t.Errorf("Expected 'must be integer' but got '%s'", err.Error())
	}
}

func TestDecrOutOfRange(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Start with maximum value that is allowed.
	err := rdb.Set(ctx, "keyTestDecrOutOfRange", fmt.Sprint(math.MinInt64), 0).Err()
	if err != nil {
		t.Error(err)
	}

	// Increment would udnerflow - we want to prevent that
	_, err = rdb.Decr(ctx, "keyTestDecrOutOfRange").Result()
	if err == nil {
		t.Error("Expected an error bur didn't get any")
	}
}

func TestDecr(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "keyDecr", "1", 0).Err()
	if err != nil {
		t.Error(err)
	}

	res, err := rdb.Decr(ctx, "keyDecr").Result()
	if err != nil {
		t.Error(err)
	}
	if res != 0 {
		t.Errorf("Expected 0 but got '%d'", res)
	}
}

func TestLPush(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	count, err := rdb.LPush(ctx, "arrKey", "firstEntry").Result()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("Expected '1' but got '%d'", count)
	}
}

func TestLPushExistingKeyWrongType(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "NotALinkedList", "foo", 0).Err()
	if err != nil {
		t.Error(err)
	}

	_, err = rdb.LPush(ctx, "NotALinkedList", "firstEntry").Result()
	if err == nil {
		t.Fatal(err)
	}
	if err.Error() != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("Expected 'WRONGTYPE' but got '%s'", err.Error())
	}
}

func TestLPushMultiple(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	count, err := rdb.LPush(ctx, "arrKey1", "firstEntry", "second", "third").Result()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("Expected '3' but got '%d'", count)
	}
}

func TestLPushLPop(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	count, err := rdb.LPush(ctx, "arrKey2", "first", "second", "third").Result()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("Expected '3' but got '%d'", count)
	}

	val, err := rdb.LPop(ctx, "arrKey2").Result()

	if val != "third" {
		t.Errorf("Expected 'third' but got '%s'", val)
	}

	val, err = rdb.LPop(ctx, "arrKey2").Result()

	if val != "second" {
		t.Errorf("Expected 'second' but got '%s'", val)
	}

	val, err = rdb.LPop(ctx, "arrKey2").Result()

	if val != "first" {
		t.Errorf("Expected 'first' but got '%s'", val)
	}
}

func TestLPushLPopNonExist(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := rdb.LPop(ctx, "arrKeyDoesNotExistPop").Result()
	if err == nil {
		t.Fatal("Expected nil")
	}
	if err.Error() != "redis: nil" {
		t.Errorf("Expected 'redis: nil' but got '%s'", err.Error())
	}
}
