package main

import (
	"strconv"
	"sync"
)

// Handlers is a map of commands to their corresponding handler functions.
var Handlers = map[string]func([]Value) Value{
	"PING":    handlePing,
	"SET":     handleSet,
	"GET":     handleGet,
	"DEL":     handleDel,
	"EXISTS":  handleExists,
	"INCR":    handleIncr,
	"HSET":    handleHSet,
	"HGET":    handleHGet,
	"HGETALL": handleHGetAll,
}

// handlePing handles the "PING" command and optionally echoes the input.
func handlePing(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// Global storage for SET command.
var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

// handleSet handles the "SET" command for storing key-value pairs.
func handleSet(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// handleGet handles the "GET" command to retrieve values by key.
func handleGet(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// handleDel handles the "DEL" command to delete one or more keys.
func handleDel(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'del' command"}
	}

	deletedCount := 0
	SETsMu.Lock()
	for _, arg := range args {
		key := arg.bulk
		if _, exists := SETs[key]; exists {
			delete(SETs, key)
			deletedCount++
		}
	}
	SETsMu.Unlock()

	return Value{typ: "integer", num: deletedCount}
}

// handleExists handles the "EXISTS" command to check if one or more keys exist.
func handleExists(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'exists' command"}
	}

	existsCount := 0
	SETsMu.RLock()
	for _, arg := range args {
		key := arg.bulk
		if _, exists := SETs[key]; exists {
			existsCount++
		}
	}
	SETsMu.RUnlock()

	return Value{typ: "integer", num: existsCount}
}

// handleIncr handles the "INCR" command to increment the integer value of a key by 1.
func handleIncr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	defer SETsMu.Unlock()

	value, ok := SETs[key]
	if !ok {
		SETs[key] = "1"
		return Value{typ: "integer", num: 1}
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer"}
	}

	intValue++
	SETs[key] = strconv.Itoa(intValue)

	return Value{typ: "integer", num: intValue}
}

// Global storage for HSET command.
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

// handleHSet handles the "HSET" command for storing field-value pairs in a hash.
func handleHSet(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// handleHGet handles the "HGET" command to retrieve a value by hash and field.
func handleHGet(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// handleHGetAll handles the "HGETALL" command to retrieve all fields and values in a hash.
func handleHGetAll(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: values}
}
