package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :5000")

	// Start a TCP server listening on port 5000.
	listener, err := net.Listen("tcp", ":5000")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	// Create an Append-Only File (AOF) for persistence.
	aof, err := NewAOF("database.aof")
	if err != nil {
		fmt.Println("Error initializing AOF:", err)
		return
	}
	defer aof.Close()

	// Replay commands from the AOF to restore state.
	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command during AOF replay:", command)
			return
		}

		// Execute the handler to restore state.
		handler(args)
	})

	for {
		// Accept a new client connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		// Handle the client in a new goroutine.
		go handleClient(conn, aof)
	}
}

// handleClient processes commands from a single client connection.
func handleClient(conn net.Conn, aof *AOF) {
	defer conn.Close()

	resp := NewRESP(conn)
	writer := NewRESPWriter(conn)

	for {
		// Read a command from the client.
		value, err := resp.Read()
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Error reading command:", err)
			}
			return
		}

		// Validate that the command is an array.
		if value.typ != "array" || len(value.array) == 0 {
			fmt.Println("Invalid request: expected non-empty array")
			writer.Write(Value{typ: "error", str: "ERR invalid request format"})
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// Find the command handler.
		handler, ok := Handlers[command]
		if !ok {
			writer.Write(Value{typ: "error", str: "ERR unknown command: " + command})
			continue
		}

		// For write commands, persist to AOF.
		if command == "SET" || command == "DEL" || command == "HSET" || command == "INCR" {
			err = aof.Write(value)
			if err != nil {
				fmt.Println("Error writing to AOF:", err)
				writer.Write(Value{typ: "error", str: "ERR internal server error"})
				continue
			}
		}

		// Execute the command and write the response.
		result := handler(args)
		writer.Write(result)
	}
}
