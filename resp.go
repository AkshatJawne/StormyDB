package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// Value represents a Redis-like data type with multiple possible types.
type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

// RESP handles the parsing of RESP (Redis Serialization Protocol) messages.
type RESP struct {
	reader *bufio.Reader
}

// NewRESP creates a new RESP instance with the given io.Reader.
func NewRESP(rd io.Reader) *RESP {
	return &RESP{reader: bufio.NewReader(rd)}
}

// readLine reads a line of input, terminated by CRLF, and trims the trailing CRLF.
func (r *RESP) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

// readInteger reads an integer from the RESP input.
func (r *RESP) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

// Read parses a single RESP value from the input.
func (r *RESP) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// readArray parses an array RESP value from the input.
func (r *RESP) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	// Read the length of the array.
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// Parse each element in the array.
	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.array = append(v.array, val)
	}

	return v, nil
}

// readBulk parses a bulk string RESP value from the input.
func (r *RESP) readBulk() (Value, error) {
	v := Value{}

	v.typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.bulk = string(bulk)

	// Read the trailing CRLF.
	r.readLine()

	return v, nil
}

// Marshal serializes a Value into its RESP representation.
func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}

// marshalString serializes a simple string.
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshalBulk serializes a bulk string.
func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshalArray serializes an array.
func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

// marshallError serializes an error message.
func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshallNull serializes a null value.
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

// RESPWriter writes RESP values to an io.Writer.
type RESPWriter struct {
	writer io.Writer
}

// NewRESPWriter creates a new RESPWriter instance.
func NewRESPWriter(w io.Writer) *RESPWriter {
	return &RESPWriter{writer: w}
}

// Write writes a serialized RESP value to the output.
func (w *RESPWriter) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
