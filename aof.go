package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

// AOF (Append-Only File) handles the append-only file for data persistence.
type AOF struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

// NewAOF initializes a new AOF file at the specified path.
func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// Start a goroutine to periodically sync the AOF file to disk.
	go func() {
		for {
			time.Sleep(time.Second)
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
		}
	}()

	return aof, nil
}

// Close safely closes the AOF file.
func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

// Write appends a serialized Value to the AOF file.
func (aof *AOF) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

// Read replays the commands stored in the AOF file.
func (aof *AOF) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// Reset the file pointer to the beginning.
	_, err := aof.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	reader := NewRESP(aof.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fn(value)
	}

	return nil
}
