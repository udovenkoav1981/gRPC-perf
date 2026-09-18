package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConcurrentConnections(t *testing.T) {
	const connections = 4
	const requestsPerConnection = 300
	total := connections * requestsPerConnection

	state := newServerState(4)
	values := make(chan uint64, total)
	errors := make(chan error, connections)
	var clients, handlers sync.WaitGroup

	for connection := 0; connection < connections; connection++ {
		serverConn, clientConn := net.Pipe()
		deadline := time.Now().Add(10 * time.Second)
		serverConn.SetDeadline(deadline)
		clientConn.SetDeadline(deadline)
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			state.handle(serverConn)
		}()
		clients.Add(1)
		go func(connection int) {
			defer clients.Done()
			defer clientConn.Close()
			errors <- exchange(clientConn, connection*requestsPerConnection, requestsPerConnection, values)
		}(connection)
	}

	clients.Wait()
	handlers.Wait()
	close(values)
	for i := 0; i < connections; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if got := state.data.Load(); got != uint64(total) {
		t.Fatalf("server counter = %d, want %d", got, total)
	}
	seen := make([]bool, total+1)
	for value := range values {
		if value < 1 || value > uint64(total) || seen[value] {
			t.Fatalf("invalid or duplicate counter value %d", value)
		}
		seen[value] = true
	}
	for value := 1; value <= total; value++ {
		if !seen[value] {
			t.Fatalf("missing counter value %d", value)
		}
	}
}

func exchange(conn net.Conn, firstID, count int, values chan<- uint64) error {
	writeErrors := make(chan error, 1)
	go func() {
		writer := bufio.NewWriter(conn)
		for i := 0; i < count; i++ {
			if _, err := fmt.Fprintf(writer, "GET %d\n", firstID+i); err != nil {
				writeErrors <- err
				return
			}
		}
		if _, err := writer.WriteString("BAD\n"); err != nil {
			writeErrors <- err
			return
		}
		writeErrors <- writer.Flush()
	}()

	reader := bufio.NewReader(conn)
	seenIDs := make([]bool, count)
	invalid := 0
	for i := 0; i < count+1; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if line == "ERR invalid request\n" {
			invalid++
			continue
		}
		var id, value uint64
		if _, err := fmt.Sscanf(line, "%d %d\n", &id, &value); err != nil {
			return fmt.Errorf("invalid response %q: %w", line, err)
		}
		if id < uint64(firstID) || id >= uint64(firstID+count) || seenIDs[int(id)-firstID] {
			return fmt.Errorf("invalid or duplicate request ID %d", id)
		}
		seenIDs[int(id)-firstID] = true
		values <- value
	}
	if err := <-writeErrors; err != nil {
		return err
	}
	if invalid != 1 {
		return fmt.Errorf("invalid request responses = %d, want 1", invalid)
	}
	for id, seen := range seenIDs {
		if !seen {
			return fmt.Errorf("missing request ID %d", firstID+id)
		}
	}
	return nil
}
