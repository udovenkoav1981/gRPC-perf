package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type clientState struct {
	id        atomic.Uint64
	sent      atomic.Uint64
	responses atomic.Uint64
}

func main() {
	connections := flag.Int("connections", 1, "number of parallel TCP connections")
	flag.Parse()
	if flag.NArg() != 2 {
		log.Fatalf("usage: %s [-connections N] <server-address> <port>", os.Args[0])
	}
	if *connections < 1 {
		log.Fatal("connections must be at least 1")
	}
	port, err := strconv.Atoi(flag.Arg(1))
	if err != nil || port < 1 || port > 65535 {
		log.Fatal("port must be a number from 1 to 65535")
	}
	address := net.JoinHostPort(flag.Arg(0), strconv.Itoa(port))

	var state clientState
	go state.reportRPS()
	var running sync.WaitGroup
	for i := 0; i < *connections; i++ {
		running.Add(1)
		go func() {
			defer running.Done()
			for {
				if err := state.run(address); err != nil {
					log.Printf("%s: %v; reconnecting in 1s", address, err)
					time.Sleep(time.Second)
				}
			}
		}()
	}
	running.Wait()
}

func (c *clientState) run(address string) error {
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	writeErrors := make(chan error, 1)
	go func() {
		writeErrors <- c.writeRequests(conn)
		conn.Close()
	}()

	readErr := c.readResponses(conn)
	conn.Close()
	writeErr := <-writeErrors
	if writeErr != nil && (errors.Is(readErr, io.EOF) || errors.Is(readErr, net.ErrClosed)) {
		return writeErr
	}
	return readErr
}

func (c *clientState) writeRequests(conn net.Conn) error {
	buffer := make([]byte, 0, 64<<10)
	for {
		var count uint64
		for cap(buffer)-len(buffer) >= 32 {
			id := c.id.Add(1)
			buffer = append(buffer, 'G', 'E', 'T', ' ')
			buffer = strconv.AppendUint(buffer, id, 10)
			buffer = append(buffer, '\n')
			count++
		}
		for remaining := buffer; len(remaining) > 0; {
			n, err := conn.Write(remaining)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			remaining = remaining[n:]
		}
		c.sent.Add(count)
		buffer = buffer[:0]
	}
}

func (c *clientState) readResponses(conn net.Conn) error {
	reader := bufio.NewReaderSize(conn, 64<<10)
	var count uint64
	defer func() {
		if count > 0 {
			c.responses.Add(count)
		}
	}()
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			return err
		}
		id, ok := parseResponse(line)
		if !ok || id == 0 || id > c.id.Load() {
			return fmt.Errorf("invalid response: %q", line)
		}
		count++
		if count == 256 || reader.Buffered() == 0 {
			c.responses.Add(count)
			count = 0
		}
	}
}

func parseResponse(line []byte) (uint64, bool) {
	if len(line) < 4 || line[len(line)-1] != '\n' {
		return 0, false
	}
	space := bytes.IndexByte(line, ' ')
	if space < 1 {
		return 0, false
	}
	id, ok := parseUint(line[:space])
	if !ok {
		return 0, false
	}
	value := line[space+1 : len(line)-1]
	if len(value) > 0 && value[len(value)-1] == '\r' {
		value = value[:len(value)-1]
	}
	if _, ok := parseUint(value); !ok {
		return 0, false
	}
	return id, true
}

func parseUint(digits []byte) (uint64, bool) {
	if len(digits) == 0 {
		return 0, false
	}
	var value uint64
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		next := uint64(digit - '0')
		if value > (^uint64(0)-next)/10 {
			return 0, false
		}
		value = value*10 + next
	}
	return value, true
}

func (c *clientState) reportRPS() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastTime := time.Now()
	var lastSent, lastResponses uint64
	for range ticker.C {
		now := time.Now()
		sent := c.sent.Load()
		responses := c.responses.Load()
		seconds := now.Sub(lastTime).Seconds()
		fmt.Printf("sent/s=%.0f responses/s=%.0f total_responses=%d\n",
			float64(sent-lastSent)/seconds,
			float64(responses-lastResponses)/seconds,
			responses,
		)
		lastTime, lastSent, lastResponses = now, sent, responses
	}
}
