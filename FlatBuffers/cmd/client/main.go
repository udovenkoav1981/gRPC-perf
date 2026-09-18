package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"grpc-perf/flatbuffers/internal/wire"
)

type clientState struct {
	id        atomic.Uint64
	responses atomic.Uint64
}

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		log.Fatalf("usage: %s <server-address> <port>", os.Args[0])
	}
	port, err := strconv.Atoi(flag.Arg(1))
	if err != nil || port < 1 || port > 65535 {
		log.Fatal("port must be a number from 1 to 65535")
	}
	address := net.JoinHostPort(flag.Arg(0), strconv.Itoa(port))
	var state clientState
	go state.reportRPS()
	for {
		if err := state.run(address); err != nil {
			log.Printf("%s: %v; reconnecting in 1s", address, err)
			time.Sleep(time.Second)
		}
	}
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

func (c *clientState) writeRequests(writer io.Writer) error {
	builder := flatbuffers.NewBuilder(128)
	out := make([]byte, 0, wire.BufferSize+64)
	for {
		for len(out) < wire.BufferSize {
			out = wire.AppendRequest(out, builder, c.id.Add(1))
		}
		if err := wire.WriteAll(writer, out); err != nil {
			return err
		}
		out = out[:0]
	}
}

func (c *clientState) readResponses(conn net.Conn) error {
	reader := wire.NewReader(conn)
	var count uint64
	defer func() {
		if count > 0 {
			c.responses.Add(count)
		}
	}()
	for {
		id, _, err := reader.ReadResponse()
		if err != nil {
			return err
		}
		if id > c.id.Load() {
			return fmt.Errorf("invalid response ID %d", id)
		}
		count++
		if count == 256 {
			c.responses.Add(count)
			count = 0
		}
	}
}

func (c *clientState) reportRPS() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastTime := time.Now()
	var lastSent, lastResponses uint64
	for range ticker.C {
		now := time.Now()
		sent := c.id.Load()
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
