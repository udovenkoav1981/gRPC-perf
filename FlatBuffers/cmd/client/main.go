package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"grpc-perf/flatbuffers/internal/wire"
)

type clientState struct {
	id        atomic.Uint64
	responses atomic.Uint64
}

type serializedRequest struct {
	frame []byte
}

const (
	requestQueueSize = 4096
	minBatchSize     = 1000
)

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
	requests := make(chan *serializedRequest, requestQueueSize)
	pool := &sync.Pool{New: func() any {
		return &serializedRequest{frame: make([]byte, 0, 128)}
	}}
	stop := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		builder := flatbuffers.NewBuilder(128)
		for {
			select {
			case <-stop:
				return
			default:
			}
			request := pool.Get().(*serializedRequest)
			request.frame = wire.AppendRequest(request.frame[:0], builder, c.id.Add(1))
			select {
			case requests <- request:
			case <-stop:
				pool.Put(request)
				return
			}
		}
	}()
	err := writeQueuedRequests(writer, requests, pool)
	close(stop)
	<-producerDone
	return err
}

func writeQueuedRequests(writer io.Writer, requests <-chan *serializedRequest, pool *sync.Pool) error {
	buffer := bufio.NewWriterSize(writer, wire.BufferSize)
	for {
		request, ok := <-requests
		if !ok {
			return nil
		}
		yielded := false
		for {
			_, err := buffer.Write(request.frame)
			request.frame = request.frame[:0]
			pool.Put(request)
			if err != nil {
				return err
			}
			select {
			case request, ok = <-requests:
				if !ok {
					return buffer.Flush()
				}
				continue
			default:
			}
			if !yielded && buffer.Buffered() < minBatchSize {
				runtime.Gosched()
				yielded = true
				select {
				case request, ok = <-requests:
					if !ok {
						return buffer.Flush()
					}
					continue
				default:
				}
			}
			if err := buffer.Flush(); err != nil {
				return err
			}
			break
		}
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
