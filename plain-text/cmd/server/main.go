package main

import (
	"bufio"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	bufferSize = 64 << 10
	batchSize  = 1024
)

type request struct {
	id    uint64
	valid bool
}

type batch struct {
	connection *connectionState
	requests   [batchSize]request
	count      int
	response   []byte
}

type connectionState struct {
	conn       net.Conn
	results    chan *batch
	pending    sync.WaitGroup
	writerDone chan struct{}
}

type serverState struct {
	data atomic.Uint64
	work chan *batch
	pool sync.Pool
}

func newServerState(workers int) *serverState {
	s := &serverState{work: make(chan *batch, workers*8)}
	s.pool.New = func() any {
		return &batch{response: make([]byte, 0, bufferSize)}
	}
	for range workers {
		go s.worker()
	}
	return s
}

func main() {
	listenAddr := flag.String("listen", ":9000", "TCP address to listen on")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "number of request batch workers")
	flag.Parse()
	if *workers < 1 {
		log.Fatal("workers must be at least 1")
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Printf("listening on %s with %d workers", listener.Addr(), *workers)

	state := newServerState(*workers)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go state.handle(conn)
	}
}

func (s *serverState) handle(conn net.Conn) {
	defer conn.Close()
	c := &connectionState{
		conn:       conn,
		results:    make(chan *batch, 8),
		writerDone: make(chan struct{}),
	}
	go s.writeResponses(c)

	reader := bufio.NewReaderSize(conn, bufferSize)
	b := s.getBatch(c)
	for {
		line, err := reader.ReadSlice('\n')
		if err == nil {
			id, valid := parseRequest(line)
			b.requests[b.count] = request{id: id, valid: valid}
			b.count++
			if b.count == batchSize || reader.Buffered() == 0 {
				s.dispatch(c, b)
				b = s.getBatch(c)
			}
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			for errors.Is(err, bufio.ErrBufferFull) {
				_, err = reader.ReadSlice('\n')
			}
			if err == nil {
				b.requests[b.count] = request{}
				b.count++
				s.dispatch(c, b)
				b = s.getBatch(c)
				continue
			}
		}
		if err != io.EOF {
			log.Printf("connection %s: %v", conn.RemoteAddr(), err)
		}
		break
	}
	if b.count > 0 {
		s.dispatch(c, b)
	} else {
		b.connection = nil
		s.pool.Put(b)
	}
	c.pending.Wait()
	close(c.results)
	<-c.writerDone
}

func (s *serverState) getBatch(c *connectionState) *batch {
	b := s.pool.Get().(*batch)
	b.connection = c
	b.count = 0
	b.response = b.response[:0]
	return b
}

func (s *serverState) dispatch(c *connectionState, b *batch) {
	c.pending.Add(1)
	s.work <- b
}

func (s *serverState) worker() {
	for b := range s.work {
		out := b.response[:0]
		for i := 0; i < b.count; i++ {
			req := b.requests[i]
			if !req.valid {
				out = append(out, "ERR invalid request\n"...)
				continue
			}
			value := s.data.Add(1)
			out = strconv.AppendUint(out, req.id, 10)
			out = append(out, ' ')
			out = strconv.AppendUint(out, value, 10)
			out = append(out, '\n')
		}
		b.response = out
		c := b.connection
		c.results <- b
		c.pending.Done()
	}
}

func (s *serverState) writeResponses(c *connectionState) {
	defer close(c.writerDone)
	writer := bufio.NewWriterSize(c.conn, bufferSize)
	failed := false
	for b := range c.results {
		if !failed {
			if _, err := writer.Write(b.response); err != nil {
				failed = true
			} else if err := writer.Flush(); err != nil {
				failed = true
			}
			if failed {
				c.conn.Close()
			}
		}
		b.connection = nil
		s.pool.Put(b)
	}
}

func parseRequest(line []byte) (uint64, bool) {
	if len(line) < 6 || line[0] != 'G' || line[1] != 'E' || line[2] != 'T' || line[3] != ' ' || line[len(line)-1] != '\n' {
		return 0, false
	}
	digits := line[4 : len(line)-1]
	if len(digits) > 0 && digits[len(digits)-1] == '\r' {
		digits = digits[:len(digits)-1]
	}
	if len(digits) == 0 {
		return 0, false
	}
	var id uint64
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		value := uint64(digit - '0')
		if id > (^uint64(0)-value)/10 {
			return 0, false
		}
		id = id*10 + value
	}
	return id, true
}
