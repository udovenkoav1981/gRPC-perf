package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"sync/atomic"

	flatbuffers "github.com/google/flatbuffers/go"
	"grpc-perf/flatbuffers/internal/wire"
)

type serverState struct {
	data atomic.Uint64
}

func (s *serverState) handle(conn net.Conn) {
	defer conn.Close()
	reader := wire.NewReader(conn)
	builder := flatbuffers.NewBuilder(128)
	out := make([]byte, 0, wire.BufferSize+64)
	for {
		id, err := reader.ReadRequest()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(out) > 0 {
					_ = wire.WriteAll(conn, out)
				}
			} else {
				log.Printf("%s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		out = wire.AppendResponse(out, builder, id, s.data.Add(1))
		if len(out) >= wire.BufferSize || reader.Buffered() == 0 {
			if err := wire.WriteAll(conn, out); err != nil {
				log.Printf("%s: %v", conn.RemoteAddr(), err)
				return
			}
			out = out[:0]
		}
	}
}

func main() {
	listenAddr := flag.String("listen", ":9002", "TCP address to listen on")
	flag.Parse()
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Printf("listening on %s", listener.Addr())
	state := &serverState{}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go state.handle(conn)
	}
}
