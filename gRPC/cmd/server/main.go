package main

import (
	"flag"
	"io"
	"log"
	"net"
	"sync/atomic"

	"google.golang.org/grpc"
	_ "grpc-perf/grpc/internal/codec"
	pb "grpc-perf/grpc/proto"
)

const queueSize = 1024

type counterServer struct {
	pb.UnimplementedCounterServer
	data atomic.Uint64
}

func (s *counterServer) Exchange(stream pb.Counter_ExchangeServer) error {
	ids := make(chan uint64, queueSize)
	receiveErrors := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	go func() {
		defer close(ids)
		request := pb.RequestFromVTPool()
		defer request.ReturnToVTPool()
		for {
			request.ResetVT()
			if err := stream.RecvMsg(request); err != nil {
				receiveErrors <- err
				return
			}
			select {
			case ids <- request.Id:
			case <-done:
				return
			case <-stream.Context().Done():
				receiveErrors <- stream.Context().Err()
				return
			}
		}
	}()

	for id := range ids {
		value := s.data.Add(1)
		// gRPC may retain a sent message for tracing, so each send owns its value.
		if err := stream.SendMsg(&pb.Response{Id: id, Data: value}); err != nil {
			return err
		}
	}
	if err := <-receiveErrors; err != io.EOF {
		return err
	}
	return nil
}

func main() {
	listenAddr := flag.String("listen", ":9001", "TCP address to listen on")
	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	server := grpc.NewServer(
		grpc.ReadBufferSize(64<<10),
		grpc.WriteBufferSize(64<<10),
	)
	pb.RegisterCounterServer(server, &counterServer{})
	log.Printf("listening on %s", listener.Addr())
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
