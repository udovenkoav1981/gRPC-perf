package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/test/bufconn"
	pb "grpc-perf/grpc/proto"
)

func TestExchangeConcurrentStreams(t *testing.T) {
	if _, ok := encoding.GetCodec("proto").(vtgrpc.Codec); !ok {
		t.Fatal("vtprotobuf codec is not registered")
	}

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	state := &counterServer{}
	pb.RegisterCounterServer(server, state)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		listener.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dials atomic.Int32
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			dials.Add(1)
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewCounterClient(connection)

	const streams = 2
	const requestsPerStream = 100
	values := make(chan uint64, streams*requestsPerStream)
	errors := make(chan error, streams)
	var running sync.WaitGroup
	for streamIndex := 0; streamIndex < streams; streamIndex++ {
		running.Add(1)
		go func(streamIndex int) {
			defer running.Done()
			errors <- exchange(ctx, client, streamIndex*requestsPerStream, requestsPerStream, values)
		}(streamIndex)
	}
	running.Wait()
	close(values)
	for i := 0; i < streams; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if got := state.data.Load(); got != streams*requestsPerStream {
		t.Fatalf("server data = %d, want %d", got, streams*requestsPerStream)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("transport dials = %d, want 1", got)
	}
	seenValues := make([]bool, streams*requestsPerStream+1)
	for value := range values {
		if value == 0 || value > streams*requestsPerStream || seenValues[value] {
			t.Fatalf("invalid or duplicate data value %d", value)
		}
		seenValues[value] = true
	}
}

func exchange(ctx context.Context, client pb.CounterClient, firstID, count int, values chan<- uint64) error {
	stream, err := client.Exchange(ctx)
	if err != nil {
		return err
	}
	sendErrors := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			if err := stream.SendMsg(&pb.Request{Id: uint64(firstID + i + 1)}); err != nil {
				sendErrors <- err
				return
			}
		}
		sendErrors <- stream.CloseSend()
	}()

	response := pb.ResponseFromVTPool()
	defer response.ReturnToVTPool()
	seenIDs := make([]bool, count)
	for i := 0; i < count; i++ {
		response.ResetVT()
		if err := stream.RecvMsg(response); err != nil {
			return err
		}
		if response.Id <= uint64(firstID) || response.Id > uint64(firstID+count) {
			return fmt.Errorf("unexpected response ID %d", response.Id)
		}
		index := int(response.Id) - firstID - 1
		if seenIDs[index] {
			return fmt.Errorf("duplicate response ID %d", response.Id)
		}
		seenIDs[index] = true
		values <- response.Data
	}
	if err := <-sendErrors; err != nil {
		return err
	}
	response.ResetVT()
	if err := stream.RecvMsg(response); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected stream EOF, got %v", err)
	}
	return nil
}
