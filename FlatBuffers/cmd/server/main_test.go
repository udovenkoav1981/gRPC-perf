package main

import (
	"net"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"grpc-perf/flatbuffers/internal/wire"
)

func TestExchangeFragmentedFrames(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	if err := clientConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	state := &serverState{}
	done := make(chan struct{})
	go func() {
		state.handle(serverConn)
		close(done)
	}()
	defer func() {
		clientConn.Close()
		<-done
	}()

	const requests = 300
	builder := flatbuffers.NewBuilder(128)
	out := make([]byte, 0, requests*40)
	for id := uint64(1); id <= requests; id++ {
		out = wire.AppendRequest(out, builder, id)
	}
	sendErrors := make(chan error, 1)
	go func() {
		for offset := 0; offset < len(out); {
			end := min(offset+1+offset%7, len(out))
			if err := wire.WriteAll(clientConn, out[offset:end]); err != nil {
				sendErrors <- err
				return
			}
			offset = end
		}
		sendErrors <- nil
	}()

	reader := wire.NewReader(clientConn)
	for want := uint64(1); want <= requests; want++ {
		id, data, err := reader.ReadResponse()
		if err != nil {
			t.Fatal(err)
		}
		if id != want || data != want {
			t.Fatalf("response=(%d, %d), want (%d, %d)", id, data, want, want)
		}
	}
	if err := <-sendErrors; err != nil {
		t.Fatal(err)
	}
	if got := state.data.Load(); got != requests {
		t.Fatalf("server data = %d, want %d", got, requests)
	}
}
