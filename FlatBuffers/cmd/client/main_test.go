package main

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"grpc-perf/flatbuffers/internal/wire"
)

type recordingWriter struct {
	writes     [][]byte
	afterWrite func()
}

type failingWriter struct{}

var errWrite = errors.New("write failed")

func (failingWriter) Write([]byte) (int, error) {
	return 0, errWrite
}

func (w *recordingWriter) Write(data []byte) (int, error) {
	w.writes = append(w.writes, bytes.Clone(data))
	if w.afterWrite != nil {
		w.afterWrite()
	}
	return len(data), nil
}

func serializedRequestWithID(id uint64) *serializedRequest {
	return &serializedRequest{frame: wire.AppendRequest(nil, flatbuffers.NewBuilder(128), id)}
}

func TestWriteQueuedRequestsDrainsAvailableFrames(t *testing.T) {
	const count = 5000
	requests := make(chan *serializedRequest, count)
	for id := uint64(1); id <= count; id++ {
		requests <- serializedRequestWithID(id)
	}
	close(requests)
	writer := &recordingWriter{}
	if err := writeQueuedRequests(writer, requests, &sync.Pool{}); err != nil {
		t.Fatal(err)
	}
	if len(writer.writes) < 2 {
		t.Fatalf("got %d writes, want multiple writes for a full buffer", len(writer.writes))
	}
	var nextID uint64 = 1
	for i, data := range writer.writes {
		reader := wire.NewReader(bytes.NewReader(data))
		for {
			id, err := reader.ReadRequest()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil || id != nextID {
				t.Fatalf("write %d: got request %d, error %v; want %d", i, id, err, nextID)
			}
			nextID++
		}
	}
	if nextID != count+1 {
		t.Fatalf("got %d requests, want %d", nextID-1, count)
	}
}

func TestWriteQueuedRequestsFlushesWhenQueueEmpties(t *testing.T) {
	requests := make(chan *serializedRequest, 1)
	requests <- serializedRequestWithID(1)
	writer := &recordingWriter{}
	writer.afterWrite = func() {
		if len(writer.writes) == 1 {
			requests <- serializedRequestWithID(2)
			close(requests)
		}
	}
	done := make(chan error, 1)
	go func() { done <- writeQueuedRequests(writer, requests, &sync.Pool{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not flush after the queue became empty")
	}
	if len(writer.writes) != 2 {
		t.Fatalf("got %d writes, want one write for each arrival", len(writer.writes))
	}
	for i, data := range writer.writes {
		reader := wire.NewReader(bytes.NewReader(data))
		id, err := reader.ReadRequest()
		if err != nil || id != uint64(i+1) {
			t.Fatalf("write %d: got ID %d, error %v", i, id, err)
		}
		if _, err := reader.ReadRequest(); !errors.Is(err, io.EOF) {
			t.Fatalf("write %d has extra data: %v", i, err)
		}
	}
}

func TestWriteRequestsStopsProducerOnWriteError(t *testing.T) {
	var state clientState
	done := make(chan error, 1)
	go func() { done <- state.writeRequests(failingWriter{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, errWrite) {
			t.Fatalf("got error %v, want %v", err, errWrite)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer or producer did not stop after a write error")
	}
}
