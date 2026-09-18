package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"grpc-perf/flatbuffers/internal/wire"
)

var errStopWriting = errors.New("stop writing")

type frameWriter struct {
	batches [][]byte
}

func (w *frameWriter) Write(data []byte) (int, error) {
	if len(w.batches) == 3 {
		return 0, errStopWriting
	}
	w.batches = append(w.batches, bytes.Clone(data))
	return len(data), nil
}

func TestWriteRequestsBatchesFrames(t *testing.T) {
	var state clientState
	writer := &frameWriter{}
	if err := state.writeRequests(writer); !errors.Is(err, errStopWriting) {
		t.Fatalf("writeRequests error = %v, want %v", err, errStopWriting)
	}
	var nextID uint64 = 1
	for i, batch := range writer.batches {
		if len(batch) < wire.BufferSize {
			t.Fatalf("write %d: got %d bytes, want at least %d", i, len(batch), wire.BufferSize)
		}
		reader := wire.NewReader(bytes.NewReader(batch))
		var count int
		for {
			id, err := reader.ReadRequest()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil || id != nextID {
				t.Fatalf("write %d: request ID = %d, error = %v, want %d", i, id, err, nextID)
			}
			nextID++
			count++
		}
		if count < 2 {
			t.Fatalf("write %d: got %d requests, want multiple", i, count)
		}
	}
}
