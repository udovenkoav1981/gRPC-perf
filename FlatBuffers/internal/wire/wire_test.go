package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestReaderRejectsMalformedFrames(t *testing.T) {
	tooLarge := make([]byte, 4)
	binary.LittleEndian.PutUint32(tooLarge, MaxFrameSize+1)
	truncated := make([]byte, 8)
	binary.LittleEndian.PutUint32(truncated, 12)
	badRoot := make([]byte, 16)
	binary.LittleEndian.PutUint32(badRoot, 12)

	for _, test := range []struct {
		name      string
		input     []byte
		truncated bool
	}{
		{"partial prefix", []byte{1, 2}, true},
		{"partial body", truncated, true},
		{"oversized body", tooLarge, false},
		{"invalid root", badRoot, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewReader(bytes.NewReader(test.input)).ReadRequest()
			if err == nil {
				t.Fatal("expected malformed frame error")
			}
			if test.truncated && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}
