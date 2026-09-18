package wire

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
	counter "grpc-perf/flatbuffers/generated/counter"
)

const (
	BufferSize   = 64 << 10
	MaxFrameSize = 4 << 10
)

type Reader struct {
	reader   *bufio.Reader
	request  counter.Request
	response counter.Response
}

func NewReader(r io.Reader) *Reader {
	return &Reader{reader: bufio.NewReaderSize(r, BufferSize)}
}

func (r *Reader) Buffered() int {
	return r.reader.Buffered()
}

func (r *Reader) ReadRequest() (uint64, error) {
	frame, err := r.peekFrame()
	if err != nil {
		return 0, err
	}
	position, err := tablePosition(frame, 1)
	if err != nil {
		return 0, err
	}
	r.request.Init(frame, position)
	id := r.request.Id()
	if id == 0 {
		return 0, fmt.Errorf("request ID is zero")
	}
	_, err = r.reader.Discard(len(frame))
	return id, err
}

func (r *Reader) ReadResponse() (uint64, uint64, error) {
	frame, err := r.peekFrame()
	if err != nil {
		return 0, 0, err
	}
	position, err := tablePosition(frame, 2)
	if err != nil {
		return 0, 0, err
	}
	r.response.Init(frame, position)
	id, data := r.response.Id(), r.response.Data()
	if id == 0 || data == 0 {
		return 0, 0, fmt.Errorf("response has zero ID or data")
	}
	_, err = r.reader.Discard(len(frame))
	return id, data, err
}

func (r *Reader) peekFrame() ([]byte, error) {
	header, err := r.reader.Peek(4)
	if err != nil {
		if err == io.EOF && len(header) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	size := binary.LittleEndian.Uint32(header)
	if size < 12 || size > MaxFrameSize {
		return nil, fmt.Errorf("invalid FlatBuffers frame size %d", size)
	}
	frame, err := r.reader.Peek(4 + int(size))
	if err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	}
	return frame, err
}

// tablePosition validates the table fields accessed by the generated getters.
func tablePosition(frame []byte, fields int) (flatbuffers.UOffsetT, error) {
	root := uint64(binary.LittleEndian.Uint32(frame[4:8]))
	position := uint64(4) + root
	if root < 4 || position+4 > uint64(len(frame)) {
		return 0, fmt.Errorf("invalid FlatBuffers root offset")
	}
	vtable := int64(position) - int64(int32(binary.LittleEndian.Uint32(frame[position:])))
	if vtable < 4 || vtable+4 > int64(len(frame)) {
		return 0, fmt.Errorf("invalid FlatBuffers vtable offset")
	}
	vtableLength := int(binary.LittleEndian.Uint16(frame[vtable:]))
	objectLength := int(binary.LittleEndian.Uint16(frame[vtable+2:]))
	if vtableLength < 4+fields*2 || int(vtable)+vtableLength > len(frame) ||
		objectLength < 4 || int(position)+objectLength > len(frame) {
		return 0, fmt.Errorf("invalid FlatBuffers table size")
	}
	for field := 0; field < fields; field++ {
		offset := int(binary.LittleEndian.Uint16(frame[int(vtable)+4+field*2:]))
		if offset < 4 || offset+8 > objectLength {
			return 0, fmt.Errorf("invalid FlatBuffers field offset")
		}
	}
	return flatbuffers.UOffsetT(position), nil
}

func AppendRequest(dst []byte, builder *flatbuffers.Builder, id uint64) []byte {
	builder.Reset()
	counter.RequestStart(builder)
	counter.RequestAddId(builder, id)
	root := counter.RequestEnd(builder)
	counter.FinishSizePrefixedRequestBuffer(builder, root)
	return append(dst, builder.FinishedBytes()...)
}

func AppendResponse(dst []byte, builder *flatbuffers.Builder, id, data uint64) []byte {
	builder.Reset()
	counter.ResponseStart(builder)
	counter.ResponseAddId(builder, id)
	counter.ResponseAddData(builder, data)
	root := counter.ResponseEnd(builder)
	counter.FinishSizePrefixedResponseBuffer(builder, root)
	return append(dst, builder.FinishedBytes()...)
}

func WriteAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
