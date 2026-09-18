package codec

import (
	vtgrpc "github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
)

func init() {
	encoding.RegisterCodec(vtgrpc.Codec{})
}
