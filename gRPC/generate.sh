#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
protoc \
  -I . \
  --plugin="protoc-gen-go=$(go tool -n protoc-gen-go)" \
  --plugin="protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc)" \
  --plugin="protoc-gen-go-vtproto=$(go tool -n protoc-gen-go-vtproto)" \
  --go_out=paths=source_relative:. \
  --go-grpc_out=paths=source_relative:. \
  --go-vtproto_out=paths=source_relative:. \
  --go-vtproto_opt=features=marshal+unmarshal+size+pool \
  --go-vtproto_opt=pool=grpc-perf/grpc/proto.Request \
  --go-vtproto_opt=pool=grpc-perf/grpc/proto.Response \
  proto/counter.proto
