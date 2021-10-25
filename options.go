package grpc

import (
	codec "go.unistack.org/micro/v3/codec"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

type unmarshalOptionsKey struct{}

func UnmarshalOptions(o jsonpb.UnmarshalOptions) codec.Option {
	return codec.SetOption(unmarshalOptionsKey{}, o)
}

type marshalOptionsKey struct{}

func MarshalOptions(o jsonpb.MarshalOptions) codec.Option {
	return codec.SetOption(marshalOptionsKey{}, o)
}
