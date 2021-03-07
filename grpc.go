// Package grpc provides a grpc codec
package grpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	// nolint: staticcheck
	oldjsonpb "github.com/golang/protobuf/jsonpb"
	// nolint: staticcheck
	oldproto "github.com/golang/protobuf/proto"
	"github.com/unistack-org/micro/v3/codec"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	JsonpbMarshaler = jsonpb.MarshalOptions{
		UseEnumNumbers:  false,
		EmitUnpopulated: false,
		UseProtoNames:   true,
		AllowPartial:    false,
	}

	JsonpbUnmarshaler = jsonpb.UnmarshalOptions{
		DiscardUnknown: false,
		AllowPartial:   false,
	}

	OldJsonpbMarshaler = oldjsonpb.Marshaler{
		OrigName:     true,
		EmitDefaults: false,
	}

	OldJsonpbUnmarshaler = oldjsonpb.Unmarshaler{
		AllowUnknownFields: false,
	}
)

type grpcCodec struct {
	ContentType string
}

func (c *grpcCodec) ReadHeader(conn io.Reader, m *codec.Message, t codec.MessageType) error {
	if ct := m.Header["content-type"]; len(ct) > 0 {
		c.ContentType = ct
	}

	// service method
	path := m.Header[":path"]
	if len(path) == 0 || path[0] != '/' {
		m.Target = m.Header["Micro-Service"]
		m.Endpoint = m.Header["Micro-Endpoint"]
	} else {
		// [ , a.package.Foo, Bar]
		parts := strings.Split(path, "/")
		if len(parts) != 3 {
			return errors.New("Unknown request path")
		}
		service := strings.Split(parts[1], ".")
		m.Endpoint = strings.Join([]string{service[len(service)-1], parts[2]}, ".")
		m.Target = strings.Join(service[:len(service)-1], ".")
	}

	return nil
}

func (c *grpcCodec) Unmarshal(d []byte, b interface{}) error {
	if d == nil {
		return nil
	}

	switch v := b.(type) {
	case nil:
		return nil
	default:
		switch c.ContentType {
		case "application/grpc+json":
			return json.Unmarshal(d, v)
		}
	case *codec.Frame:
		v.Data = d
	case oldproto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			return OldJsonpbUnmarshaler.Unmarshal(bytes.NewReader(d), v)
		case "application/grpc+proto", "application/grpc":
			return oldproto.Unmarshal(d, v)
		}
	case proto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			return JsonpbUnmarshaler.Unmarshal(d, v)
		case "application/grpc+proto", "application/grpc":
			return proto.Unmarshal(d, v)
		}
	}

	return codec.ErrInvalidMessage
}

func (c *grpcCodec) Marshal(b interface{}) ([]byte, error) {
	switch m := b.(type) {
	case nil:
		return nil, nil
	case *codec.Frame:
		return m.Data, nil
	case proto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			return JsonpbMarshaler.Marshal(m)
		case "application/grpc+proto", "application/grpc":
			return proto.Marshal(m)
		}
	case oldproto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			str, err := OldJsonpbMarshaler.MarshalToString(m)
			return []byte(str), err
		case "application/grpc+proto", "application/grpc":
			return oldproto.Marshal(m)
		}
	default:
		switch c.ContentType {
		case "application/grpc+json":
			return json.Marshal(m)
		}
	}

	return nil, codec.ErrUnknownContentType
}

func (c *grpcCodec) ReadBody(conn io.Reader, b interface{}) error {
	// no body
	if b == nil {
		return nil
	}

	_, buf, err := c.decode(conn)
	if err != nil {
		return err
	}

	switch v := b.(type) {
	default:
		switch c.ContentType {
		case "application/grpc+json":
			return json.Unmarshal(buf, v)
		}
	case *codec.Frame:
		v.Data = buf
	case oldproto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			return OldJsonpbUnmarshaler.Unmarshal(bytes.NewReader(buf), v)
		case "application/grpc+proto", "application/grpc":
			return oldproto.Unmarshal(buf, v)
		}
	case proto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			return JsonpbUnmarshaler.Unmarshal(buf, v)
		case "application/grpc+proto", "application/grpc":
			return proto.Unmarshal(buf, v)
		}
	}

	return codec.ErrUnknownContentType
}

func (c *grpcCodec) Write(conn io.Writer, m *codec.Message, b interface{}) error {
	var buf []byte
	var err error

	if ct := m.Header["content-type"]; len(ct) > 0 {
		c.ContentType = ct
	}

	switch m.Type {
	case codec.Request:
		parts := strings.Split(m.Endpoint, ".")
		m.Header[":method"] = "POST"
		m.Header[":path"] = fmt.Sprintf("/%s.%s/%s", m.Target, parts[0], parts[1])
		m.Header[":proto"] = "HTTP/2.0"
		m.Header["te"] = "trailers"
		m.Header["user-agent"] = "grpc-go/1.0.0"
		m.Header[":authority"] = m.Target
		m.Header["content-type"] = c.ContentType
	case codec.Response:
		m.Header["Trailer"] = "grpc-status" //, grpc-message"
		m.Header["content-type"] = c.ContentType
		m.Header[":status"] = "200"
		m.Header["grpc-status"] = "0"
		//		m.Header["grpc-message"] = ""
	case codec.Error:
		m.Header["Trailer"] = "grpc-status, grpc-message"
		// micro end of stream
		if m.Error == "EOS" {
			m.Header["grpc-status"] = "0"
		} else {
			m.Header["grpc-message"] = m.Error
			m.Header["grpc-status"] = "13"
		}

		return nil
	}

	switch m := b.(type) {
	case *codec.Frame:
		buf = m.Data
	case proto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			buf, err = JsonpbMarshaler.Marshal(m)
		case "application/grpc+proto", "application/grpc":
			buf, err = proto.Marshal(m)
		default:
			err = codec.ErrUnknownContentType
		}
	case oldproto.Message:
		switch c.ContentType {
		case "application/grpc+json":
			var str string
			str, err = OldJsonpbMarshaler.MarshalToString(m)
			buf = []byte(str)
		case "application/grpc+proto", "application/grpc":
			buf, err = oldproto.Marshal(m)
		default:
			err = codec.ErrUnknownContentType
		}
	default:
		switch c.ContentType {
		case "application/grpc+json":
			buf, err = json.Marshal(m)
		default:
			err = codec.ErrUnknownContentType
		}

	}
	// check error
	if err != nil {
		m.Header["grpc-status"] = "8"
		m.Header["grpc-message"] = err.Error()
		return err
	}

	if len(buf) == 0 {
		return nil
	}

	return c.encode(0, buf, conn)
}

func (c *grpcCodec) String() string {
	return "grpc"
}

func NewCodec() codec.Codec {
	return &grpcCodec{ContentType: "application/grpc"}
}
