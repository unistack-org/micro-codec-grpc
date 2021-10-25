// Package grpc provides a grpc codec
package grpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/metadata"
	rutil "go.unistack.org/micro/v3/util/reflect"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	DefaultMarshalOptions = jsonpb.MarshalOptions{
		UseEnumNumbers:  false,
		EmitUnpopulated: false,
		UseProtoNames:   true,
		AllowPartial:    false,
	}

	DefaultUnmarshalOptions = jsonpb.UnmarshalOptions{
		DiscardUnknown: false,
		AllowPartial:   false,
	}
)

type jsonpbCodec struct {
	opts codec.Options
}

const (
	flattenTag = "flatten"
)

type grpcCodec struct {
	opts        codec.Options
	ContentType string
}

func (c *grpcCodec) ReadHeader(conn io.Reader, m *codec.Message, t codec.MessageType) error {
	if ct := m.Header["content-type"]; len(ct) > 0 {
		c.ContentType = ct
	}

	// service method
	path := m.Header[":path"]
	if len(path) == 0 || path[0] != '/' {
		m.Target = m.Header[metadata.HeaderService]
		m.Endpoint = m.Header[metadata.HeaderEndpoint]
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

func (c *grpcCodec) Unmarshal(d []byte, v interface{}, opts ...codec.Option) error {
	if v == nil || len(d) == 0 {
		return nil
	}

	options := c.opts
	for _, o := range opts {
		o(&options)
	}

	if nv, nerr := rutil.StructFieldByTag(v, options.TagName, flattenTag); nerr == nil {
		v = nv
	}

	if m, ok := v.(*codec.Frame); ok {
		m.Data = d
		return nil
	}

	if c.ContentType == "application/grpc+json" {
		return json.Unmarshal(d, v)
	}

	if _, ok := v.(proto.Message); !ok {
		return codec.ErrInvalidMessage
	}

	unmarshalOptions := DefaultUnmarshalOptions
	if options.Context != nil {
		if f, ok := options.Context.Value(unmarshalOptionsKey{}).(jsonpb.UnmarshalOptions); ok {
			unmarshalOptions = f
		}
	}

	switch c.ContentType {
	case "application/grpc+json":
		return unmarshalOptions.Unmarshal(d, v.(proto.Message))
	case "application/grpc+proto", "application/grpc":
		return proto.Unmarshal(d, v.(proto.Message))
	}

	return codec.ErrInvalidMessage
}

func (c *grpcCodec) Marshal(v interface{}, opts ...codec.Option) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	options := c.opts
	for _, o := range opts {
		o(&options)
	}

	if nv, nerr := rutil.StructFieldByTag(v, options.TagName, flattenTag); nerr == nil {
		v = nv
	}

	if m, ok := v.(*codec.Frame); ok {
		return m.Data, nil
	}

	if c.ContentType == "application/grpc+json" {
		return json.Marshal(v)
	}

	if _, ok := v.(proto.Message); !ok {
		return nil, codec.ErrInvalidMessage
	}

	marshalOptions := DefaultMarshalOptions
	if options.Context != nil {
		if f, ok := options.Context.Value(marshalOptionsKey{}).(jsonpb.MarshalOptions); ok {
			marshalOptions = f
		}
	}

	switch c.ContentType {
	case "application/grpc+json":
		return marshalOptions.Marshal(v.(proto.Message))
	case "application/grpc+proto", "application/grpc":
		return proto.Marshal(v.(proto.Message))
	}

	return nil, codec.ErrUnknownContentType
}

func (c *grpcCodec) ReadBody(conn io.Reader, v interface{}) error {
	// no body
	if v == nil {
		return nil
	}

	n, buf, err := c.decode(conn)
	if err != nil {
		return err
	} else if n == 0 {
		return nil
	}

	return c.Unmarshal(buf, v)
}

func (c *grpcCodec) Write(conn io.Writer, m *codec.Message, v interface{}) error {
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

	buf, err := c.Marshal(v)
	if err != nil {
		m.Header["grpc-status"] = "8"
		m.Header["grpc-message"] = err.Error()
		return err
	}

	if len(buf) == 0 {
		return nil
	}

	m.Body = buf
	return c.encode(0, buf, conn)
}

func (c *grpcCodec) String() string {
	return "grpc"
}

func NewCodec(opts ...codec.Option) codec.Codec {
	return &grpcCodec{opts: codec.NewOptions(opts...), ContentType: "application/grpc"}
}
