package grpc

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/unistack-org/micro/v3/codec"
)

var (
	maxInt = int(^uint(0) >> 1)
)

func (c *grpcCodec) decode(r io.Reader) (uint8, []byte, error) {
	header := make([]byte, 5)

	// read the header
	if n, err := r.Read(header[:]); err != nil {
		if err == io.EOF && n == 0 {
			return 0, nil, nil
		}
		return 0, nil, err
	}

	// get encoding format e.g compressed
	cf := uint8(header[0])

	// get message length
	length := binary.BigEndian.Uint32(header[1:])

	// no encoding format
	if length == 0 {
		return cf, nil, nil
	}

	//
	if int64(length) > int64(maxInt) {
		return cf, nil, fmt.Errorf("grpc: received message larger than max length allowed on current machine (%d vs. %d)", length, maxInt)
	}
	if int(length) > codec.DefaultMaxMsgSize {
		return cf, nil, fmt.Errorf("grpc: received message larger than max (%d vs. %d)", length, codec.DefaultMaxMsgSize)
	}

	msg := make([]byte, int(length))

	if _, err := r.Read(msg); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return cf, nil, err
	}

	return cf, msg, nil
}

func (c *grpcCodec) encode(cf uint8, buf []byte, w io.Writer) error {
	header := make([]byte, 5)

	// set compression
	header[0] = byte(cf)

	// write length as header
	binary.BigEndian.PutUint32(header[1:], uint32(len(buf)))

	// read the header
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	// write the buffer
	_, err := w.Write(buf)
	return err
}
