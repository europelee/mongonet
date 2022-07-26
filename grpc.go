package mongonet

import (
	"fmt"
)

// Codec to pass along []byte slice pointers
// Inspired by:
// https://pkg.go.dev/encoding/json#RawMessage
type BytesCodec struct{}

// Dereference []byte slice pointer
func (c BytesCodec) Marshal(v interface{}) ([]byte, error) {
	rawMessage, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("Input was not of type *[]byte")
	}
	return *rawMessage, nil
}

// Expect v to be empty []byte slice pointer
func (c BytesCodec) Unmarshal(data []byte, v interface{}) error {
	rawMessage, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("Input was not of type *[]byte")
	}
	*rawMessage = append((*rawMessage)[0:0], data...)
	return nil
}

func (c BytesCodec) Name() string {
	return "bytes"
}
