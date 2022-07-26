package mongonet

import (
	"bytes"
	"testing"
)

func TestBytesCodec(t *testing.T) {
	var emptySlice []byte
	testcases := [][]byte{nil, emptySlice, []byte("hello world")}
	for _, input := range testcases {
		codec := BytesCodec{}
		mashalled, err := codec.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshall %v", input)
		}

		var unmarshalled []byte
		err = codec.Unmarshal(mashalled, &unmarshalled)
		if err != nil {
			t.Errorf("Failed to unmarshall %v", mashalled)
		}

		if bytes.Compare(input, unmarshalled) != 0 {
			t.Errorf("input does not equal unmarshalled (%v != %v)", input, unmarshalled)
		}
	}
}
