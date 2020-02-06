package gensupport

import (
	"bytes"
	"encoding/json"
	"io"
)

func JSONReader(v interface{}) (io.Reader, error) {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
