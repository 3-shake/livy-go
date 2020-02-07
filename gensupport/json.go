package gensupport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func JSONReader(v interface{}) (io.Reader, error) {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}

	return bytes.NewBufferString(strings.TrimRight(buf.String(), "\n")), nil
}
