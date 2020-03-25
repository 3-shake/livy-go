package livy

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type ErrorResponse struct {
	ErrorMessage string `json:"error_message"`
}

func (err *ErrorResponse) Error() string {
	return err.ErrorMessage
}

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

func SendRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	reqHeaders := make(http.Header)
	reqHeaders.Set("Content-Type", "application/json")
	req.Header = reqHeaders
	return client.Do(req)
}

// DecodeResponse decodes the body of res into target. If there is no body,
// target is unchanged.
func DecodeResponse(target interface{}, res *http.Response) error {
	if res.StatusCode >= http.StatusBadRequest {
		b, _ := ioutil.ReadAll(res.Body)
		return &ErrorResponse{string(b)}
	}

	return json.NewDecoder(res.Body).Decode(target)
}
