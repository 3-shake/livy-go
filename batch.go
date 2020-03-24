package livy

import (
	"fmt"
	"io"
	"net/http"

	"github.com/3-shake/livy-go/gensupport"
)

type BatchesService struct {
	s *Service
}

type Batches struct {
	From     int      `json:"from"`
	Total    int      `json:"total"`
	Sessions []*Batch `json:"sessions"`
}

type Batch struct {
	ID      int               `json:"id"`
	AppID   string            `json:"appid"`
	AppInfo map[string]string `json:"appInfo"`
	Log     []string          `json:"log"`
	State   string            `json:"state"`
}

func NewBatchesService(s *Service) *BatchesService {
	rs := &BatchesService{s: s}
	return rs
}

type BatchesListCall struct {
	s *Service
}

func (r *BatchesService) List() *BatchesListCall {
	c := &BatchesListCall{s: r.s}
	return c
}

func (c *BatchesListCall) Do() (*Batches, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	batches := &Batches{}
	err = gensupport.DecodeResponse(batches, res)
	if err != nil {
		return nil, err
	}
	return batches, nil
}

func (c *BatchesListCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + "/batches"
	var body io.Reader = nil
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type BatchesGetCall struct {
	s       *Service
	batchID int
}

func (r *BatchesService) Get(batchID int) *BatchesGetCall {
	c := &BatchesGetCall{s: r.s}
	c.batchID = batchID

	return c
}

func (c *BatchesGetCall) Do() (*Batch, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	batch := &Batch{}
	err = gensupport.DecodeResponse(batch, res)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func (c *BatchesGetCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/batches/%v", c.batchID)
	var body io.Reader = nil
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type BatchState struct {
	ID    int
	State string
}

type BatchesStateCall struct {
	s       *Service
	batchID int
}

func (r *BatchesService) State(batchID int) *BatchesStateCall {
	c := &BatchesStateCall{s: r.s}
	c.batchID = batchID

	return c
}

func (c *BatchesStateCall) Do() (*BatchState, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	state := &BatchState{}
	err = gensupport.DecodeResponse(state, res)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (c *BatchesStateCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/batches/%v/state", c.batchID)
	var body io.Reader = nil
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type BatchesDeleteCall struct {
	s *Service

	batchID int
}

func (r *BatchesService) Delete(batchID int) *BatchesDeleteCall {
	c := &BatchesDeleteCall{s: r.s}
	c.batchID = batchID
	return c
}

func (c *BatchesDeleteCall) Do() error {
	res, err := c.doRequest()
	if err != nil {
		return err
	}

	defer res.Body.Close()

	return nil
}

func (c *BatchesDeleteCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/batches/%v", c.batchID)
	var body io.Reader = nil

	req, err := http.NewRequest("DELETE", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type BatchLogResponse struct {
	ID   int
	From int
	Size int
	Log  []string
}

type BatchesLogCall struct {
	s *Service

	batchID int
}

func (r *BatchesService) Log(batchID int) *BatchesLogCall {
	c := &BatchesLogCall{s: r.s}
	c.batchID = batchID
	return c
}

func (c *BatchesLogCall) Do() (*BatchLogResponse, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	batchLog := &BatchLogResponse{}

	err = gensupport.DecodeResponse(batchLog, res)
	if err != nil {
		return nil, err
	}

	return batchLog, nil
}

func (c *BatchesLogCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/batches/%v/log", c.batchID)
	var body io.Reader = nil

	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}
