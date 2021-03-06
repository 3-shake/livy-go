package livy

import (
	"fmt"
	"io"
	"net/http"
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
	err = DecodeResponse(batches, res)
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

	return SendRequest(c.s.client, req)
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
	err = DecodeResponse(batch, res)
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

	return SendRequest(c.s.client, req)
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
	err = DecodeResponse(state, res)
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

	return SendRequest(c.s.client, req)
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

	return SendRequest(c.s.client, req)
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

	err = DecodeResponse(batchLog, res)
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

	return SendRequest(c.s.client, req)
}

type InsertBatchRequest struct {
	// File containing the application to execute
	File string `json:"file"`
	// User to impersonate when starting the batch
	ProxyUser string `json:"proxyUser,omitempty"`
	// Application Java/Spark main class
	ClassName string `json:"className,omitempty"`
	// Command line arguments for the application
	Args []string `json:"args,omitempty"`
	// jars to be used in this session
	Jars []string `json:"jars,omitempty"`
	// Python files to be used in this session
	PyFiles []string `json:"pyFiles,omitempty"`
	// files to be used in this session
	Files []string `json:"files,omitempty"`
	// Amount of memory to use for the driver process
	DriverMemory string `json:"driverMemory,omitempty"`
	// Number of cores to use for the driver process
	DriverCores int `json:"driverCores,omitempty"`
	// Amount of memory to use per executor process
	ExecutorMemory string `json:"executorMemory,omitempty"`
	// Number of cores to use for each executor
	ExecutorCores int `json:"executorCores,omitempty"`
	// Number of executors to launch for this session
	NumExecutors int `json:"numExecutors,omitempty"`
	// Archives to be used in this session
	Archives []string `json:"archives,omitempty"`
	// The name of the YARN queue to which submitted
	Queue string `json:"queue,omitempty"`
	// The name of this session
	Name string `json:"name,omitempty"`
	// Spark configuration properties
	Conf map[string]string `json:"conf,omitempty"`
}

type BatchesInsertCall struct {
	s                  *Service
	insertBatchRequest *InsertBatchRequest
}

// Insert: Creates a new batch.
func (r *BatchesService) Insert(insertBatchRequest *InsertBatchRequest) *BatchesInsertCall {
	c := &BatchesInsertCall{s: r.s}
	c.insertBatchRequest = insertBatchRequest

	return c
}

func (c *BatchesInsertCall) Do() (*Batch, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	batch := &Batch{}
	err = DecodeResponse(batch, res)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

func (c *BatchesInsertCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + "/batches"

	body, err := JSONReader(c.insertBatchRequest)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	return SendRequest(c.s.client, req)
}
