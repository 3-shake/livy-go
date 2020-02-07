package livy

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/locona/livy/gensupport"
)

const (
	StatementState_Waiting    = StatementState("waiting")
	StatementState_Running    = StatementState("running")
	StatementState_Available  = StatementState("available")
	StatementState_Error      = StatementState("error")
	StatementState_Cancelling = StatementState("cancelling")
	StatementState_Cancelled  = StatementState("cancelled")
)

type StatementState string

type Statements []*Statement

type Statement struct {
	ID     int
	Code   string
	State  StatementState
	Output StatementState
}

type StatementOutput struct {
	Status         string
	ExecutionCount int
	Data           *json.RawMessage
}

type StatementsService struct {
	s *Service
}

func NewStatementsService(s *Service) *StatementsService {
	rs := &StatementsService{s: s}
	return rs
}

type StatementsListCall struct {
	s         *Service
	sessionID int
}

func (r *StatementsService) List(sessionID int) *StatementsListCall {
	c := &StatementsListCall{s: r.s}
	c.sessionID = sessionID
	return c
}

func (c *StatementsListCall) Do() (*Statements, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	statements := &Statements{}
	err = gensupport.DecodeResponse(statements, res)
	if err != nil {
		return nil, err
	}
	return statements, nil
}

func (c *StatementsListCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v/statements", c.sessionID)
	var body io.Reader = nil
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type StatementsGetCall struct {
	s           *Service
	sessionID   int
	statementID int
}

func (r *StatementsService) Get(sessionID, statementID int) *StatementsGetCall {
	c := &StatementsGetCall{s: r.s}
	c.sessionID = sessionID
	c.statementID = statementID

	return c
}

func (c *StatementsGetCall) Do() (*Statement, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	statement := &Statement{}
	err = gensupport.DecodeResponse(statement, res)

	if err != nil {
		return nil, err
	}
	return statement, nil
}

func (c *StatementsGetCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v/statements/%v", c.sessionID, c.statementID)
	var body io.Reader = nil
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type InsertStatementRequest struct {
	// The code to execute
	Code string `json:"code"`
}

type StatementsInsertCall struct {
	s                      *Service
	sessionID              int
	insertStatementRequest *InsertStatementRequest
}

func (r *StatementsService) Insert(sessionID int, insertStatementRequest *InsertStatementRequest) *StatementsInsertCall {
	c := &StatementsInsertCall{s: r.s}
	c.sessionID = sessionID
	c.insertStatementRequest = insertStatementRequest

	return c
}

func (c *StatementsInsertCall) Do() (*Statement, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	s, _ := ioutil.ReadAll(res.Body)
	statement := &Statement{}
	err = gensupport.DecodeResponse(statement, res)
	if err != nil {
		return nil, err
	}
	return statement, nil
}

func (c *StatementsInsertCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v/statements", c.sessionID)
	body, err := gensupport.JSONReader(c.insertStatementRequest)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}
