package livy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
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

type Statements struct {
	TotalStatements int          `json:"total_statements"`
	Statements      []*Statement `json:"statements"`
}

type Statement struct {
	ID     int
	Code   string
	State  StatementState
	Output StatementOutput
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
	err = DecodeResponse(statements, res)
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

	return SendRequest(c.s.client, req)
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
	err = DecodeResponse(statement, res)

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

	return SendRequest(c.s.client, req)
}

type InsertStatementRequest struct {
	// The code to execute
	Code string `json:"code"`
}

type StatementsInsertCall struct {
	s                      *Service
	sessionID              int
	insertStatementRequest *InsertStatementRequest
	wait                   bool
}

func (r *StatementsService) Insert(sessionID int, insertStatementRequest *InsertStatementRequest, wait bool) *StatementsInsertCall {
	c := &StatementsInsertCall{s: r.s}
	c.sessionID = sessionID
	c.insertStatementRequest = insertStatementRequest
	c.wait = wait

	return c
}

func (c *StatementsInsertCall) Do() (*Statement, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	statement := &Statement{}
	err = DecodeResponse(statement, res)
	if err != nil {
		return nil, err
	}
	if !c.wait {
		return statement, nil
	}

	availableStmt := &Statement{}
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		availableStmt, err = c.s.Statements.Get(c.sessionID, statement.ID).Do()
		if err != nil {
			break
		}

		if availableStmt.State == StatementState_Available {
			break
		}
	}

	return availableStmt, err
}

func (c *StatementsInsertCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v/statements", c.sessionID)
	body, err := JSONReader(c.insertStatementRequest)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	return SendRequest(c.s.client, req)
}
