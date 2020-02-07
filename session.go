package livy

import (
	"fmt"
	"io"
	"net/http"

	"github.com/locona/livy/gensupport"
)

const (
	SessionKind_Spark   = SessionKind("spark")
	SessionKind_PySpark = SessionKind("pyspark")
	SessionKind_SparkR  = SessionKind("sparkr")
	SessionKind_Sql     = SessionKind("sql")
)

var (
	SessionKindList = []SessionKind{
		SessionKind_Spark,
		SessionKind_PySpark,
		SessionKind_SparkR,
		SessionKind_Sql,
	}
)

const (
	SessionState_NotStarted   = SessionState("not_started")
	SessionState_Starting     = SessionState("starting")
	SessionState_Idle         = SessionState("idle")
	SessionState_Busy         = SessionState("busy")
	SessionState_ShuttingDown = SessionState("shutting_down")
	SessionState_Error        = SessionState("error")
	SessionState_Dead         = SessionState("dead")
	SessionState_Killed       = SessionState("killed")
	SessionState_Success      = SessionState("success")
)

type SessionKind string

type SessionState string

type SessionsService struct {
	s *Service
}

type Sessions struct {
	s     *Service
	From  int        `json:"from"`
	Size  int        `json:"size"`
	Items []*Session `json:"sessions"`
}

type SessionsListCall struct {
	s    *Service
	from int
	size int
}

type Session struct {
	ID        int               `json:"id"`
	AppID     string            `json:"appId"`
	Owner     string            `json:"owner"`
	ProxyUser string            `json:"proxyUser"`
	Kind      SessionKind       `json:"kind"`
	Log       []string          `json:"log"`
	State     SessionState      `json:"state"`
	AppInfo   map[string]string `json:"appInfo"`
}

func NewSessionsService(s *Service) *SessionsService {
	rs := &SessionsService{s: s}
	return rs
}

func (r *SessionsService) List() *SessionsListCall {
	c := &SessionsListCall{s: r.s}

	return c
}

func (c *SessionsListCall) Do() (*Sessions, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	sessions := &Sessions{}
	err = gensupport.DecodeResponse(sessions, res)
	if err != nil {
		return nil, err
	}
	return sessions, nil
}

func (c *SessionsListCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + "/sessions"
	var body io.Reader = nil
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type SessionsGetCall struct {
	s *Service

	sessionID int
}

func (r *SessionsService) Get(sessionID int) *SessionsGetCall {
	c := &SessionsGetCall{s: r.s}
	c.sessionID = sessionID
	return c
}

func (c *SessionsGetCall) Do() (*Session, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	session := &Session{}

	err = gensupport.DecodeResponse(session, res)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func (c *SessionsGetCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v", c.sessionID)
	var body io.Reader = nil

	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type InsertSessionRequest struct {
	// The name of this session
	Name string `json:"name,omitempty"`
	// The session kind
	Kind SessionKind `json:"kind"`
	// User to impersonate when starting the session
	ProxyUser string `json:"proxyUser,omitempty"`
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
	NumExecutors int `json:"num_executors,omitempty"`
	// Archives to be used in this session
	Archives []string `json:"archives,omitempty"`
	// The name of the YARN queue to which submitted
	Queue string `json:"queue,omitempty"`
	// Spark configuration properties
	Conf map[string]string `json:"conf,omitempty"`
	// Timeout in second to which session be orphaned
	HeartbeatTimeoutInSecond int `json:"heartbeatTimeoutInSecond,omitempty"`
}

type SessionsInsertCall struct {
	s                    *Service
	insertSessionRequest *InsertSessionRequest
}

// Insert: Creates a new session.
func (r *SessionsService) Insert(insertSessionRequest *InsertSessionRequest) *SessionsInsertCall {
	c := &SessionsInsertCall{s: r.s}
	c.insertSessionRequest = insertSessionRequest

	return c
}

func (c *SessionsInsertCall) Do() (*Session, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	session := &Session{}
	err = gensupport.DecodeResponse(session, res)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func (c *SessionsInsertCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + "/sessions"
	var body io.Reader = nil

	body, err := gensupport.JSONReader(c.insertSessionRequest)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type SessionsDeleteCall struct {
	s *Service

	sessionID int
}

func (r *SessionsService) Delete(sessionID int) *SessionsDeleteCall {
	c := &SessionsDeleteCall{s: r.s}
	c.sessionID = sessionID
	return c
}

func (c *SessionsDeleteCall) Do() error {
	res, err := c.doRequest()
	if err != nil {
		return err
	}

	defer res.Body.Close()

	return nil
}

func (c *SessionsDeleteCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v", c.sessionID)
	var body io.Reader = nil

	req, err := http.NewRequest("DELETE", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type SessionStateResponse struct {
	ID    int          `json:"id"`
	State SessionState `json:"state"`
}

type SessionsGetStateCall struct {
	s *Service

	sessionID int
}

func (r *SessionsService) State(sessionID int) *SessionsGetStateCall {
	c := &SessionsGetStateCall{s: r.s}
	c.sessionID = sessionID
	return c
}

func (c *SessionsGetStateCall) Do() (*SessionStateResponse, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	sessionState := &SessionStateResponse{}

	err = gensupport.DecodeResponse(sessionState, res)
	if err != nil {
		return nil, err
	}

	return sessionState, nil
}

func (c *SessionsGetStateCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v/state", c.sessionID)
	var body io.Reader = nil

	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}

type SessionLogResponse struct {
	ID   int
	From int
	Size int
	Log  []string
}

type SessionsLogCall struct {
	s *Service

	sessionID int
}

func (r *SessionsService) Log(sessionID int) *SessionsLogCall {
	c := &SessionsLogCall{s: r.s}
	c.sessionID = sessionID
	return c
}

func (c *SessionsLogCall) Do() (*SessionLogResponse, error) {
	res, err := c.doRequest()
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	sessionLog := &SessionLogResponse{}

	err = gensupport.DecodeResponse(sessionLog, res)
	if err != nil {
		return nil, err
	}

	return sessionLog, nil
}

func (c *SessionsLogCall) doRequest() (*http.Response, error) {
	url := c.s.BasePath + fmt.Sprintf("/sessions/%v/log", c.sessionID)
	var body io.Reader = nil

	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, err
	}

	return gensupport.SendRequest(c.s.client, req)
}
