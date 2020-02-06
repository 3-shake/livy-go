package livy

import (
	"context"
	"errors"
	"net/http"
)

// https://livy.incubator.apache.org/docs/latest/rest-api.html
const basePath = "http://localhost:8998"

type Service struct {
	client   *http.Client
	BasePath string // API endpoint base URL

	Batches *BatchesService

	Sessions *SessionsService

	Statements *StatementsService
}

func NewService(ctx context.Context) (*Service, error) {
	s, err := New(http.DefaultClient)
	if err != nil {
		return nil, err
	}

	return s, nil

}

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Batches = NewBatchesService(s)
	s.Sessions = NewSessionsService(s)
	s.Statements = NewStatementsService(s)
	return s, nil
}
