package livy

import (
	"context"
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

func NewService(ctx context.Context) *Service {
	return New(http.DefaultClient)

}

func New(client *http.Client) *Service {
	s := &Service{client: client, BasePath: basePath}
	s.Batches = NewBatchesService(s)
	s.Sessions = NewSessionsService(s)
	s.Statements = NewStatementsService(s)

	return s
}
