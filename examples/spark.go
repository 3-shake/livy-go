package main

import (
	"context"

	"github.com/k0kubun/pp"
	"github.com/lithammer/dedent"
	"github.com/locona/livy"
)

func SessionsList() {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.List().Do()
	pp.Println(res, err)
}

func SessionsGet(sessionID int) {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.Get(sessionID).Do()
	pp.Println(res, err)
}

func SessionsInsert() {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.Insert(&livy.InsertSessionRequest{
		Kind: livy.SessionKind_Spark,
	}).Do()
	pp.Println(res, err)
}

func SessionsDelete(sessionID int) {
	svc := livy.NewService(context.Background())
	err := svc.Sessions.Delete(sessionID).Do()
	pp.Println(err)
}

func SessionsState(sessionID int) {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.State(sessionID).Do()
	pp.Println(res, err)
}

func SessionsLog(sessionID int) {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.Log(sessionID).Do()
	pp.Println(res, err)
}

func StatementsList(sessionID int) {
	svc := livy.NewService(context.Background())
	res, err := svc.Statements.List(sessionID).Do()
	pp.Println(res, err)
}

func StatementsGet(sessionID, statementId int) {
	svc := livy.NewService(context.Background())
	res, err := svc.Statements.Get(sessionID, statementId).Do()
	pp.Println(res, err)
}

func StatementsInsert(sessionID int) {
	svc := livy.NewService(context.Background())
	letter := "val NUM_SAMPLES = 100000;\n" +
		"val count = sc.parallelize(1 to NUM_SAMPLES).map { i => \n" +
		"val x = Math.random();\n" +
		"val y = Math.random();\n" +
		"if (x*x + y*y < 1) 1 else 0\n" +
		"}.reduce(_ + _);\n" +
		"println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)"

	letter = "val NUM_SAMPLES = 100000;\nval count = sc.parallelize(1 to NUM_SAMPLES).map { i =>\nval x = Math.random();\nval y = Math.random();\nif (x*x + y*y < 1) 1 else 0\n}.reduce(_ + _);\nprintln(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)"
	// fmt.Println(dedent.Dedent(letter))
	res, err := svc.Statements.Insert(sessionID, &livy.InsertStatementRequest{
		Code: dedent.Dedent(letter),
	}).Do()
	pp.Println(res, err)
}

func BatchesList() {
	svc := livy.NewService(context.Background())
	res, err := svc.Batches.List().Do()
	pp.Println(res, err)
}

func main() {
	// add your function calls here
	sessionID := 2
	// SessionsInsert()
	// SessionsGet(sessionID)
	// SessionsDelete(sessionID)
	// SessionsState(sessionID)
	// SessionsLog(sessionID)

	// StatementsList(sessionID)
	StatementsInsert(sessionID)
}
