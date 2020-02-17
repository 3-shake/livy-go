package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/k0kubun/pp"
	"github.com/lithammer/dedent"
	"github.com/locona/livy"
)

func SessionsList() {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.List().Do()
	pp.Println(res, err)
}

func SessionsGet(sessionID int) *livy.Session {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.Get(sessionID).Do()
	pp.Println(res, err)
	return res
}

func SessionsInsert() *livy.Session {
	svc := livy.NewService(context.Background())
	res, err := svc.Sessions.Insert(&livy.InsertSessionRequest{
		Kind: livy.SessionKind_Spark,
	}).Do()
	pp.Println(res, err)

	return res
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

func StatementsGet(sessionID, statementID int) *livy.Statement {
	svc := livy.NewService(context.Background())
	res, err := svc.Statements.Get(sessionID, statementID).Do()
	pp.Println(res, err)
	return res
}

func StatementsWait(sessionID, statementID int) *livy.Statement {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for range t.C {
		stmt := StatementsGet(sessionID, statementID)
		pp.Println(stmt)
		if stmt.State == livy.StatementState_Available {
			return stmt
		}
	}

	return nil
}

func StatementsInsert(sessionID int) *livy.Statement {
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
	return res
}

func BatchesList() {
	svc := livy.NewService(context.Background())
	res, err := svc.Batches.List().Do()
	pp.Println(res, err)
}

func work() {
	fmt.Println("#")
}

func routine(command <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var status = "Play"
	for {
		select {
		case cmd := <-command:
			switch cmd {
			case "Stop":
				return
			case "Pause":
				status = "Pause"
			default:
				status = "Play"
			}
		default:
			if status == "Play" {
				work()
			}
		}
	}
}

func main() {
	// add your function calls here
	// sessionID := 0
	// session := SessionsInsert()
	session := SessionsGet(0)
	// SessionsDelete(sessionID)
	// SessionsState(sessionID)
	// SessionsLog(sessionID)

	// Statement
	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// command := make(chan string)
	// go routine(command & wg)
	// StatementsList(sessionID)
	statement := StatementsInsert(session.ID)
	pp.Println(session.ID, statement.ID)
	// statement := StatementsGet(session.ID, statement.ID)
	statement = StatementsWait(session.ID, statement.ID)
	pp.Println(statement)
	b, _ := statement.Output.Data.MarshalJSON()
	fmt.Println(string(b))
}
