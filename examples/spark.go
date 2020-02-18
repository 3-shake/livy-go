package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
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

func SessionsInsert(localJarPath string) *livy.Session {
	svc := livy.NewService(context.Background())
	res, _ := svc.Sessions.Insert(&livy.InsertSessionRequest{
		Kind: livy.SessionKind_Spark,
		Jars: []string{
			fmt.Sprintf("local://%v", localJarPath),
		},
		Conf: map[string]string{
			"spark.driver.extraClassPath": localJarPath,
		},
	}).Do()

	pp.Println(localJarPath, res)
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
		b, _ := stmt.Output.Data.MarshalJSON()
		pp.Println(string(b))
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

	letter = "import com.locona.livy._\n" +
		"val ds = WordCount.executor()\n" +
		"ds.show(false)\n" +
		"ds.printSchema"
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

// Stat is exported out of golang convention, rather than necessity

func rootPath() (string, error) {
	path, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", err
	}
	fmt.Println(string(path))
	return strings.TrimSpace(string(path)), nil
}

func main() {
	rootPath, err := rootPath()
	if err != nil {
		panic(err)
	}

	jar := "/jars/target/scala-2.11/root-assembly-1.0.0-SNAPSHOT.jar"
	jarPath := fmt.Sprintf("%v/%v", rootPath, jar)
	SessionsInsert(jarPath)

	sessionID := 2
	// sessionID = session.ID
	// session := SessionsGet(sessionID)
	// SessionsDelete(sessionID)
	// SessionsState(sessionID)
	// SessionsLog(sessionID)

	// Statement
	// StatementsList(sessionID)
	statement := StatementsInsert(sessionID)
	statement = StatementsWait(sessionID, statement.ID)
	pp.Println(statement)
}
