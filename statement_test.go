package livy_test

import (
	"testing"
	"time"

	"github.com/k0kubun/pp"
	"github.com/lithammer/dedent"
	"github.com/stretchr/testify/assert"

	"github.com/3-shake/livy-go"
)

func TestStatement_List(t *testing.T) {
	sess, err := insert()

	sessionWait(sess.ID)
	_, _ = statementInsert(sess.ID)

	res, err := service.Statements.List(sess.ID).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestStatement_Get(t *testing.T) {
	sess, err := insert()

	sessionWait(sess.ID)
	stmt, _ := statementInsert(sess.ID)

	res, err := service.Statements.Get(sess.ID, stmt.ID).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestStatement_Insert(t *testing.T) {
	sess, err := insert()

	sessionWait(sess.ID)
	res, err := statementInsert(sess.ID)
	pp.Println(res, err)

	assert.Equal(t, err, nil)
	assert.Equal(t, res.State, livy.StatementState_Waiting)
}

func TestStatement_Insert_Wait(t *testing.T) {
	sess, err := insert()

	sessionWait(sess.ID)
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

	res, err := service.Statements.Insert(sess.ID, &livy.InsertStatementRequest{
		Code: dedent.Dedent(letter),
	}, true).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
	assert.Equal(t, res.State, livy.StatementState_Available)
}

func sessionWait(sessionID int) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for range t.C {
		res, _ := service.Sessions.State(sessionID).Do()
		if res.State == livy.SessionState_Idle {
			return
		}
	}
}

func statementInsert(sessionID int) (*livy.Statement, error) {
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

	return service.Statements.Insert(sessionID, &livy.InsertStatementRequest{
		Code: dedent.Dedent(letter),
	}, false).Do()
}
