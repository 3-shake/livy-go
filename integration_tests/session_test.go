package livy_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"

	"github.com/3-shake/livy-go"
)

func TestSession_List(t *testing.T) {
	_, _ = insert()
	res, err := service.Sessions.List().Do()
	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestSession_Get(t *testing.T) {
	sess, _ := insert()
	res, err := service.Sessions.Get(sess.ID).Do()
	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestSession_Insert(t *testing.T) {
	_, err := insert()

	pp.Println(err)

	assert.Equal(t, err, nil)
}

func TestSession_Delete(t *testing.T) {
	sess, _ := insert()
	err := service.Sessions.Delete(sess.ID).Do()

	pp.Println(err)

	assert.Equal(t, err, nil)
}

func TestSession_State(t *testing.T) {
	sess, _ := insert()
	res, err := service.Sessions.State(sess.ID).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestSession_Log(t *testing.T) {
	sess, _ := insert()
	res, err := service.Sessions.Log(sess.ID).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func insert() (*livy.Session, error) {
	rootPath := RootPath

	jar := "/integration_tests/wordcount/target/scala-2.11/root-assembly-1.0.0-SNAPSHOT.jar"
	jarPath := fmt.Sprintf("%v/%v", rootPath, jar)

	uid := uuid.New()
	return service.Sessions.Insert(&livy.InsertSessionRequest{
		Name: uid.String(),
		Kind: livy.SessionKind_Spark,
		Jars: []string{
			fmt.Sprintf("local://%v", jarPath),
		},
		Conf: map[string]string{
			"spark.driver.extraClassPath": jarPath,
		},
	}).Do()
}
