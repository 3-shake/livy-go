package livy_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"

	"github.com/3-shake/livy-go"
)

func TestBatch_List(t *testing.T) {
	res, err := service.Batches.List().Do()
	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestBatch_Get(t *testing.T) {
	bat, _ := insertBatch()
	res, err := service.Batches.Get(bat.ID).Do()
	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestBatch_Insert(t *testing.T) {
	_, err := insertBatch()

	pp.Println(err)

	assert.Equal(t, err, nil)
}

func TestBatch_Delete(t *testing.T) {
	bat, _ := insertBatch()
	err := service.Batches.Delete(bat.ID).Do()

	pp.Println(err)

	assert.Equal(t, err, nil)
}

func TestBatch_State(t *testing.T) {
	bat, _ := insertBatch()
	res, err := service.Batches.State(bat.ID).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func TestBatch_Log(t *testing.T) {
	bat, _ := insertBatch()
	res, err := service.Batches.Log(bat.ID).Do()

	pp.Println(res, err)

	assert.Equal(t, err, nil)
}

func insertBatch() (*livy.Batch, error) {
	className := "com.example.livy.WordCount"
	jarPath := "/work/root-assembly-1.0.0-SNAPSHOT.jar"
	uid := uuid.New()
	return service.Batches.Insert(&livy.InsertBatchRequest{
		File:      fmt.Sprintf("local:%v", jarPath),
		ClassName: className,
		Name:      uid.String(),
	}).Do()
}
