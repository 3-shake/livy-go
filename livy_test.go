package livy_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/3-shake/livy-go"
)

var service = livy.NewService(context.Background())

var RootPath string

func TestMain(m *testing.M) {
	exitVal := m.Run()

	os.Exit(exitVal)
}

func rootPath() {
	path, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		panic(err)
	}
	fmt.Println(string(path))
	RootPath = strings.TrimSpace(string(path))
}
