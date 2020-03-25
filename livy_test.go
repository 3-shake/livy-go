package livy_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/3-shake/livy-go"
)

var service = livy.NewService(context.Background())

var RootPath string

func TestMain(m *testing.M) {
	rootPath()

	exitVal := m.Run()

	os.Exit(exitVal)
}

func rootPath() {
	path, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		panic(err)
	}

	RootPath = strings.TrimSpace(string(path))
}
