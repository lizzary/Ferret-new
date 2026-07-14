package index

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestInitializeTantivyContainsDependencyStdout(t *testing.T) {
	const (
		helperEnvironment = "INDEXNODE_TANTIVY_INIT_HELPER"
		marker            = "tantivy-initialized"
	)
	if os.Getenv(helperEnvironment) == "1" {
		if err := InitializeTantivy(); err != nil {
			t.Fatalf("InitializeTantivy() error = %v", err)
		}
		fmt.Fprintln(os.Stdout, marker)
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestInitializeTantivyContainsDependencyStdout$", "-test.count=1")
	command.Env = append(os.Environ(), helperEnvironment+"=1")
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("Tantivy helper failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "### lenient") {
		t.Fatalf("Tantivy dependency output leaked to stdout: %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), marker) {
		t.Fatalf("stdout was not restored after initialization: %q", stdout.String())
	}
}
