package instance

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestExclusiveLockRejectsSecondOwnerAndReleases(t *testing.T) {
	directory := t.TempDir()
	first, err := Acquire(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Acquire(directory)
	if !errors.Is(err, ErrAlreadyRunning) || second != nil {
		t.Fatalf("second Acquire() = %v, %v", second, err)
	}
	if _, err := first.file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(first.file)
	if err != nil || !strings.Contains(string(contents), "pid=") {
		t.Fatalf("lock diagnostics = %q, %v", contents, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := Acquire(directory)
	if err != nil {
		t.Fatalf("Acquire() after release: %v", err)
	}
	defer third.Close()
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("idempotent Close(): %v", err)
	}
}

func TestAcquireValidation(t *testing.T) {
	if _, err := Acquire(""); err == nil {
		t.Fatal("Acquire(empty) error = nil")
	}
}
