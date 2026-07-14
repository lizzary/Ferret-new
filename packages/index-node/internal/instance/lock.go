// Package instance owns the cross-process data-directory lock. Every lifecycle
// or stopped-node maintenance operation that may mutate the durable store must
// hold this lock before Store.Open, so maintenance cannot race a live node or
// accidentally trigger its crash recovery.
package instance

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var ErrAlreadyRunning = errors.New("index-node: data directory is already owned by another process")

const lockFilename = "indexnode.lock"

type Lock struct {
	file *os.File
	once sync.Once
	err  error
}

// Acquire takes a non-blocking exclusive OS lock. The lock file is retained
// after Close; ownership belongs to the open handle and is released by the OS
// on process death, so stale files never require PID guessing or deletion.
func Acquire(dataDir string) (*Lock, error) {
	if dataDir == "" {
		return nil, errors.New("index-node: data directory is required for instance lock")
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("index-node: create data directory for lock: %w", err)
	}
	path := filepath.Join(dataDir, lockFilename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("index-node: open instance lock: %w", err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		if errors.Is(err, errLockUnavailable) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("index-node: acquire instance lock: %w", err)
	}
	lock := &Lock{file: file}
	// Diagnostic contents are never used to decide ownership; the OS lock is
	// authoritative. A failed diagnostic write must not discard a valid lock.
	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	_, _ = file.WriteString("pid=" + strconv.Itoa(os.Getpid()) + "\nacquired_at=" + time.Now().UTC().Format(time.RFC3339Nano) + "\n")
	_ = file.Sync()
	return lock, nil
}

func (lock *Lock) Close() error {
	if lock == nil {
		return nil
	}
	lock.once.Do(func() {
		lock.err = errors.Join(unlockFile(lock.file), lock.file.Close())
	})
	return lock.err
}
