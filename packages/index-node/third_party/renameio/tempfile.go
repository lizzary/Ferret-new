// Package renameio supplies the small atomic-temp-file API required by
// coder/hnsw. Upstream renameio v1 excludes that API on Windows, which makes
// coder/hnsw fail to compile there even when SavedGraph is not used.
package renameio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type PendingFile struct {
	*os.File
	destination string
	done        bool
	closed      bool
}

func TempFile(dir, path string) (*PendingFile, error) {
	if path == "" {
		return nil, errors.New("renameio: destination is empty")
	}
	if dir == "" {
		dir = filepath.Dir(path)
	}
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return nil, err
	}
	return &PendingFile{File: file, destination: path}, nil
}

func (file *PendingFile) Cleanup() error {
	if file == nil || file.done {
		return nil
	}
	var closeErr error
	if !file.closed {
		closeErr = file.Close()
		file.closed = true
	}
	removeErr := os.Remove(file.Name())
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

func (file *PendingFile) CloseAtomicallyReplace() error {
	if file == nil || file.File == nil {
		return errors.New("renameio: invalid pending file")
	}
	if file.done {
		return nil
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if !file.closed {
		if err := file.Close(); err != nil {
			return err
		}
		file.closed = true
	}
	if err := atomicReplace(file.Name(), file.destination); err != nil {
		return fmt.Errorf("renameio: replace destination: %w", err)
	}
	file.done = true
	return nil
}
