package reconcile

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lizzary/index-node/internal/fsmeta"
)

const defaultStatConcurrency = 4

// OSFileSystem never follows symlinks or Windows reparse-point entries exposed
// as ModeSymlink. A scan therefore cannot escape its configured root.
type OSFileSystem struct {
	// StatConcurrency bounds both in-flight stat calls and the internal queue
	// sizes for one root walk. Values <= 0 use defaultStatConcurrency.
	StatConcurrency int
	// LstatFunc is an optional test/platform injection. Production callers use
	// os.Lstat, which preserves the no-symlink-following contract.
	LstatFunc func(string) (fs.FileInfo, error)
}

func (fileSystem OSFileSystem) Lstat(path string) (fs.FileInfo, error) {
	if fileSystem.LstatFunc != nil {
		return fileSystem.LstatFunc(path)
	}
	return os.Lstat(path)
}

func (OSFileSystem) SameFile(left, right fs.FileInfo) bool { return os.SameFile(left, right) }

func (fileSystem OSFileSystem) Walk(ctx context.Context, root Root, visit func(FileSnapshot) error) error {
	if ctx == nil || visit == nil {
		return fs.ErrInvalid
	}
	statConcurrency := fileSystem.StatConcurrency
	if statConcurrency <= 0 {
		statConcurrency = defaultStatConcurrency
	}

	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type statResult struct {
		snapshot FileSnapshot
	}
	paths := make(chan string, statConcurrency)
	results := make(chan statResult, statConcurrency)
	statErrors := make(chan error, 1)

	var workers sync.WaitGroup
	workers.Add(statConcurrency)
	for range statConcurrency {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-walkCtx.Done():
					return
				case path, ok := <-paths:
					if !ok {
						return
					}
					info, err := fileSystem.Lstat(path)
					if err != nil {
						select {
						case statErrors <- err:
						default:
						}
						cancel()
						return
					}
					if !info.Mode().IsRegular() {
						continue
					}
					result := statResult{snapshot: FileSnapshot{
						Path: path, Size: info.Size(), MTimeNS: info.ModTime().UnixNano(),
						Inode: fsmeta.InodeAt(path, info.Sys()),
					}}
					select {
					case results <- result:
					case <-walkCtx.Done():
						return
					}
				}
			}
		}()
	}

	visitDone := make(chan error, 1)
	go func() {
		var visitErr error
		for result := range results {
			if visitErr != nil || walkCtx.Err() != nil {
				continue
			}
			if err := visit(result.snapshot); err != nil {
				visitErr = err
				cancel()
			}
		}
		visitDone <- visitErr
	}()

	walkErr := filepath.WalkDir(root.Path, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := walkCtx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if path != root.Path && !root.Recursive {
				return filepath.SkipDir
			}
			return nil
		}
		if !root.Recursive {
			relative, err := filepath.Rel(root.Path, path)
			if err != nil || strings.Contains(relative, string(filepath.Separator)) {
				return nil
			}
		}
		select {
		case paths <- path:
			return nil
		case <-walkCtx.Done():
			return walkCtx.Err()
		}
	})
	if walkErr != nil {
		// Stop queued stat/visit work after an invalid partial traversal. Walk
		// still joins every scoped goroutine below before returning the cause.
		cancel()
	}
	close(paths)
	workers.Wait()
	close(results)
	visitErr := <-visitDone

	if visitErr != nil {
		return visitErr
	}
	select {
	case statErr := <-statErrors:
		return statErr
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return walkErr
}
