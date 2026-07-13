package watch

import (
	"context"
	"errors"
	"fmt"
	"time"

	filecat "github.com/lizzary/filecat-go"
)

// FilecatFactory is the sole adapter to github.com/lizzary/filecat-go. Keeping
// every third-party type in this file lets Manager and its tests stay backend
// neutral.
type FilecatFactory struct{}

func (FilecatFactory) Open(root Root, bufferSize int, coalesceWindow time.Duration) (Watcher, error) {
	backend, err := filecat.NewWatcher(root.Path, root.Recursive, bufferSize, coalesceWindow)
	if err != nil {
		return nil, err
	}
	return &filecatWatcher{
		backend: backend,
		events:  backend.Events(),
		errors:  backend.Errors(),
	}, nil
}

type filecatWatcher struct {
	backend *filecat.Watcher
	events  <-chan filecat.FileEvent
	errors  <-chan error
}

func (watcher *filecatWatcher) Next(ctx context.Context) (BackendEvent, error) {
	if ctx == nil {
		return BackendEvent{}, errors.New("watch: context is required")
	}
	for {
		select {
		case <-ctx.Done():
			return BackendEvent{}, ctx.Err()
		case event, ok := <-watcher.events:
			if !ok {
				return BackendEvent{}, ErrWatcherClosed
			}
			return translateFilecatEvent(event)
		case err, ok := <-watcher.errors:
			if !ok {
				// v1.0.0 never closes Errors, but treating a future closed error
				// channel as disabled avoids a zero-value busy loop.
				watcher.errors = nil
				continue
			}
			if errors.Is(err, filecat.ErrOverflow) {
				return BackendEvent{}, fmt.Errorf("%w: %v", ErrOverflow, err)
			}
			if err == nil {
				return BackendEvent{}, errors.New("watch: filecat returned a nil error")
			}
			return BackendEvent{}, fmt.Errorf("watch: filecat runtime failure: %w", err)
		}
	}
}

func translateFilecatEvent(event filecat.FileEvent) (BackendEvent, error) {
	translated := BackendEvent{Path: event.Path, OldPath: event.OldPath}
	switch event.Type {
	case filecat.EventCreated:
		translated.Op = OpCreated
	case filecat.EventRemoved:
		translated.Op = OpRemoved
	case filecat.EventModified:
		translated.Op = OpModified
	case filecat.EventMove:
		translated.Op = OpMove
	default:
		return BackendEvent{}, fmt.Errorf("%w: filecat event type %d", ErrInvalidEvent, event.Type)
	}
	if translated.Path == "" || (translated.Op == OpMove && translated.OldPath == "") {
		return BackendEvent{}, fmt.Errorf("%w: %s event has incomplete paths", ErrInvalidEvent, translated.Op)
	}
	return translated, nil
}

func (watcher *filecatWatcher) Close() error {
	if watcher == nil || watcher.backend == nil {
		return nil
	}
	return watcher.backend.Close()
}
