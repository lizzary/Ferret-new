package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/lizzary/index-node/internal/store"
)

const (
	mediaImageBackfillMarker = "media_image_backfill_v1"
	mediaBackfillPageSize    = 1000
	mediaBackfillSniffBytes  = 8192
)

type mediaBackfillMatcher interface {
	Match(path string, sniff []byte) bool
}

// enqueueLegacyImages upgrades the pre-M5 kind=other population by durable
// task, never by rewriting type or vector truth in place. The marker is stored
// only after every candidate page has been inspected and every selected task
// has committed.
func enqueueLegacyImages(
	ctx context.Context,
	durable *store.Store,
	matcher mediaBackfillMatcher,
	priority int,
	notify func(),
) (int, error) {
	if ctx == nil || durable == nil || matcher == nil {
		return 0, errors.New("lifecycle: media backfill dependencies are required")
	}
	marker, err := durable.GetMeta(ctx, mediaImageBackfillMarker)
	if err == nil && marker == "complete" {
		return 0, nil
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return 0, fmt.Errorf("lifecycle: read media backfill marker: %w", err)
	}

	cursor := int64(0)
	enqueued := 0
	for {
		candidates, err := durable.ListMediaBackfillCandidates(ctx, cursor, mediaBackfillPageSize)
		if err != nil {
			return enqueued, err
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			cursor = candidate.ID
			sniff := sniffMediaFile(candidate.Path)
			if !matcher.Match(candidate.Path, sniff) {
				continue
			}
			result, err := durable.EnqueueMediaBackfill(ctx, candidate.ID, candidate.Generation, priority)
			if err != nil {
				return enqueued, fmt.Errorf("lifecycle: enqueue legacy image %d: %w", candidate.ID, err)
			}
			if result.Applied {
				enqueued++
			}
		}
		if len(candidates) < mediaBackfillPageSize {
			break
		}
	}
	if err := durable.SetMeta(ctx, mediaImageBackfillMarker, "complete"); err != nil {
		return enqueued, fmt.Errorf("lifecycle: persist media backfill marker: %w", err)
	}
	if enqueued != 0 && notify != nil {
		notify()
	}
	return enqueued, nil
}

func sniffMediaFile(path string) []byte {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	buffer := make([]byte, mediaBackfillSniffBytes)
	count, err := io.ReadFull(file, buffer)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil
	}
	return buffer[:count]
}
