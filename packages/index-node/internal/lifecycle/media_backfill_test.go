package lifecycle

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"

	"github.com/lizzary/index-node/internal/pipeline/media"
	"github.com/lizzary/index-node/internal/store"
)

func TestEnqueueLegacyImagesSniffsAndMarksIdempotently(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	processor, err := media.NewImageProcessor(media.ImageConfig{})
	if err != nil {
		t.Fatal(err)
	}

	magicPath := filepath.Join(dataDir, "image-without-extension")
	var encoded bytes.Buffer
	pixels := image.NewRGBA(image.Rect(0, 0, 2, 2))
	for y := 0; y < 2; y++ {
		for x := 0; x < 2; x++ {
			pixels.Set(x, y, color.RGBA{R: 20, G: 40, B: 60, A: 255})
		}
	}
	if err := jpeg.Encode(&encoded, pixels, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(magicPath, encoded.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	damagedPath := filepath.Join(dataDir, "damaged.jpg")
	if err := os.WriteFile(damagedPath, []byte("not an image"), 0o600); err != nil {
		t.Fatal(err)
	}
	textPath := filepath.Join(dataDir, "plain.txt")
	if err := os.WriteFile(textPath, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	indexedAt := int64(123)
	files := make([]store.File, 0, 3)
	for _, path := range []string{magicPath, damagedPath, textPath} {
		file, err := durable.UpsertFile(ctx, store.File{
			Path: path, Size: 1, Kind: store.FileKindOther, Generation: 1,
			Status: store.FileStatusIndexed, IndexedAtMS: &indexedAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	woke := 0
	count, err := enqueueLegacyImages(ctx, durable, processor, 4, func() { woke++ })
	if err != nil || count != 2 || woke != 1 {
		t.Fatalf("enqueueLegacyImages() = count %d woke %d error %v", count, woke, err)
	}
	for _, file := range files[:2] {
		current, err := durable.GetFileByID(ctx, file.ID)
		if err != nil || current.Generation != 2 || current.Status != store.FileStatusPending || current.IndexedAtMS != nil {
			t.Fatalf("backfilled file = %+v, %v", current, err)
		}
	}
	currentText, err := durable.GetFileByID(ctx, files[2].ID)
	if err != nil || currentText.Generation != 1 || currentText.Status != store.FileStatusIndexed {
		t.Fatalf("non-image file = %+v, %v", currentText, err)
	}
	if marker, err := durable.GetMeta(ctx, mediaImageBackfillMarker); err != nil || marker != "complete" {
		t.Fatalf("backfill marker = %q, %v", marker, err)
	}
	count, err = enqueueLegacyImages(ctx, durable, processor, 4, func() { woke++ })
	if err != nil || count != 0 || woke != 1 {
		t.Fatalf("enqueueLegacyImages(repeat) = count %d woke %d error %v", count, woke, err)
	}
}
