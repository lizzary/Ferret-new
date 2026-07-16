package index_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	computev1 "github.com/lizzary/index-node/gen/compute/v1"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/pipeline/embed"
	"github.com/lizzary/index-node/internal/pipeline/media"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const acceptanceModel = "deterministic-colors-v1"

type deterministicEmbedService struct {
	computev1.UnimplementedEmbedServiceServer
}

func (deterministicEmbedService) EmbedImages(
	_ context.Context,
	request *computev1.EmbedImagesRequest,
) (*computev1.EmbedImagesResponse, error) {
	embeddings := make([]*computev1.Embedding, len(request.GetJpeg()))
	for position, encoded := range request.GetJpeg() {
		decoded, err := jpeg.Decode(bytes.NewReader(encoded))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "decode image %d: %v", position, err)
		}
		red, _, blue, _ := decoded.At(decoded.Bounds().Min.X, decoded.Bounds().Min.Y).RGBA()
		values := []float32{0, 1}
		if red > blue {
			values = []float32{1, 0}
		}
		embeddings[position] = &computev1.Embedding{Values: values}
	}
	return &computev1.EmbedImagesResponse{
		Embeddings: embeddings,
		Model:      &computev1.ModelInfo{ModelVersion: acceptanceModel, Dims: 2},
	}, nil
}

func (deterministicEmbedService) EmbedText(
	_ context.Context,
	request *computev1.EmbedTextRequest,
) (*computev1.EmbedTextResponse, error) {
	values := []float32{0, 1}
	if strings.Contains(strings.ToLower(request.GetText()), "red") {
		values = []float32{1, 0}
	}
	return &computev1.EmbedTextResponse{
		Embedding: &computev1.Embedding{Values: values},
		Model:     &computev1.ModelInfo{ModelVersion: acceptanceModel, Dims: 2},
	}, nil
}

type batcherQueryEmbedder struct{ batcher *embed.Batcher }

func (adapter batcherQueryEmbedder) EmbedText(ctx context.Context, text string) (index.QueryEmbedding, error) {
	values, model, err := adapter.batcher.EmbedText(ctx, text)
	if err != nil {
		return index.QueryEmbedding{}, err
	}
	return index.QueryEmbedding{Values: values, ModelVersion: model.Version}, nil
}

type fixedKeywordSearcher struct{ fileID int64 }

func (searcher fixedKeywordSearcher) SearchKeyword(context.Context, string, int) ([]index.KeywordHit, error) {
	return []index.KeywordHit{{FileID: searcher.fileID, Content: "red fallback", Score: 1}}, nil
}

func TestM5AcceptanceDeterministicSemanticDegradeAndRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	computev1.RegisterEmbedServiceServer(grpcServer, deterministicEmbedService{})
	serveGroup := new(errgroup.Group)
	serveGroup.Go(func() error {
		err := grpcServer.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	})
	serverStopped := false
	defer func() {
		if !serverStopped {
			grpcServer.Stop()
			if err := serveGroup.Wait(); err != nil {
				t.Errorf("mock EmbedService: %v", err)
			}
		}
	}()

	transport, err := embed.NewGRPCTransport(
		"passthrough:///m5-acceptance", time.Second,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := embed.NewController(durable, embed.Config{
		Failures: 1, OpenFor: 10 * time.Millisecond,
		IsFailure: func(err error) bool {
			code := status.Code(err)
			return code == codes.Unavailable || code == codes.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	batcher, err := embed.NewBatcher(transport, controller, embed.BatcherConfig{
		BatchSize: 2, BatchLinger: 5 * time.Millisecond, InflightBatches: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(dataDir, "vector.snapshot")
	vectorConfig := index.VectorIndexConfig{
		M: 8, EFConstruction: 32, EFSearch: 16, SnapshotPath: snapshotPath,
		SnapshotInterval: time.Hour, SnapshotChanges: 1000,
	}
	vectors, _, err := index.OpenVectorIndex(ctx, durable, vectorConfig)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	components, componentCtx := errgroup.WithContext(runCtx)
	components.Go(func() error { return controller.Run(componentCtx) })
	components.Go(func() error { return batcher.Run(componentCtx) })
	components.Go(func() error { return vectors.Run(componentCtx) })
	componentsStopped := false
	defer func() {
		if !componentsStopped {
			cancelRun()
			if err := components.Wait(); err != nil {
				t.Errorf("M5 components: %v", err)
			}
		}
		if err := batcher.Close(); err != nil {
			t.Errorf("close embed batcher: %v", err)
		}
	}()

	imageProcessor, err := media.NewImageProcessor(media.ImageConfig{Size: 16, JPEGQuality: 90})
	if err != nil {
		t.Fatal(err)
	}
	redID := indexAcceptanceImage(t, ctx, durable, imageProcessor, batcher, vectors,
		filepath.Join(dataDir, "red.jpg"), solidJPEG(t, color.RGBA{R: 240, G: 10, B: 10, A: 255}))
	blueID := indexAcceptanceImage(t, ctx, durable, imageProcessor, batcher, vectors,
		filepath.Join(dataDir, "blue.jpg"), solidJPEG(t, color.RGBA{R: 10, G: 10, B: 240, A: 255}))

	semantic, err := index.NewSearchService(nil, durable, batcherQueryEmbedder{batcher}, vectors, index.SearchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := semantic.Search(ctx, index.SearchRequest{Query: "red", Mode: index.ModeSemantic, TopK: 2})
	if err != nil || len(response.Hits) != 2 || response.Hits[0].FileID != redID || response.Hits[1].FileID != blueID {
		t.Fatalf("semantic red search = %+v, %v", response, err)
	}

	grpcServer.Stop()
	serverStopped = true
	if err := serveGroup.Wait(); err != nil {
		t.Fatal(err)
	}
	hybrid, err := index.NewSearchService(
		fixedKeywordSearcher{fileID: redID}, durable, batcherQueryEmbedder{batcher}, vectors,
		index.SearchConfig{IsSemanticUnavailable: func(err error) bool { return status.Code(err) == codes.Unavailable }},
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err = hybrid.Search(ctx, index.SearchRequest{Query: "red", Mode: index.ModeHybrid, TopK: 2})
	if err != nil || !response.DegradedSemantic || len(response.Hits) != 1 || response.Hits[0].FileID != redID {
		t.Fatalf("degraded hybrid search = %+v, %v", response, err)
	}

	cancelRun()
	if err := components.Wait(); err != nil {
		t.Fatal(err)
	}
	componentsStopped = true
	if err := batcher.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, recovery, err := index.OpenVectorIndex(ctx, durable, vectorConfig)
	if err != nil || !recovery.ImportedSnapshot || recovery.Rebuilt {
		t.Fatalf("vector restart = %+v, %v", recovery, err)
	}
	hits, err := recovered.Search(ctx, []float32{1, 0}, acceptanceModel, 2)
	if err != nil || len(hits) != 2 || hits[0].FileID != redID || hits[1].FileID != blueID {
		t.Fatalf("restarted vector search = %+v, %v", hits, err)
	}
}

func indexAcceptanceImage(
	t *testing.T,
	ctx context.Context,
	durable *store.Store,
	processor *media.ImageProcessor,
	batcher *embed.Batcher,
	vectors *index.VectorIndex,
	path string,
	encoded []byte,
) int64 {
	t.Helper()
	enqueued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != enqueued.Task.ID {
		t.Fatalf("claim image task = %+v, %v", claimed, err)
	}
	frame, err := processor.Process(ctx, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := durable.PrepareFileForTask(ctx, enqueued.Task.ID, store.File{
		Path: path, Size: int64(len(encoded)), MTimeNS: 1, Kind: store.FileKindImage,
		Generation: 1, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	embeddings, err := batcher.EmbedImagesForTask(ctx, enqueued.Task.ID, []pipeline.Frame{frame})
	if err != nil || len(embeddings) != 1 {
		t.Fatalf("embed image = %+v, %v", embeddings, err)
	}
	vector := store.Vector{
		FileID: prepared.ID, FrameIndex: embeddings[0].FrameIndex,
		FrameTSMS: embeddings[0].FrameTSMS, Values: embeddings[0].Values,
		ModelVersion: embeddings[0].ModelVersion,
	}
	if err := vectors.Replace(ctx, prepared.ID, prepared.Generation, []store.Vector{vector}); err != nil {
		t.Fatal(err)
	}
	indexedAt := time.Now().UnixMilli()
	if err := durable.CompleteTask(ctx, store.CompleteTaskParams{
		TaskID: enqueued.Task.ID, FileID: prepared.ID, Generation: prepared.Generation,
		Status: store.FileStatusIndexed, IndexedAtMS: &indexedAt,
		EmbedModelVersion: &embeddings[0].ModelVersion,
	}); err != nil {
		t.Fatal(err)
	}
	return prepared.ID
}

func solidJPEG(t *testing.T, fill color.RGBA) []byte {
	t.Helper()
	pixels := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			pixels.SetRGBA(x, y, fill)
		}
	}
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, pixels, &jpeg.Options{Quality: 100}); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}
