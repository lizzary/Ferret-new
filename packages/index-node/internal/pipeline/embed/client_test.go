package embed

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
)

type scriptedTransport struct {
	image func(context.Context, [][]byte) ([][]float32, ModelInfo, error)
	text  func(context.Context, string) ([]float32, ModelInfo, error)

	mu         sync.Mutex
	closeCalls int
}

func (transport *scriptedTransport) EmbedImages(ctx context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
	if transport.image == nil {
		return nil, ModelInfo{}, errors.New("unexpected EmbedImages call")
	}
	return transport.image(ctx, images)
}

func (transport *scriptedTransport) EmbedText(ctx context.Context, text string) ([]float32, ModelInfo, error) {
	if transport.text == nil {
		return nil, ModelInfo{}, errors.New("unexpected EmbedText call")
	}
	return transport.text(ctx, text)
}

func (transport *scriptedTransport) Close() error {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.closeCalls++
	return nil
}

func TestClientNormalizesAndCopiesImageVectors(t *testing.T) {
	t.Parallel()
	raw := [][]float32{{3, 4}, {0, -2}}
	client, err := NewClient(&scriptedTransport{image: func(context.Context, [][]byte) ([][]float32, ModelInfo, error) {
		return raw, ModelInfo{Version: "clip-v1", Dims: 2}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	vectors, info, err := client.EmbedImages(context.Background(), [][]byte{{1}, {2}})
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != "clip-v1" || info.Dims != 2 {
		t.Fatalf("model info = %+v", info)
	}
	if math.Abs(float64(vectors[0][0])-0.6) > 1e-6 || math.Abs(float64(vectors[0][1])-0.8) > 1e-6 {
		t.Fatalf("normalized vector = %v", vectors[0])
	}
	if vectors[1][0] != 0 || vectors[1][1] != -1 {
		t.Fatalf("normalized vector = %v", vectors[1])
	}
	vectors[0][0] = 99
	if raw[0][0] != 3 {
		t.Fatal("Client exposed the transport response buffer")
	}
}

func TestClientRejectsMalformedResponses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		vectors [][]float32
		info    ModelInfo
		images  int
	}{
		{name: "empty version", vectors: [][]float32{{1}}, info: ModelInfo{Dims: 1}, images: 1},
		{name: "invalid dimensions", vectors: [][]float32{{1}}, info: ModelInfo{Version: "v", Dims: 0}, images: 1},
		{name: "cardinality", vectors: [][]float32{{1}}, info: ModelInfo{Version: "v", Dims: 1}, images: 2},
		{name: "dimension mismatch", vectors: [][]float32{{1}}, info: ModelInfo{Version: "v", Dims: 2}, images: 1},
		{name: "nan", vectors: [][]float32{{float32(math.NaN())}}, info: ModelInfo{Version: "v", Dims: 1}, images: 1},
		{name: "infinity", vectors: [][]float32{{float32(math.Inf(1))}}, info: ModelInfo{Version: "v", Dims: 1}, images: 1},
		{name: "zero norm", vectors: [][]float32{{0, 0}}, info: ModelInfo{Version: "v", Dims: 2}, images: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(&scriptedTransport{image: func(context.Context, [][]byte) ([][]float32, ModelInfo, error) {
				return test.vectors, test.info, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			images := make([][]byte, test.images)
			for index := range images {
				images[index] = []byte{byte(index + 1)}
			}
			if _, _, err := client.EmbedImages(context.Background(), images); !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("EmbedImages() = %v, want ErrInvalidResponse", err)
			}
		})
	}
}

func TestClientTextValidationAndIdempotentClose(t *testing.T) {
	t.Parallel()
	transport := &scriptedTransport{text: func(context.Context, string) ([]float32, ModelInfo, error) {
		return []float32{3, 4}, ModelInfo{Version: "text-v1", Dims: 2}, nil
	}}
	client, err := NewClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	vector, _, err := client.EmbedText(context.Background(), "ferret")
	if err != nil || math.Abs(float64(vector[0])-0.6) > 1e-6 || math.Abs(float64(vector[1])-0.8) > 1e-6 {
		t.Fatalf("EmbedText() = %v, %v", vector, err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	closeCalls := transport.closeCalls
	transport.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("transport Close calls = %d", closeCalls)
	}
	if _, _, err := client.EmbedText(context.Background(), "ferret"); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("EmbedText(after Close) = %v", err)
	}
}
