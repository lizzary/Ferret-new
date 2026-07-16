package embed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	computev1 "github.com/lizzary/index-node/gen/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrInvalidGRPCTransportConfig = errors.New("embed grpc transport: invalid configuration")

// GRPCTransport implements Transport over the compute-node EmbedService.
//
// The current compute endpoint is an internal plaintext endpoint. Dial uses
// explicit insecure transport credentials so gRPC never silently relies on
// legacy defaults. Caller options are applied afterwards and can therefore
// safely add a context dialer or replace the default credentials.
type GRPCTransport struct {
	conn           *grpc.ClientConn
	client         computev1.EmbedServiceClient
	requestTimeout time.Duration

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

var _ Transport = (*GRPCTransport)(nil)

// DialGRPCTransport creates a lazy gRPC client connection. Network connection
// establishment happens on the first RPC and is bounded by that RPC's timeout.
func DialGRPCTransport(
	endpoint string,
	requestTimeout time.Duration,
	options ...grpc.DialOption,
) (*GRPCTransport, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("%w: endpoint is required", ErrInvalidGRPCTransportConfig)
	}
	if requestTimeout <= 0 {
		return nil, fmt.Errorf("%w: request timeout must be greater than zero", ErrInvalidGRPCTransportConfig)
	}

	// Defaults come first so an explicit caller option can override credentials.
	dialOptions := make([]grpc.DialOption, 0, len(options)+1)
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dialOptions = append(dialOptions, options...)
	connection, err := grpc.NewClient(endpoint, dialOptions...)
	if err != nil {
		return nil, err
	}
	return &GRPCTransport{
		conn:           connection,
		client:         computev1.NewEmbedServiceClient(connection),
		requestTimeout: requestTimeout,
	}, nil
}

// NewGRPCTransport is the constructor spelling used by lifecycle wiring.
func NewGRPCTransport(
	endpoint string,
	requestTimeout time.Duration,
	options ...grpc.DialOption,
) (*GRPCTransport, error) {
	return DialGRPCTransport(endpoint, requestTimeout, options...)
}

func (transport *GRPCTransport) EmbedImages(
	ctx context.Context,
	images [][]byte,
) ([][]float32, ModelInfo, error) {
	if err := transport.ready(ctx); err != nil {
		return nil, ModelInfo{}, err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, transport.requestTimeout)
	defer cancel()

	response, err := transport.client.EmbedImages(
		rpcCtx,
		&computev1.EmbedImagesRequest{Jpeg: images},
	)
	if err != nil {
		// Preserve the original gRPC status for breaker classification.
		return nil, ModelInfo{}, err
	}
	if response == nil {
		return nil, ModelInfo{}, invalidGRPCResponse("image response is nil")
	}
	info, err := modelInfoFromProto(response.Model)
	if err != nil {
		return nil, ModelInfo{}, err
	}
	vectors, err := embeddingsFromProto(response.Embeddings)
	if err != nil {
		return nil, ModelInfo{}, err
	}
	return vectors, info, nil
}

func (transport *GRPCTransport) EmbedText(
	ctx context.Context,
	text string,
) ([]float32, ModelInfo, error) {
	if err := transport.ready(ctx); err != nil {
		return nil, ModelInfo{}, err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, transport.requestTimeout)
	defer cancel()

	response, err := transport.client.EmbedText(
		rpcCtx,
		&computev1.EmbedTextRequest{Text: text},
	)
	if err != nil {
		// Preserve the original gRPC status for breaker classification.
		return nil, ModelInfo{}, err
	}
	if response == nil {
		return nil, ModelInfo{}, invalidGRPCResponse("text response is nil")
	}
	info, err := modelInfoFromProto(response.Model)
	if err != nil {
		return nil, ModelInfo{}, err
	}
	if response.Embedding == nil {
		return nil, ModelInfo{}, invalidGRPCResponse("text embedding is nil")
	}
	return append([]float32(nil), response.Embedding.Values...), info, nil
}

func (transport *GRPCTransport) Close() error {
	if transport == nil {
		return nil
	}
	transport.closeOnce.Do(func() {
		transport.closed.Store(true)
		if transport.conn != nil {
			transport.closeErr = transport.conn.Close()
		}
	})
	return transport.closeErr
}

func (transport *GRPCTransport) ready(ctx context.Context) error {
	if transport == nil || transport.conn == nil || transport.client == nil {
		return ErrNilTransport
	}
	if transport.closed.Load() {
		return ErrClientClosed
	}
	if ctx == nil {
		return errors.New("embed grpc transport: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func modelInfoFromProto(model *computev1.ModelInfo) (ModelInfo, error) {
	if model == nil {
		return ModelInfo{}, invalidGRPCResponse("model is nil")
	}
	dimensions, err := checkedDimensions(uint64(model.Dims), uint64(^uint(0)>>1))
	if err != nil {
		return ModelInfo{}, err
	}
	return ModelInfo{Version: model.ModelVersion, Dims: dimensions}, nil
}

func embeddingsFromProto(embeddings []*computev1.Embedding) ([][]float32, error) {
	vectors := make([][]float32, len(embeddings))
	for index, embedding := range embeddings {
		if embedding == nil {
			return nil, invalidGRPCResponse(fmt.Sprintf("image embedding %d is nil", index))
		}
		vectors[index] = append([]float32(nil), embedding.Values...)
	}
	return vectors, nil
}

func checkedDimensions(dimensions, maxInt uint64) (int, error) {
	if dimensions > maxInt {
		return 0, invalidGRPCResponse(fmt.Sprintf("model dimensions %d overflow int", dimensions))
	}
	return int(dimensions), nil
}

func invalidGRPCResponse(problem string) error {
	return &ResponseError{Problem: "gRPC: " + problem}
}
