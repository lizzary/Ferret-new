package embed

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrNilTransport    = errors.New("embed client: transport is required")
	ErrInvalidResponse = errors.New("embed client: invalid response")
	ErrClientClosed    = errors.New("embed client: closed")
	ErrEmptyImages     = errors.New("embed client: image batch is empty")
	ErrEmptyText       = errors.New("embed client: text is empty")
)

// ModelInfo is the compute service's persistence handshake. Version identifies
// the model space and Dims is the exact vector width for that version.
type ModelInfo struct {
	Version string
	Dims    int
}

// Transport is the wire boundary. Implementations may use gRPC, an in-process
// service, or a test double; Client owns response validation and normalization.
type Transport interface {
	EmbedImages(context.Context, [][]byte) ([][]float32, ModelInfo, error)
	EmbedText(context.Context, string) ([]float32, ModelInfo, error)
	Close() error
}

// ResponseError reports a compute response that cannot safely be persisted.
// It matches ErrInvalidResponse while retaining a precise diagnostic.
type ResponseError struct{ Problem string }

func (failure *ResponseError) Error() string {
	if failure == nil || failure.Problem == "" {
		return ErrInvalidResponse.Error()
	}
	return fmt.Sprintf("%s: %s", ErrInvalidResponse, failure.Problem)
}

func (*ResponseError) Is(target error) bool { return target == ErrInvalidResponse }

// Client validates the transport contract and returns defensive, L2-normalized
// vector copies. Transport-owned response buffers are never exposed upstream.
type Client struct {
	transport Transport
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func NewClient(transport Transport) (*Client, error) {
	if transport == nil {
		return nil, ErrNilTransport
	}
	return &Client{transport: transport}, nil
}

func (client *Client) EmbedImages(ctx context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
	if client == nil || client.transport == nil {
		return nil, ModelInfo{}, ErrNilTransport
	}
	if client.closed.Load() {
		return nil, ModelInfo{}, ErrClientClosed
	}
	if ctx == nil {
		return nil, ModelInfo{}, errors.New("embed client: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, ModelInfo{}, err
	}
	if len(images) == 0 {
		return nil, ModelInfo{}, ErrEmptyImages
	}
	for index, image := range images {
		if len(image) == 0 {
			return nil, ModelInfo{}, fmt.Errorf("embed client: image %d is empty", index)
		}
	}
	vectors, info, err := client.transport.EmbedImages(ctx, images)
	if err != nil {
		return nil, ModelInfo{}, err
	}
	normalized, err := validateAndNormalize(vectors, info, len(images))
	if err != nil {
		return nil, ModelInfo{}, err
	}
	return normalized, info, nil
}

func (client *Client) EmbedText(ctx context.Context, text string) ([]float32, ModelInfo, error) {
	if client == nil || client.transport == nil {
		return nil, ModelInfo{}, ErrNilTransport
	}
	if client.closed.Load() {
		return nil, ModelInfo{}, ErrClientClosed
	}
	if ctx == nil {
		return nil, ModelInfo{}, errors.New("embed client: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, ModelInfo{}, err
	}
	if strings.TrimSpace(text) == "" {
		return nil, ModelInfo{}, ErrEmptyText
	}
	vector, info, err := client.transport.EmbedText(ctx, text)
	if err != nil {
		return nil, ModelInfo{}, err
	}
	normalized, err := validateAndNormalize([][]float32{vector}, info, 1)
	if err != nil {
		return nil, ModelInfo{}, err
	}
	return normalized[0], info, nil
}

func (client *Client) Close() error {
	if client == nil || client.transport == nil {
		return nil
	}
	client.closeOnce.Do(func() {
		client.closed.Store(true)
		client.closeErr = client.transport.Close()
	})
	return client.closeErr
}

func validateAndNormalize(vectors [][]float32, info ModelInfo, expected int) ([][]float32, error) {
	trimmedVersion := strings.TrimSpace(info.Version)
	if trimmedVersion == "" {
		return nil, &ResponseError{Problem: "model version is empty"}
	}
	if trimmedVersion != info.Version {
		return nil, &ResponseError{Problem: "model version has leading or trailing whitespace"}
	}
	if info.Dims <= 0 {
		return nil, &ResponseError{Problem: fmt.Sprintf("model dimensions %d are not positive", info.Dims)}
	}
	if len(vectors) != expected {
		return nil, &ResponseError{Problem: fmt.Sprintf("cardinality %d does not match request cardinality %d", len(vectors), expected)}
	}

	normalized := make([][]float32, len(vectors))
	for vectorIndex, vector := range vectors {
		if len(vector) != info.Dims {
			return nil, &ResponseError{Problem: fmt.Sprintf("vector %d has %d dimensions, want %d", vectorIndex, len(vector), info.Dims)}
		}
		var squaredNorm float64
		for valueIndex, value := range vector {
			asFloat64 := float64(value)
			if math.IsNaN(asFloat64) || math.IsInf(asFloat64, 0) {
				return nil, &ResponseError{Problem: fmt.Sprintf("vector %d value %d is not finite", vectorIndex, valueIndex)}
			}
			squaredNorm += asFloat64 * asFloat64
		}
		if squaredNorm == 0 || math.IsNaN(squaredNorm) || math.IsInf(squaredNorm, 0) {
			return nil, &ResponseError{Problem: fmt.Sprintf("vector %d has invalid L2 norm", vectorIndex)}
		}
		inverseNorm := 1 / math.Sqrt(squaredNorm)
		copyOfVector := make([]float32, len(vector))
		for valueIndex, value := range vector {
			normalizedValue := float32(float64(value) * inverseNorm)
			if math.IsNaN(float64(normalizedValue)) || math.IsInf(float64(normalizedValue), 0) {
				return nil, &ResponseError{Problem: fmt.Sprintf("vector %d value %d cannot be normalized", vectorIndex, valueIndex)}
			}
			copyOfVector[valueIndex] = normalizedValue
		}
		normalized[vectorIndex] = copyOfVector
	}
	return normalized, nil
}
