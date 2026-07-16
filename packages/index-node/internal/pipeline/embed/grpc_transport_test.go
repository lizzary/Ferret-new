package embed

import (
	"context"
	"errors"
	"math"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	computev1 "github.com/lizzary/index-node/gen/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCTransportMapsImagesAndText(t *testing.T) {
	imageRequest := make(chan [][]byte, 1)
	textRequest := make(chan string, 1)
	server := &fakeEmbedService{
		embedImages: func(_ context.Context, request *computev1.EmbedImagesRequest) (*computev1.EmbedImagesResponse, error) {
			imageRequest <- cloneBytes(request.Jpeg)
			return &computev1.EmbedImagesResponse{
				Embeddings: []*computev1.Embedding{
					{Values: []float32{1, 2}},
					{Values: []float32{3, 4}},
				},
				Model: &computev1.ModelInfo{ModelVersion: "clip-v1", Dims: 2},
			}, nil
		},
		embedText: func(_ context.Context, request *computev1.EmbedTextRequest) (*computev1.EmbedTextResponse, error) {
			textRequest <- request.Text
			return &computev1.EmbedTextResponse{
				Embedding: &computev1.Embedding{Values: []float32{5, 6}},
				Model:     &computev1.ModelInfo{ModelVersion: "clip-v1", Dims: 2},
			}, nil
		},
	}
	// An explicit credentials option after the default exercises the supported
	// caller-override path while the context dialer supplies the bufconn socket.
	transport := newBufconnTransport(
		t,
		server,
		time.Second,
		true,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	vectors, imageInfo, err := transport.EmbedImages(context.Background(), [][]byte{{1, 2}, {3, 4}})
	if err != nil {
		t.Fatalf("EmbedImages: %v", err)
	}
	if !reflect.DeepEqual(vectors, [][]float32{{1, 2}, {3, 4}}) {
		t.Fatalf("EmbedImages vectors = %#v", vectors)
	}
	if imageInfo != (ModelInfo{Version: "clip-v1", Dims: 2}) {
		t.Fatalf("EmbedImages model = %#v", imageInfo)
	}
	if got := <-imageRequest; !reflect.DeepEqual(got, [][]byte{{1, 2}, {3, 4}}) {
		t.Fatalf("image request = %#v", got)
	}

	vector, textInfo, err := transport.EmbedText(context.Background(), "a red fox")
	if err != nil {
		t.Fatalf("EmbedText: %v", err)
	}
	if !reflect.DeepEqual(vector, []float32{5, 6}) {
		t.Fatalf("EmbedText vector = %#v", vector)
	}
	if textInfo != imageInfo {
		t.Fatalf("EmbedText model = %#v, want %#v", textInfo, imageInfo)
	}
	if got := <-textRequest; got != "a red fox" {
		t.Fatalf("text request = %q", got)
	}
}

func TestGRPCTransportDefaultCredentialsAreExplicitPlaintext(t *testing.T) {
	server := &fakeEmbedService{
		embedText: func(context.Context, *computev1.EmbedTextRequest) (*computev1.EmbedTextResponse, error) {
			return &computev1.EmbedTextResponse{
				Embedding: &computev1.Embedding{Values: []float32{1}},
				Model:     &computev1.ModelInfo{ModelVersion: "model-v1", Dims: 1},
			}, nil
		},
	}
	// No caller credentials are supplied. A successful RPC proves that the
	// constructor installed plaintext transport credentials itself.
	transport := newBufconnTransport(t, server, time.Second, false)
	if _, _, err := transport.EmbedText(context.Background(), "query"); err != nil {
		t.Fatalf("EmbedText with default credentials: %v", err)
	}
}

func TestGRPCTransportAppliesRequestTimeoutToEveryRPC(t *testing.T) {
	const requestTimeout = 100 * time.Millisecond
	tests := []struct {
		name string
		call func(*GRPCTransport) error
	}{
		{
			name: "images",
			call: func(transport *GRPCTransport) error {
				_, _, err := transport.EmbedImages(context.Background(), [][]byte{{1}})
				return err
			},
		},
		{
			name: "text",
			call: func(transport *GRPCTransport) error {
				_, _, err := transport.EmbedText(context.Background(), "query")
				return err
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			deadlineSeen := make(chan time.Duration, 1)
			block := func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				if !ok {
					deadlineSeen <- -1
				} else {
					deadlineSeen <- time.Until(deadline)
				}
				<-ctx.Done()
				return status.FromContextError(ctx.Err()).Err()
			}
			server := &fakeEmbedService{
				embedImages: func(ctx context.Context, _ *computev1.EmbedImagesRequest) (*computev1.EmbedImagesResponse, error) {
					return nil, block(ctx)
				},
				embedText: func(ctx context.Context, _ *computev1.EmbedTextRequest) (*computev1.EmbedTextResponse, error) {
					return nil, block(ctx)
				},
			}
			transport := newBufconnTransport(t, server, requestTimeout, false)

			started := time.Now()
			err := test.call(transport)
			elapsed := time.Since(started)
			if status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("RPC error = %v, code %v", err, status.Code(err))
			}
			if elapsed >= time.Second {
				t.Fatalf("RPC elapsed %v, request timeout was %v", elapsed, requestTimeout)
			}
			select {
			case remaining := <-deadlineSeen:
				if remaining <= 0 || remaining > requestTimeout {
					t.Fatalf("server deadline remaining = %v", remaining)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not observe RPC deadline")
			}
		})
	}
}

func TestGRPCTransportPreservesGRPCStatus(t *testing.T) {
	server := &fakeEmbedService{
		embedText: func(context.Context, *computev1.EmbedTextRequest) (*computev1.EmbedTextResponse, error) {
			return nil, status.Error(codes.Unavailable, "compute warming up")
		},
	}
	transport := newBufconnTransport(t, server, time.Second, false)
	_, _, err := transport.EmbedText(context.Background(), "query")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("status code = %v, error %v", status.Code(err), err)
	}
}

func TestGRPCTransportRejectsMalformedResponses(t *testing.T) {
	t.Run("nil image model", func(t *testing.T) {
		server := &fakeEmbedService{
			embedImages: func(context.Context, *computev1.EmbedImagesRequest) (*computev1.EmbedImagesResponse, error) {
				return &computev1.EmbedImagesResponse{
					Embeddings: []*computev1.Embedding{{Values: []float32{1}}},
				}, nil
			},
		}
		transport := newBufconnTransport(t, server, time.Second, false)
		_, _, err := transport.EmbedImages(context.Background(), [][]byte{{1}})
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("nil text embedding", func(t *testing.T) {
		server := &fakeEmbedService{
			embedText: func(context.Context, *computev1.EmbedTextRequest) (*computev1.EmbedTextResponse, error) {
				return &computev1.EmbedTextResponse{
					Model: &computev1.ModelInfo{ModelVersion: "model-v1", Dims: 1},
				}, nil
			},
		}
		transport := newBufconnTransport(t, server, time.Second, false)
		_, _, err := transport.EmbedText(context.Background(), "query")
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("nil image embedding", func(t *testing.T) {
		if _, err := embeddingsFromProto([]*computev1.Embedding{nil}); !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("dimensions overflow int", func(t *testing.T) {
		_, err := checkedDimensions(uint64(math.MaxInt32)+1, math.MaxInt32)
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want ErrInvalidResponse", err)
		}
	})
}

func TestGRPCTransportCloseIsIdempotent(t *testing.T) {
	transport := newBufconnTransport(t, &fakeEmbedService{}, time.Second, false)

	const closers = 16
	errorsSeen := make(chan error, closers)
	var group sync.WaitGroup
	group.Add(closers)
	for index := 0; index < closers; index++ {
		go func() {
			defer group.Done()
			errorsSeen <- transport.Close()
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	if _, _, err := transport.EmbedText(context.Background(), "query"); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("EmbedText after Close = %v, want ErrClientClosed", err)
	}
	if _, _, err := transport.EmbedImages(context.Background(), [][]byte{{1}}); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("EmbedImages after Close = %v, want ErrClientClosed", err)
	}
	var nilTransport *GRPCTransport
	if err := nilTransport.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
}

func TestGRPCTransportConstructorValidation(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		timeout  time.Duration
	}{
		{name: "blank endpoint", endpoint: "  ", timeout: time.Second},
		{name: "zero timeout", endpoint: "dns:///compute:7801"},
		{name: "negative timeout", endpoint: "dns:///compute:7801", timeout: -time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport, err := DialGRPCTransport(test.endpoint, test.timeout)
			if transport != nil || !errors.Is(err, ErrInvalidGRPCTransportConfig) {
				t.Fatalf("DialGRPCTransport = %#v, %v", transport, err)
			}
		})
	}
}

type fakeEmbedService struct {
	computev1.UnimplementedEmbedServiceServer
	embedImages func(context.Context, *computev1.EmbedImagesRequest) (*computev1.EmbedImagesResponse, error)
	embedText   func(context.Context, *computev1.EmbedTextRequest) (*computev1.EmbedTextResponse, error)
}

func (server *fakeEmbedService) EmbedImages(
	ctx context.Context,
	request *computev1.EmbedImagesRequest,
) (*computev1.EmbedImagesResponse, error) {
	if server.embedImages == nil {
		return nil, status.Error(codes.Unimplemented, "images not configured")
	}
	return server.embedImages(ctx, request)
}

func (server *fakeEmbedService) EmbedText(
	ctx context.Context,
	request *computev1.EmbedTextRequest,
) (*computev1.EmbedTextResponse, error) {
	if server.embedText == nil {
		return nil, status.Error(codes.Unimplemented, "text not configured")
	}
	return server.embedText(ctx, request)
}

func newBufconnTransport(
	t *testing.T,
	service computev1.EmbedServiceServer,
	requestTimeout time.Duration,
	useNewConstructor bool,
	extraOptions ...grpc.DialOption,
) *GRPCTransport {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	computev1.RegisterEmbedServiceServer(server, service)
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	options := make([]grpc.DialOption, 0, len(extraOptions)+1)
	options = append(options, grpc.WithContextDialer(dialer))
	options = append(options, extraOptions...)
	var (
		transport *GRPCTransport
		err       error
	)
	if useNewConstructor {
		transport, err = NewGRPCTransport("passthrough:///bufconn", requestTimeout, options...)
	} else {
		transport, err = DialGRPCTransport("passthrough:///bufconn", requestTimeout, options...)
	}
	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatalf("construct gRPC transport: %v", err)
	}
	t.Cleanup(func() {
		_ = transport.Close()
		server.Stop()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("gRPC server did not stop")
		}
	})
	return transport
}

func cloneBytes(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = append([]byte(nil), value...)
	}
	return cloned
}
