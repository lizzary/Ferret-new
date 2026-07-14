package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/lizzary/index-node/internal/config"
)

func TestFetchHealthAcceptsReadyAndWarmingFromWildcardListeners(t *testing.T) {
	tests := []struct {
		name       string
		listenHost string
		httpStatus int
		status     string
		ready      bool
	}{
		{name: "IPv4 wildcard ready", listenHost: "0.0.0.0", httpStatus: http.StatusOK, status: "ready", ready: true},
		{name: "empty wildcard warming", listenHost: "", httpStatus: http.StatusServiceUnavailable, status: "warming", ready: false},
		{name: "star wildcard ready", listenHost: "*", httpStatus: http.StatusOK, status: "ready", ready: true},
		{name: "IPv6 wildcard warming", listenHost: "::", httpStatus: http.StatusServiceUnavailable, status: "warming", ready: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/healthz" {
					t.Errorf("request path = %q, want /healthz", request.URL.Path)
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.httpStatus)
				_, _ = fmt.Fprintf(writer, `{"status":%q,"roots":4,"active_roots":2,"pending_roots":1,"degraded_roots":1,"dirty_roots":3}`, test.status)
			}))
			defer server.Close()

			serverURL, err := url.Parse(server.URL)
			if err != nil {
				t.Fatalf("parse test server URL: %v", err)
			}
			_, port, err := net.SplitHostPort(serverURL.Host)
			if err != nil {
				t.Fatalf("split test server address: %v", err)
			}
			cfg := &config.Config{MetricsListen: net.JoinHostPort(test.listenHost, port)}
			status, err := FetchHealth(context.Background(), cfg)
			if err != nil {
				t.Fatalf("FetchHealth: %v", err)
			}
			if status.Status != test.status || status.Ready() != test.ready {
				t.Fatalf("status = %#v, ready=%t; want status %q, ready=%t", status, status.Ready(), test.status, test.ready)
			}
			if status.Roots != 4 || status.ActiveRoots != 2 || status.PendingRoots != 1 || status.DegradedRoots != 1 || status.DirtyRoots != 3 {
				t.Fatalf("aggregate health fields = %#v", status)
			}
		})
	}
}
