package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lizzary/index-node/internal/config"
)

// NodeStatus is the complete M2-M4 /healthz response. It intentionally
// contains aggregate counts only; watch-root paths never cross this boundary.
type NodeStatus struct {
	Status        string `json:"status"`
	Roots         int    `json:"roots"`
	ActiveRoots   int    `json:"active_roots"`
	PendingRoots  int    `json:"pending_roots"`
	DegradedRoots int    `json:"degraded_roots"`
	DirtyRoots    int    `json:"dirty_roots"`
}

// Ready reports the lifecycle's authoritative health state.
func (status NodeStatus) Ready() bool { return status.Status == "ready" }

const maxHealthResponseBytes = 64 << 10

// FetchHealth requests the node-local health endpoint. Wildcard listener hosts
// are normalized to loopback because they are bind targets, not dial targets.
// A decoded 503 warming/degraded response is valid health data, not a transport
// error.
func FetchHealth(ctx context.Context, cfg *config.Config) (NodeStatus, error) {
	if ctx == nil {
		return NodeStatus{}, errors.New("health context is required")
	}
	if cfg == nil {
		return NodeStatus{}, errors.New("health configuration is required")
	}
	endpoint, err := healthEndpoint(cfg.MetricsListen)
	if err != nil {
		return NodeStatus{}, err
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return NodeStatus{}, fmt.Errorf("build health request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return NodeStatus{}, fmt.Errorf("request node health: %w", err)
	}
	defer response.Body.Close()

	limited := io.LimitReader(response.Body, maxHealthResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return NodeStatus{}, fmt.Errorf("read node health: %w", err)
	}
	if len(body) > maxHealthResponseBytes {
		return NodeStatus{}, errors.New("node health response is too large")
	}
	var status NodeStatus
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	if err := decoder.Decode(&status); err != nil {
		return NodeStatus{}, fmt.Errorf("decode node health (HTTP %d): %w", response.StatusCode, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return NodeStatus{}, errors.New("decode node health: multiple JSON values")
		}
		return NodeStatus{}, fmt.Errorf("decode node health trailer: %w", err)
	}
	if strings.TrimSpace(status.Status) == "" {
		return NodeStatus{}, errors.New("decode node health: status is empty")
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return NodeStatus{}, fmt.Errorf("node health returned HTTP %d (%s)", response.StatusCode, status.Status)
	}
	return status, nil
}

func healthEndpoint(listen string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(listen))
	if err != nil {
		return "", fmt.Errorf("parse metrics_listen %q: %w", listen, err)
	}
	switch strings.ToLower(strings.TrimSpace(host)) {
	case "", "*", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	if strings.TrimSpace(port) == "" {
		return "", fmt.Errorf("parse metrics_listen %q: port is empty", listen)
	}
	endpoint := url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/healthz"}
	return endpoint.String(), nil
}
