package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	want := Config{
		DataDir:       "/var/lib/indexnode",
		GRPCListen:    "0.0.0.0:7701",
		MetricsListen: "127.0.0.1:7702",
		Watch: WatchConfig{
			BufferSize:   4096,
			SettleWindow: time.Second,
		},
		Compute: ComputeConfig{
			Endpoint:        "dns:///compute:7801",
			BatchSize:       32,
			BatchLinger:     100 * time.Millisecond,
			InflightBatches: 8,
			Breaker:         BreakerConfig{Failures: 5, OpenFor: 30 * time.Second},
		},
		Pipeline: PipelineConfig{
			IOBytesInflight: 256 << 20,
			CPUPercentCap:   50,
			MaxFileSize:     512 << 20,
			MaxExtractBytes: 2 << 20,
			VideoFrames:     5,
			FFmpegPath:      "ffmpeg",
		},
		Index: IndexConfig{
			CommitMaxOps:   1000,
			CommitInterval: 3 * time.Second,
			Vector: VectorConfig{
				M:                16,
				EFConstruction:   200,
				EFSearch:         64,
				SnapshotInterval: 10 * time.Minute,
			},
		},
		Retry: RetryConfig{
			Base:                 5 * time.Second,
			Cap:                  30 * time.Minute,
			MaxAttemptsTransient: 8,
			RetryBudgetRatio:     0.2,
		},
		DeadLetter: DeadLetterConfig{RetentionDays: 90},
		Reconcile:  ReconcileConfig{Periodic: 24 * time.Hour},
		Notes:      NotesConfig{OnFileDelete: "orphan"},
		Resources:  ResourcesConfig{MinFreeBytes: 2 << 30},
		Log:        LogConfig{Level: "info", RetainDays: 7},
	}

	got := Default()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Default() mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Default().Validate() = %v", err)
	}
	if got.Watch.Roots != nil {
		t.Fatalf("Default().Watch.Roots = %#v, want nil", got.Watch.Roots)
	}
}

func TestLoadLayersFileAndEnvironment(t *testing.T) {
	dataDir := t.TempDir()
	configPath := writeConfig(t, fmt.Sprintf(`
data_dir: %q
watch:
  buffer_size: 2048
  roots:
    - {path: /from-file, recursive: true}
compute:
  batch_size: 16
log:
  level: debug
`, filepath.ToSlash(dataDir)))

	t.Setenv("INDEXNODE_WATCH_BUFFER_SIZE", "8192")
	t.Setenv("INDEXNODE_WATCH_ROOTS", `[{path: /from-env, recursive: false}]`)
	t.Setenv("INDEXNODE_COMPUTE_BREAKER", `{failures: 7, open_for: 40s}`)
	t.Setenv("INDEXNODE_COMPUTE_BREAKER_OPEN_FOR", "45s")
	t.Setenv("INDEXNODE_LOG_REDACT_PATHS", "true")

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if cfg.Watch.BufferSize != 8192 {
		t.Errorf("Watch.BufferSize = %d, want 8192", cfg.Watch.BufferSize)
	}
	if want := []WatchRoot{{Path: "/from-env", Recursive: false}}; !reflect.DeepEqual(cfg.Watch.Roots, want) {
		t.Errorf("Watch.Roots = %#v, want %#v", cfg.Watch.Roots, want)
	}
	if cfg.Compute.BatchSize != 16 {
		t.Errorf("Compute.BatchSize = %d, want file value 16", cfg.Compute.BatchSize)
	}
	if cfg.Compute.Breaker.Failures != 7 || cfg.Compute.Breaker.OpenFor != 45*time.Second {
		t.Errorf("Compute.Breaker = %#v, want failures=7 and open_for=45s", cfg.Compute.Breaker)
	}
	if !cfg.Log.RedactPaths || cfg.Log.Level != "debug" {
		t.Errorf("Log = %#v, want file and environment values", cfg.Log)
	}
	if cfg.Pipeline.MaxFileSize != 512<<20 {
		t.Errorf("Pipeline.MaxFileSize = %d, want untouched default", cfg.Pipeline.MaxFileSize)
	}

	uuidPattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidPattern.MatchString(cfg.NodeID) {
		t.Fatalf("NodeID = %q, want UUIDv4", cfg.NodeID)
	}
	persisted, err := os.ReadFile(filepath.Join(dataDir, NodeIDFilename))
	if err != nil {
		t.Fatalf("read persisted node ID: %v", err)
	}
	if strings.TrimSpace(string(persisted)) != cfg.NodeID {
		t.Fatalf("persisted node ID = %q, loaded = %q", persisted, cfg.NodeID)
	}

	cfgAgain, err := Load(configPath)
	if err != nil {
		t.Fatalf("second Load() = %v", err)
	}
	if cfgAgain.NodeID != cfg.NodeID {
		t.Fatalf("second NodeID = %q, first = %q", cfgAgain.NodeID, cfg.NodeID)
	}
}

func TestExampleConfigurationLoads(t *testing.T) {
	t.Setenv("INDEXNODE_NODE_ID", "example-config-test")
	t.Setenv("INDEXNODE_DATA_DIR", filepath.ToSlash(t.TempDir()))

	cfg, err := Load(filepath.Join("..", "..", "configs", "indexnode.example.yaml"))
	if err != nil {
		t.Fatalf("Load(example) = %v", err)
	}
	if cfg.NodeID != "example-config-test" {
		t.Fatalf("NodeID = %q", cfg.NodeID)
	}
	if len(cfg.Watch.Roots) != 1 || cfg.Watch.Roots[0].Path != "/home/user/docs" {
		t.Fatalf("Watch.Roots = %#v", cfg.Watch.Roots)
	}
	if cfg.Index.Vector.SnapshotInterval != 10*time.Minute {
		t.Fatalf("snapshot interval = %v", cfg.Index.Vector.SnapshotInterval)
	}
}

func TestLoadExplicitNodeIDDoesNotPersistGeneratedID(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "not-created")
	configPath := writeConfig(t, fmt.Sprintf("node_id: rack-a-node-7\ndata_dir: %q\n", filepath.ToSlash(dataDir)))

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if cfg.NodeID != "rack-a-node-7" {
		t.Fatalf("NodeID = %q", cfg.NodeID)
	}
	if _, err := os.Stat(filepath.Join(dataDir, NodeIDFilename)); !os.IsNotExist(err) {
		t.Fatalf("generated node ID file exists for explicit ID, stat error = %v", err)
	}
}

func TestLoadNodeIDIsStableUnderConcurrency(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	configPath := writeConfig(t, fmt.Sprintf("data_dir: %q\n", filepath.ToSlash(dataDir)))

	const callers = 12
	results := make(chan string, callers)
	errors := make(chan error, callers)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			cfg, err := Load(configPath)
			if err != nil {
				errors <- err
				return
			}
			results <- cfg.NodeID
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errors)

	for err := range errors {
		t.Errorf("concurrent Load() = %v", err)
	}
	var first string
	for id := range results {
		if first == "" {
			first = id
		}
		if id != first {
			t.Errorf("concurrent NodeID = %q, want %q", id, first)
		}
	}
	if first == "" {
		t.Fatal("no concurrent Load succeeded")
	}
}

func TestLoadRejectsStrictYAML(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "unknown field", content: "watc: {}\n", want: "field watc not found"},
		{name: "wrong type", content: "watch:\n  buffer_size: many\n", want: "cannot unmarshal"},
		{name: "multiple documents", content: "log: {level: info}\n---\nlog: {level: debug}\n", want: "multiple YAML documents"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, test.content))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	t.Run("unknown variable", func(t *testing.T) {
		t.Setenv("INDEXNODE_WATCH_BUFFFER_SIZE", "1")
		_, err := Load(writeConfig(t, "node_id: explicit\n"))
		if err == nil || !strings.Contains(err.Error(), "unknown configuration field") {
			t.Fatalf("Load() error = %v", err)
		}
	})

	t.Run("invalid duration", func(t *testing.T) {
		t.Setenv("INDEXNODE_RETRY_BASE", "soon")
		_, err := Load(writeConfig(t, "node_id: explicit\n"))
		if err == nil || !strings.Contains(err.Error(), "INDEXNODE_RETRY_BASE") {
			t.Fatalf("Load() error = %v", err)
		}
	})

	t.Run("unknown aggregate field", func(t *testing.T) {
		t.Setenv("INDEXNODE_COMPUTE_BREAKER", `{failures: 3, typo: 1}`)
		_, err := Load(writeConfig(t, "node_id: explicit\n"))
		if err == nil || !strings.Contains(err.Error(), "field typo not found") {
			t.Fatalf("Load() error = %v", err)
		}
	})
}

func TestValidateReportsQualifiedFields(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{name: "node id", change: func(c *Config) { c.NodeID = " bad" }, want: "node_id"},
		{name: "data dir", change: func(c *Config) { c.DataDir = "" }, want: "data_dir"},
		{name: "grpc address", change: func(c *Config) { c.GRPCListen = "7701" }, want: "grpc_listen"},
		{name: "same listener", change: func(c *Config) { c.MetricsListen = c.GRPCListen }, want: "metrics_listen"},
		{name: "empty root", change: func(c *Config) { c.Watch.Roots = []WatchRoot{{}} }, want: "watch.roots[0].path"},
		{name: "duplicate root", change: func(c *Config) { c.Watch.Roots = []WatchRoot{{Path: "/a/b"}, {Path: "/a/./b"}} }, want: "duplicates"},
		{name: "overlapping roots", change: func(c *Config) { c.Watch.Roots = []WatchRoot{{Path: "/a"}, {Path: "/a/b"}} }, want: "overlaps"},
		{name: "watch buffer", change: func(c *Config) { c.Watch.BufferSize = 0 }, want: "watch.buffer_size"},
		{name: "compute batch", change: func(c *Config) { c.Compute.BatchSize = 0 }, want: "compute.batch_size"},
		{name: "compute breaker", change: func(c *Config) { c.Compute.Breaker.OpenFor = 0 }, want: "compute.breaker.open_for"},
		{name: "io concurrency", change: func(c *Config) { c.Pipeline.IOConcurrency = -1 }, want: "pipeline.io_concurrency"},
		{name: "cpu cap", change: func(c *Config) { c.Pipeline.CPUPercentCap = 101 }, want: "pipeline.cpu_percent_cap"},
		{name: "video frames", change: func(c *Config) { c.Pipeline.VideoFrames = 1 << 16 }, want: "pipeline.video_frames"},
		{name: "commit interval", change: func(c *Config) { c.Index.CommitInterval = 0 }, want: "index.commit_interval"},
		{name: "vector construction", change: func(c *Config) { c.Index.Vector.EFConstruction = c.Index.Vector.M - 1 }, want: "index.vector.ef_construction"},
		{name: "retry cap", change: func(c *Config) { c.Retry.Cap = c.Retry.Base - time.Nanosecond }, want: "retry.cap"},
		{name: "retry ratio", change: func(c *Config) { c.Retry.RetryBudgetRatio = math.NaN() }, want: "retry.retry_budget_ratio"},
		{name: "retry ratio zero", change: func(c *Config) { c.Retry.RetryBudgetRatio = 0 }, want: "retry.retry_budget_ratio"},
		{name: "retry ratio one", change: func(c *Config) { c.Retry.RetryBudgetRatio = 1 }, want: "retry.retry_budget_ratio"},
		{name: "dead letter", change: func(c *Config) { c.DeadLetter.RetentionDays = 0 }, want: "dead_letter.retention_days"},
		{name: "dead letter overflow", change: func(c *Config) { c.DeadLetter.RetentionDays = int(int64(math.MaxInt64)/int64(24*time.Hour) + 1) }, want: "dead_letter.retention_days"},
		{name: "reconcile", change: func(c *Config) { c.Reconcile.Periodic = 0 }, want: "reconcile.periodic"},
		{name: "notes", change: func(c *Config) { c.Notes.OnFileDelete = "delete" }, want: "notes.on_file_delete"},
		{name: "resources", change: func(c *Config) { c.Resources.MinFreeBytes = -1 }, want: "resources.min_free_bytes"},
		{name: "log level", change: func(c *Config) { c.Log.Level = "verbose" }, want: "log.level"},
		{name: "log retention", change: func(c *Config) { c.Log.RetainDays = 0 }, want: "log.retain_days"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Default()
			test.change(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want field %q", err, test.want)
			}
		})
	}
}

func TestLoadValidatesBeforePersistingNodeID(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	path := writeConfig(t, fmt.Sprintf("data_dir: %q\nwatch: {buffer_size: 0}\n", filepath.ToSlash(dataDir)))

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "watch.buffer_size") {
		t.Fatalf("Load() error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, NodeIDFilename)); !os.IsNotExist(statErr) {
		t.Fatalf("node ID persisted for invalid config, stat error = %v", statErr)
	}
}

func TestLoadRejectsCorruptPersistedNodeID(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, NodeIDFilename), []byte("bad-id\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := writeConfig(t, fmt.Sprintf("data_dir: %q\n", filepath.ToSlash(dataDir)))

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "node ID file") {
		t.Fatalf("Load() error = %v, want corrupt node ID error", err)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "indexnode.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
