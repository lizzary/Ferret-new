// Package config loads and validates index-node configuration.
package config

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	// EnvironmentPrefix is prepended to upper-case, underscore-separated YAML
	// field paths. For example, watch.buffer_size is overridden by
	// INDEXNODE_WATCH_BUFFER_SIZE.
	EnvironmentPrefix = "INDEXNODE_"

	// ConfigEnvironmentVariable selects the YAML file loaded by the process.
	// It is launcher metadata rather than a Config field, so applyEnvironment
	// permits it without attempting to decode it into Config.
	ConfigEnvironmentVariable = "INDEXNODE_CONFIG"

	// NodeIDFilename is the file used when node_id is not explicitly configured.
	NodeIDFilename = "node_id"
)

// PathFromEnvironment returns the YAML path selected by
// INDEXNODE_CONFIG. Surrounding whitespace is ignored so an empty or
// whitespace-only value consistently means that no YAML file was selected.
func PathFromEnvironment() string {
	return strings.TrimSpace(os.Getenv(ConfigEnvironmentVariable))
}

// Config is the complete index-node configuration described in section 9 of
// the development specification.
type Config struct {
	NodeID        string           `yaml:"node_id"`
	DataDir       string           `yaml:"data_dir"`
	GRPCListen    string           `yaml:"grpc_listen"`
	MetricsListen string           `yaml:"metrics_listen"`
	Watch         WatchConfig      `yaml:"watch"`
	Compute       ComputeConfig    `yaml:"compute"`
	Pipeline      PipelineConfig   `yaml:"pipeline"`
	Index         IndexConfig      `yaml:"index"`
	Retry         RetryConfig      `yaml:"retry"`
	DeadLetter    DeadLetterConfig `yaml:"dead_letter"`
	Reconcile     ReconcileConfig  `yaml:"reconcile"`
	Notes         NotesConfig      `yaml:"notes"`
	Resources     ResourcesConfig  `yaml:"resources"`
	Log           LogConfig        `yaml:"log"`
}

type WatchConfig struct {
	Roots        []WatchRoot   `yaml:"roots"`
	BufferSize   int           `yaml:"buffer_size"`
	SettleWindow time.Duration `yaml:"settle_window"`
}

type WatchRoot struct {
	Path      string `yaml:"path"`
	Recursive bool   `yaml:"recursive"`
}

type ComputeConfig struct {
	Endpoint        string        `yaml:"endpoint"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
	BatchSize       int           `yaml:"batch_size"`
	BatchLinger     time.Duration `yaml:"batch_linger"`
	InflightBatches int           `yaml:"inflight_batches"`
	Breaker         BreakerConfig `yaml:"breaker"`
}

type BreakerConfig struct {
	Failures int           `yaml:"failures"`
	OpenFor  time.Duration `yaml:"open_for"`
}

type PipelineConfig struct {
	IOConcurrency      int    `yaml:"io_concurrency"`
	IOBytesInflight    int64  `yaml:"io_bytes_inflight"`
	CPUPercentCap      int    `yaml:"cpu_percent_cap"`
	MaxFileSize        int64  `yaml:"max_file_size"`
	MaxExtractBytes    int64  `yaml:"max_extract_bytes"`
	ImageSize          int    `yaml:"image_size"`
	ImageJPEGQuality   int    `yaml:"image_jpeg_quality"`
	ImageMaxPixels     int64  `yaml:"image_max_pixels"`
	ImageBytesInflight int64  `yaml:"image_bytes_inflight"`
	VideoFrames        int    `yaml:"video_frames"`
	FFmpegPath         string `yaml:"ffmpeg_path"`
}

type IndexConfig struct {
	CommitMaxOps   int           `yaml:"commit_max_ops"`
	CommitInterval time.Duration `yaml:"commit_interval"`
	Vector         VectorConfig  `yaml:"vector"`
}

type VectorConfig struct {
	M                int           `yaml:"m"`
	EFConstruction   int           `yaml:"ef_construction"`
	EFSearch         int           `yaml:"ef_search"`
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
	SnapshotChanges  int           `yaml:"snapshot_changes"`
}

type RetryConfig struct {
	Base                 time.Duration `yaml:"base"`
	Cap                  time.Duration `yaml:"cap"`
	MaxAttemptsTransient int           `yaml:"max_attempts_transient"`
	RetryBudgetRatio     float64       `yaml:"retry_budget_ratio"`
}

type DeadLetterConfig struct {
	RetentionDays int `yaml:"retention_days"`
}

type ReconcileConfig struct {
	Periodic time.Duration `yaml:"periodic"`
}

type NotesConfig struct {
	OnFileDelete string `yaml:"on_file_delete"`
}

type ResourcesConfig struct {
	MinFreeBytes int64 `yaml:"min_free_bytes"`
}

type LogConfig struct {
	Level       string `yaml:"level"`
	RedactPaths bool   `yaml:"redact_paths"`
	RetainDays  int    `yaml:"retain_days"`
}

// Default returns a new configuration populated with the specification's
// production defaults. No placeholder watch root is installed: a node may be
// started without roots and configured later through the admin service.
func Default() Config {
	return Config{
		DataDir:       "/var/lib/indexnode",
		GRPCListen:    "0.0.0.0:7701",
		MetricsListen: "127.0.0.1:7702",
		Watch: WatchConfig{
			BufferSize:   4096,
			SettleWindow: time.Second,
		},
		Compute: ComputeConfig{
			Endpoint:        "dns:///compute:7801",
			RequestTimeout:  30 * time.Second,
			BatchSize:       32,
			BatchLinger:     100 * time.Millisecond,
			InflightBatches: 8,
			Breaker: BreakerConfig{
				Failures: 5,
				OpenFor:  30 * time.Second,
			},
		},
		Pipeline: PipelineConfig{
			IOConcurrency:      0,
			IOBytesInflight:    256 << 20,
			CPUPercentCap:      50,
			MaxFileSize:        512 << 20,
			MaxExtractBytes:    2 << 20,
			ImageSize:          384,
			ImageJPEGQuality:   90,
			ImageMaxPixels:     25_000_000,
			ImageBytesInflight: 256 << 20,
			VideoFrames:        5,
			FFmpegPath:         "ffmpeg",
		},
		Index: IndexConfig{
			CommitMaxOps:   1000,
			CommitInterval: 3 * time.Second,
			Vector: VectorConfig{
				M:                16,
				EFConstruction:   200,
				EFSearch:         64,
				SnapshotInterval: 10 * time.Minute,
				SnapshotChanges:  5000,
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
		Log: LogConfig{
			Level:      "info",
			RetainDays: 7,
		},
	}
}

// Load overlays a strict YAML file and INDEXNODE_ environment variables on
// Default, validates the result, and resolves an empty node_id to the stable ID
// stored in <data_dir>/node_id. A non-empty path takes precedence over
// INDEXNODE_CONFIG; an empty path selects its trimmed value, or defaults plus
// field environment variables when INDEXNODE_CONFIG is also empty.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		path = PathFromEnvironment()
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config %q: %w", path, err)
		}
		if err := decodeOneYAML(data, &cfg); err != nil {
			return nil, fmt.Errorf("decode config %q: %w", path, err)
		}
	}

	if err := applyEnvironment(&cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.NodeID == "" {
		nodeID, err := loadOrCreateNodeID(cfg.DataDir)
		if err != nil {
			return nil, fmt.Errorf("resolve node_id: %w", err)
		}
		cfg.NodeID = nodeID
	}
	if problem := nodeIDProblem(cfg.NodeID); problem != "" {
		return nil, validationError([]string{"node_id " + problem})
	}

	return &cfg, nil
}

// Validate performs side-effect-free validation. An empty NodeID is valid here
// because Load resolves it after all other fields have passed validation.
func (c Config) Validate() error {
	var problems []string
	add := func(condition bool, problem string) {
		if condition {
			problems = append(problems, problem)
		}
	}

	if c.NodeID != "" {
		if problem := nodeIDProblem(c.NodeID); problem != "" {
			problems = append(problems, "node_id "+problem)
		}
	}
	add(strings.TrimSpace(c.DataDir) == "", "data_dir must not be empty")
	add(strings.ContainsRune(c.DataDir, 0), "data_dir must not contain NUL")
	validateListenAddress(&problems, "grpc_listen", c.GRPCListen)
	validateListenAddress(&problems, "metrics_listen", c.MetricsListen)
	add(c.GRPCListen != "" && c.GRPCListen == c.MetricsListen, "metrics_listen must differ from grpc_listen")

	seenRoots := make(map[string]int, len(c.Watch.Roots))
	for i, root := range c.Watch.Roots {
		field := fmt.Sprintf("watch.roots[%d].path", i)
		if strings.TrimSpace(root.Path) == "" {
			problems = append(problems, field+" must not be empty")
			continue
		}
		if strings.ContainsRune(root.Path, 0) {
			problems = append(problems, field+" must not contain NUL")
			continue
		}
		key := filepath.Clean(root.Path)
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if previous, ok := seenRoots[key]; ok {
			problems = append(problems, fmt.Sprintf("%s duplicates watch.roots[%d].path", field, previous))
		} else {
			for existing, previous := range seenRoots {
				if configPathsOverlap(existing, key) {
					problems = append(problems, fmt.Sprintf("%s overlaps watch.roots[%d].path", field, previous))
					break
				}
			}
			seenRoots[key] = i
		}
	}
	add(c.Watch.BufferSize <= 0, "watch.buffer_size must be greater than 0")
	add(c.Watch.SettleWindow <= 0, "watch.settle_window must be greater than 0")

	add(strings.TrimSpace(c.Compute.Endpoint) == "", "compute.endpoint must not be empty")
	add(strings.ContainsRune(c.Compute.Endpoint, 0), "compute.endpoint must not contain NUL")
	add(c.Compute.RequestTimeout <= 0, "compute.request_timeout must be greater than 0")
	add(c.Compute.BatchSize <= 0, "compute.batch_size must be greater than 0")
	add(c.Compute.BatchLinger <= 0, "compute.batch_linger must be greater than 0")
	add(c.Compute.InflightBatches <= 0, "compute.inflight_batches must be greater than 0")
	add(c.Compute.Breaker.Failures <= 0, "compute.breaker.failures must be greater than 0")
	add(c.Compute.Breaker.OpenFor <= 0, "compute.breaker.open_for must be greater than 0")

	add(c.Pipeline.IOConcurrency < 0, "pipeline.io_concurrency must be 0 (automatic) or greater")
	add(c.Pipeline.IOBytesInflight <= 0, "pipeline.io_bytes_inflight must be greater than 0")
	add(c.Pipeline.CPUPercentCap < 1 || c.Pipeline.CPUPercentCap > 100, "pipeline.cpu_percent_cap must be between 1 and 100")
	add(c.Pipeline.MaxFileSize <= 0, "pipeline.max_file_size must be greater than 0")
	add(c.Pipeline.MaxExtractBytes <= 0, "pipeline.max_extract_bytes must be greater than 0")
	add(c.Pipeline.ImageSize <= 0, "pipeline.image_size must be greater than 0")
	add(c.Pipeline.ImageJPEGQuality < 1 || c.Pipeline.ImageJPEGQuality > 100, "pipeline.image_jpeg_quality must be between 1 and 100")
	add(c.Pipeline.ImageMaxPixels <= 0, "pipeline.image_max_pixels must be greater than 0")
	add(c.Pipeline.ImageMaxPixels > math.MaxInt64/4, "pipeline.image_max_pixels is too large")
	add(c.Pipeline.ImageBytesInflight <= 0, "pipeline.image_bytes_inflight must be greater than 0")
	if c.Pipeline.ImageMaxPixels > 0 && c.Pipeline.ImageMaxPixels <= math.MaxInt64/4 {
		add(c.Pipeline.ImageBytesInflight < c.Pipeline.ImageMaxPixels*4,
			"pipeline.image_bytes_inflight must be at least pipeline.image_max_pixels * 4")
	}
	add(c.Pipeline.VideoFrames <= 0 || c.Pipeline.VideoFrames >= 1<<16, "pipeline.video_frames must be between 1 and 65535")
	add(strings.ContainsRune(c.Pipeline.FFmpegPath, 0), "pipeline.ffmpeg_path must not contain NUL")

	add(c.Index.CommitMaxOps <= 0, "index.commit_max_ops must be greater than 0")
	add(c.Index.CommitInterval <= 0, "index.commit_interval must be greater than 0")
	add(c.Index.Vector.M < 2, "index.vector.m must be at least 2")
	add(c.Index.Vector.EFConstruction < c.Index.Vector.M, "index.vector.ef_construction must be at least index.vector.m")
	add(c.Index.Vector.EFSearch <= 0, "index.vector.ef_search must be greater than 0")
	add(c.Index.Vector.SnapshotInterval <= 0, "index.vector.snapshot_interval must be greater than 0")
	add(c.Index.Vector.SnapshotChanges <= 0, "index.vector.snapshot_changes must be greater than 0")

	add(c.Retry.Base <= 0, "retry.base must be greater than 0")
	add(c.Retry.Cap < c.Retry.Base, "retry.cap must be greater than or equal to retry.base")
	add(c.Retry.MaxAttemptsTransient < 0, "retry.max_attempts_transient must be 0 or greater")
	add(math.IsNaN(c.Retry.RetryBudgetRatio) || math.IsInf(c.Retry.RetryBudgetRatio, 0) || c.Retry.RetryBudgetRatio <= 0 || c.Retry.RetryBudgetRatio >= 1, "retry.retry_budget_ratio must be greater than 0 and less than 1")

	add(c.DeadLetter.RetentionDays <= 0, "dead_letter.retention_days must be greater than 0")
	add(int64(c.DeadLetter.RetentionDays) > int64(math.MaxInt64)/int64(24*time.Hour), "dead_letter.retention_days is too large")
	add(c.Reconcile.Periodic <= 0, "reconcile.periodic must be greater than 0")
	add(c.Notes.OnFileDelete != "orphan" && c.Notes.OnFileDelete != "cascade", "notes.on_file_delete must be one of orphan or cascade")
	add(c.Resources.MinFreeBytes < 0, "resources.min_free_bytes must be 0 or greater")
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		problems = append(problems, "log.level must be one of debug, info, warn, or error")
	}
	add(c.Log.RetainDays <= 0, "log.retain_days must be greater than 0")

	if len(problems) != 0 {
		return validationError(problems)
	}
	return nil
}

func configPathsOverlap(left, right string) bool {
	return configPathWithin(left, right) || configPathWithin(right, left)
}

func configPathWithin(root, candidate string) bool {
	if root == candidate {
		return true
	}
	if !strings.HasSuffix(root, string(filepath.Separator)) {
		root += string(filepath.Separator)
	}
	return strings.HasPrefix(candidate, root)
}

func validationError(problems []string) error {
	return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
}

func validateListenAddress(problems *[]string, field, address string) {
	if strings.TrimSpace(address) == "" {
		*problems = append(*problems, field+" must not be empty")
		return
	}
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		*problems = append(*problems, field+" must be a host:port address")
		return
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		*problems = append(*problems, field+" port must be between 1 and 65535")
	}
}

func nodeIDProblem(id string) string {
	if id == "" {
		return "must not be empty"
	}
	if len(id) > 255 {
		return "must not exceed 255 bytes"
	}
	if !utf8.ValidString(id) {
		return "must be valid UTF-8"
	}
	if strings.TrimSpace(id) != id {
		return "must not have leading or trailing whitespace"
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return "must not contain control characters"
		}
	}
	return ""
}

func decodeOneYAML(data []byte, destination any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("multiple YAML documents are not allowed")
	}
	return nil
}

func applyEnvironment(cfg *Config) error {
	known := map[string]string{
		ConfigEnvironmentVariable: "<configuration path>",
	}
	if err := applyEnvironmentValue(reflect.ValueOf(cfg).Elem(), "", known); err != nil {
		return err
	}

	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, EnvironmentPrefix) {
			continue
		}
		if _, ok := known[name]; !ok {
			return fmt.Errorf("environment variable %s: unknown configuration field", name)
		}
	}
	return nil
}

func applyEnvironmentValue(value reflect.Value, parentPath string, known map[string]string) error {
	typeOfValue := value.Type()
	for i := 0; i < value.NumField(); i++ {
		structField := typeOfValue.Field(i)
		yamlName := strings.Split(structField.Tag.Get("yaml"), ",")[0]
		if yamlName == "" || yamlName == "-" || structField.PkgPath != "" {
			continue
		}

		fieldPath := yamlName
		if parentPath != "" {
			fieldPath = parentPath + "." + yamlName
		}
		environmentName := EnvironmentPrefix + strings.ToUpper(strings.ReplaceAll(fieldPath, ".", "_"))
		if previous, exists := known[environmentName]; exists && previous != fieldPath {
			return fmt.Errorf("configuration schema maps both %s and %s to %s", previous, fieldPath, environmentName)
		}
		known[environmentName] = fieldPath

		field := value.Field(i)
		if raw, exists := os.LookupEnv(environmentName); exists {
			if err := decodeEnvironmentValue(raw, field); err != nil {
				return fmt.Errorf("environment variable %s (%s): %w", environmentName, fieldPath, err)
			}
		}

		if field.Kind() == reflect.Struct {
			if err := applyEnvironmentValue(field, fieldPath, known); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeEnvironmentValue(raw string, destination reflect.Value) error {
	if destination.Kind() == reflect.String {
		destination.SetString(raw)
		return nil
	}
	return decodeOneYAML([]byte(raw), destination.Addr().Interface())
}

func loadOrCreateNodeID(dataDir string) (string, error) {
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return "", fmt.Errorf("create data_dir %q: %w", dataDir, err)
	}
	path := filepath.Join(dataDir, NodeIDFilename)

	id, err := readNodeID(path)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		// Another process may have created the file but not completed its one-line
		// write yet. A short bounded retry also makes concurrent process startup
		// deterministic; a genuinely corrupt file still fails after the bound.
		return readNodeIDAfterConcurrentCreate(path)
	}

	id, err = newUUID()
	if err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return readNodeIDAfterConcurrentCreate(path)
	}
	if err != nil {
		return "", fmt.Errorf("create node ID file %q: %w", path, err)
	}

	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(path)
		}
	}()
	if _, err := io.WriteString(file, id+"\n"); err != nil {
		return "", fmt.Errorf("write node ID file %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("sync node ID file %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close node ID file %q: %w", path, err)
	}
	complete = true
	return id, nil
}

func readNodeIDAfterConcurrentCreate(path string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		id, err := readNodeID(path)
		if err == nil {
			return id, nil
		}
		lastErr = err
		time.Sleep(5 * time.Millisecond)
	}
	return "", fmt.Errorf("read concurrently-created node ID file %q: %w", path, lastErr)
}

func readNodeID(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read node ID file %q: %w", path, err)
	}
	if len(data) > 1024 {
		return "", fmt.Errorf("node ID file %q is unexpectedly large", path)
	}
	id := strings.TrimSuffix(string(data), "\n")
	id = strings.TrimSuffix(id, "\r")
	if problem := nodeIDProblem(id); problem != "" {
		return "", fmt.Errorf("node ID file %q: node_id %s", path, problem)
	}
	return id, nil
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	hexText := hex.EncodeToString(raw[:])
	return hexText[0:8] + "-" + hexText[8:12] + "-" + hexText[12:16] + "-" + hexText[16:20] + "-" + hexText[20:32], nil
}
