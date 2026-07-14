// Package obs provides the node's structured logging, metrics, and audit
// primitives. It intentionally owns no package-level mutable state so callers
// can create isolated instances for tests and for single-process deployments.
package obs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogRetainDays = 7
	defaultLogMaxSizeMB  = 100
)

// LoggerOptions configures a JSON slog handler.
type LoggerOptions struct {
	Level       slog.Leveler
	AddSource   bool
	RedactPaths bool
}

// LocalLogOptions configures the node-local rotating log. Local logs always
// retain full paths; RedactPaths belongs on boundary/telemetry loggers created
// with NewJSONLogger, preserving the privacy dual-track required by the spec.
type LocalLogOptions struct {
	Path       string
	Level      slog.Leveler
	AddSource  bool
	AlsoStderr bool
	// LogWriter receives the same local JSON records as the rotating log.
	LogWriter  io.Writer
	RetainDays int
	MaxSizeMB  int
	MaxBackups int
	Compress   bool
}

// TaskFields are attached to every state-transition log for a task.
type TaskFields struct {
	TaskID     int64
	FileID     int64
	Generation int64
}

type contextFields struct {
	task    TaskFields
	hasTask bool
	traceID string
}

type contextFieldsKey struct{}

// ParseLevel parses the levels accepted by slog (debug, info, warn, error and
// their numeric offsets). An empty value resolves to info.
func ParseLevel(value string) (slog.Level, error) {
	if strings.TrimSpace(value) == "" {
		return slog.LevelInfo, nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.TrimSpace(value))); err != nil {
		return 0, fmt.Errorf("parse log level %q: %w", value, err)
	}
	return level, nil
}

// NewJSONLogger constructs a structured JSON logger. Set RedactPaths for logs
// that leave the process boundary; node-local logs must leave it false.
func NewJSONLogger(writer io.Writer, options LoggerOptions) *slog.Logger {
	if writer == nil {
		writer = io.Discard
	}

	level := options.Level
	if level == nil {
		level = slog.LevelInfo
	}

	handlerOptions := &slog.HandlerOptions{
		AddSource: options.AddSource,
		Level:     level,
	}
	if options.RedactPaths {
		handlerOptions.ReplaceAttr = redactHandlerGeneratedAttr
	}
	handler := slog.Handler(slog.NewJSONHandler(writer, handlerOptions))
	if options.RedactPaths {
		handler = redactingHandler{next: handler}
	}
	return slog.New(handler)
}

// OpenLocalLogger opens a lumberjack-backed JSON logger. The returned closer
// must be closed during lifecycle shutdown so the active log file is flushed.
func OpenLocalLogger(options LocalLogOptions) (*slog.Logger, io.Closer, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, nil, fmt.Errorf("open local logger: path is required")
	}
	if options.RetainDays < 0 {
		return nil, nil, fmt.Errorf("open local logger: retain days must be non-negative")
	}
	if options.MaxSizeMB < 0 {
		return nil, nil, fmt.Errorf("open local logger: max size must be non-negative")
	}
	if options.MaxBackups < 0 {
		return nil, nil, fmt.Errorf("open local logger: max backups must be non-negative")
	}

	path := filepath.Clean(options.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, nil, fmt.Errorf("create log directory: %w", err)
	}

	retainDays := options.RetainDays
	if retainDays == 0 {
		retainDays = defaultLogRetainDays
	}
	maxSizeMB := options.MaxSizeMB
	if maxSizeMB == 0 {
		maxSizeMB = defaultLogMaxSizeMB
	}

	roller := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    maxSizeMB,
		MaxBackups: options.MaxBackups,
		MaxAge:     retainDays,
		LocalTime:  true,
		Compress:   options.Compress,
	}

	var writer io.Writer = roller
	writers := []io.Writer{writer}
	if options.AlsoStderr {
		writers = append(writers, os.Stderr)
	}
	if options.LogWriter != nil {
		writers = append(writers, options.LogWriter)
	}
	if len(writers) > 1 {
		writer = io.MultiWriter(writers...)
	}

	logger := NewJSONLogger(writer, LoggerOptions{
		Level:     options.Level,
		AddSource: options.AddSource,
	})
	return logger, roller, nil
}

// WithTask stores the correlation fields required for task lifecycle logs.
func WithTask(ctx context.Context, fields TaskFields) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	current := fieldsFromContext(ctx)
	current.task = fields
	current.hasTask = true
	return context.WithValue(ctx, contextFieldsKey{}, current)
}

// WithTraceID stores a request trace ID alongside any existing task fields.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	current := fieldsFromContext(ctx)
	current.traceID = traceID
	return context.WithValue(ctx, contextFieldsKey{}, current)
}

// WithContext derives a logger containing task and trace correlation fields.
func WithContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := ContextAttrs(ctx)
	if len(attrs) == 0 {
		return logger
	}
	return logger.With(attrsToAny(attrs)...)
}

// ContextAttrs returns correlation attributes suitable for slog.LogAttrs.
func ContextAttrs(ctx context.Context) []slog.Attr {
	fields := fieldsFromContext(ctx)
	attrs := make([]slog.Attr, 0, 4)
	if fields.hasTask {
		attrs = append(attrs,
			slog.Int64("task_id", fields.task.TaskID),
			slog.Int64("file_id", fields.task.FileID),
			slog.Int64("generation", fields.task.Generation),
		)
	}
	if fields.traceID != "" {
		attrs = append(attrs, slog.String("trace_id", fields.traceID))
	}
	return attrs
}

// RedactPath hashes the complete path while retaining only its extension. It
// is intended for remote telemetry, never for security decisions.
func RedactPath(path string) string {
	if path == "" {
		return ""
	}

	extension := filepath.Ext(path)
	if extension == filepath.Base(path) || len(extension) > 16 {
		extension = ""
	}
	digest := sha256.Sum256([]byte(path))
	return "sha256:" + hex.EncodeToString(digest[:]) + extension
}

func fieldsFromContext(ctx context.Context) contextFields {
	if ctx == nil {
		return contextFields{}
	}
	fields, _ := ctx.Value(contextFieldsKey{}).(contextFields)
	return fields
}

func attrsToAny(attrs []slog.Attr) []any {
	values := make([]any, len(attrs))
	for i := range attrs {
		values[i] = attrs[i]
	}
	return values
}

type redactingHandler struct {
	next slog.Handler
}

func (handler redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.next.Enabled(ctx, level)
}

func (handler redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	redacted := slog.NewRecord(record.Time, record.Level, redactPathsInText(record.Message), record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		redacted.AddAttrs(redactBoundaryAttr(attr))
		return true
	})
	return handler.next.Handle(ctx, redacted)
}

func (handler redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i := range attrs {
		redacted[i] = redactBoundaryAttr(attrs[i])
	}
	return redactingHandler{next: handler.next.WithAttrs(redacted)}
}

func (handler redactingHandler) WithGroup(name string) slog.Handler {
	return redactingHandler{next: handler.next.WithGroup(name)}
}

func redactBoundaryAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	if isPathKey(attr.Key) {
		return redactExplicitPathAttr(attr)
	}

	switch attr.Value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(redactPathsInText(attr.Value.String()))
	case slog.KindGroup:
		group := attr.Value.Group()
		redacted := make([]slog.Attr, len(group))
		for i := range group {
			redacted[i] = redactBoundaryAttr(group[i])
		}
		attr.Value = slog.GroupValue(redacted...)
	case slog.KindAny:
		attr.Value = slog.AnyValue(redactBoundaryAny(attr.Value.Any()))
	}
	return attr
}

func redactHandlerGeneratedAttr(_ []string, attr slog.Attr) slog.Attr {
	if attr.Key != slog.SourceKey {
		return attr
	}
	source, ok := attr.Value.Any().(*slog.Source)
	if !ok || source == nil {
		return attr
	}
	redacted := *source
	redacted.File = RedactPath(source.File)
	return slog.Any(attr.Key, &redacted)
}

func redactExplicitPathAttr(attr slog.Attr) slog.Attr {
	switch attr.Value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(RedactPath(attr.Value.String()))
	case slog.KindAny:
		attr.Value = slog.AnyValue(redactExplicitPathAny(attr.Value.Any()))
	}
	return attr
}

func redactBoundaryAny(value any) any {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64:
		return value
	case string:
		return redactPathsInText(value)
	case error:
		// Arbitrary errors commonly embed paths (notably fs.PathError).
		return fmt.Sprintf("%T", value)
	case []string:
		redacted := make([]string, len(value))
		for i := range value {
			redacted[i] = redactPathsInText(value[i])
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for i := range value {
			redacted[i] = redactBoundaryAny(value[i])
		}
		return redacted
	default:
		// Arbitrary containers can hide path strings at any depth. Boundary
		// logs keep only their type; callers needing fields should use slog groups.
		return fmt.Sprintf("%T", value)
	}
}

func redactExplicitPathAny(value any) any {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64:
		return value
	case string:
		return RedactPath(value)
	case []string:
		redacted := make([]string, len(value))
		for i := range value {
			redacted[i] = RedactPath(value[i])
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for i := range value {
			redacted[i] = redactExplicitPathAny(value[i])
		}
		return redacted
	default:
		return fmt.Sprintf("%T", value)
	}
}

func redactPathsInText(text string) string {
	if !strings.ContainsAny(text, `/\\`) {
		return text
	}
	// Free-form messages do not expose reliable path boundaries: valid paths
	// may contain whitespace. Hash the complete message rather than risk leaking
	// a suffix. Path-bearing details should be structured attributes.
	digest := sha256.Sum256([]byte(text))
	return "redacted_message:sha256:" + hex.EncodeToString(digest[:])
}

func isPathKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	return normalized == "path" || normalized == "paths" ||
		normalized == "root" || normalized == "roots" ||
		strings.HasSuffix(normalized, "_path") ||
		strings.HasSuffix(normalized, "_paths")
}
