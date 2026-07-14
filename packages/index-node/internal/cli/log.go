package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LogLevel is the severity shown by the interactive log viewer.
type LogLevel uint8

const (
	LogLevelInfo LogLevel = iota
	LogLevelWarn
	LogLevelError
)

func (level LogLevel) String() string {
	switch level {
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "INFO"
	}
}

// LogEntry is one immutable entry retained by LogHub.
type LogEntry struct {
	Sequence uint64
	Time     time.Time
	Level    LogLevel
	Scope    string
	Message  string
}

// LogHub is a bounded, concurrency-safe log sink and io.Writer. Write accepts
// newline-delimited JSON emitted by slog's JSON handler. Plain lines are also
// retained so startup failures from non-slog boundaries stay visible.
type LogHub struct {
	// mirrorMu serializes the full append-to-mirror sequence so optional
	// non-thread-safe writers see the same ordering as Snapshot.
	mirrorMu sync.Mutex
	mu       sync.RWMutex
	entries  []LogEntry
	capacity int
	next     uint64
	mirror   io.Writer

	writeMu sync.Mutex
	pending []byte
}

const (
	defaultLogCapacity = 2000
	maxPendingLogBytes = 256 << 10
)

// NewLogHub constructs a bounded hub. An optional first mirror receives the
// human-readable form after an entry has been safely retained.
func NewLogHub(capacity int, mirror ...io.Writer) *LogHub {
	if capacity < 1 {
		capacity = 1
	}
	var output io.Writer
	if len(mirror) != 0 {
		output = mirror[0]
	}
	return &LogHub{capacity: capacity, mirror: output}
}

// Snapshot returns a stable oldest-to-newest copy.
func (hub *LogHub) Snapshot() []LogEntry {
	if hub == nil {
		return nil
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	entries := make([]LogEntry, len(hub.entries))
	copy(entries, hub.entries)
	return entries
}

// Log records one UI-originated message.
func (hub *LogHub) Log(level LogLevel, scope, format string, args ...any) {
	if hub == nil {
		return
	}
	message := strings.TrimSpace(fmt.Sprintf(format, args...))
	if message == "" {
		return
	}
	hub.append(LogEntry{
		Time: time.Now(), Level: level,
		Scope: strings.TrimSpace(scope), Message: message,
	})
}

func (hub *LogHub) Info(scope, format string, args ...any) {
	hub.Log(LogLevelInfo, scope, format, args...)
}

func (hub *LogHub) Warn(scope, format string, args ...any) {
	hub.Log(LogLevelWarn, scope, format, args...)
}

func (hub *LogHub) Error(scope, format string, args ...any) {
	hub.Log(LogLevelError, scope, format, args...)
}

func (hub *LogHub) append(entry LogEntry) {
	hub.mirrorMu.Lock()
	defer hub.mirrorMu.Unlock()
	if entry.Time.IsZero() {
		entry.Time = time.Now()
	}
	if entry.Scope == "" {
		entry.Scope = "node"
	}
	hub.mu.Lock()
	hub.next++
	entry.Sequence = hub.next
	if len(hub.entries) == hub.capacity {
		copy(hub.entries, hub.entries[1:])
		hub.entries[len(hub.entries)-1] = entry
	} else {
		hub.entries = append(hub.entries, entry)
	}
	mirror := hub.mirror
	hub.mu.Unlock()

	if mirror != nil {
		_, _ = fmt.Fprintln(mirror, FormatLogEntry(entry))
	}
}

// FormatLogEntry renders one entry without terminal control sequences.
func FormatLogEntry(entry LogEntry) string {
	scope := strings.TrimSpace(safeInline(entry.Scope))
	if scope == "" {
		scope = "node"
	}
	return fmt.Sprintf("%s %-5s %-9s %s", entry.Time.Format("15:04:05"), entry.Level, scope, safeInline(entry.Message))
}

// Write implements io.Writer for slog JSON output. It deliberately returns the
// full input length after retaining malformed lines as plain text: logging must
// not be able to fail the node lifecycle.
func (hub *LogHub) Write(data []byte) (int, error) {
	if hub == nil {
		return len(data), nil
	}
	hub.writeMu.Lock()
	hub.pending = append(hub.pending, data...)
	for {
		newline := bytes.IndexByte(hub.pending, '\n')
		if newline < 0 {
			break
		}
		line := append([]byte(nil), hub.pending[:newline]...)
		hub.pending = hub.pending[newline+1:]
		hub.consumeLine(line)
	}
	// Tests, adapters, and a few slog wrappers may issue one complete JSON
	// object without a trailing newline. Consume it as soon as it is complete,
	// while retaining incomplete chunks for the next Write call.
	trimmed := bytes.TrimSpace(hub.pending)
	if len(trimmed) != 0 && json.Valid(trimmed) {
		line := append([]byte(nil), trimmed...)
		hub.pending = hub.pending[:0]
		hub.consumeLine(line)
	} else if len(hub.pending) > maxPendingLogBytes {
		line := append([]byte(nil), hub.pending[:maxPendingLogBytes]...)
		hub.pending = hub.pending[:0]
		hub.append(LogEntry{Time: time.Now(), Level: LogLevelWarn, Scope: "log", Message: strings.TrimSpace(string(line)) + "…"})
	}
	hub.writeMu.Unlock()
	return len(data), nil
}

func (hub *LogHub) consumeLine(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	entry, ok := parseSlogJSON(line)
	if !ok {
		entry = LogEntry{Time: time.Now(), Level: LogLevelInfo, Scope: "node", Message: string(line)}
	}
	if strings.TrimSpace(entry.Message) != "" {
		hub.append(entry)
	}
}

func parseSlogJSON(line []byte) (LogEntry, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		return LogEntry{}, false
	}
	message := jsonString(fields["msg"])
	if message == "" {
		message = jsonString(fields["message"])
	}
	if message == "" {
		return LogEntry{}, false
	}
	entry := LogEntry{
		Time:    parseLogTime(fields["time"]),
		Level:   parseLogLevel(jsonString(fields["level"])),
		Scope:   firstJSONText(fields, "scope", "component", "subsystem", "logger"),
		Message: message,
	}
	if entry.Scope == "" {
		entry.Scope = "node"
	}

	reserved := map[string]struct{}{
		"time": {}, "level": {}, "msg": {}, "message": {}, "scope": {},
		"component": {}, "subsystem": {}, "logger": {}, "source": {},
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		if _, skip := reserved[key]; !skip {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	attributes := make([]string, 0, len(keys))
	for _, key := range keys {
		attributes = append(attributes, key+"="+compactJSONValue(fields[key]))
	}
	if len(attributes) != 0 {
		entry.Message += "  " + strings.Join(attributes, " ")
	}
	return entry, true
}

func firstJSONText(fields map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if value := jsonString(fields[key]); value != "" {
			return value
		}
	}
	return ""
}

func jsonString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return strings.TrimSpace(value)
	}
	return ""
}

func parseLogTime(raw json.RawMessage) time.Time {
	text := jsonString(raw)
	if text != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
			return parsed
		}
	}
	var seconds float64
	if len(raw) != 0 && json.Unmarshal(raw, &seconds) == nil {
		whole, fraction := mathModf(seconds)
		return time.Unix(int64(whole), int64(fraction*float64(time.Second)))
	}
	return time.Now()
}

// mathModf avoids pulling floating-point formatting into the common parser.
func mathModf(value float64) (float64, float64) {
	whole := float64(int64(value))
	return whole, value - whole
}

func parseLogLevel(value string) LogLevel {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "WARN", "WARNING":
		return LogLevelWarn
	case "ERROR", "ERR", "FATAL", "PANIC":
		return LogLevelError
	default:
		// DEBUG is intentionally grouped with INFO because the Artifex viewer's
		// established four filters are ALL, INFO, WARN, and ERROR.
		return LogLevelInfo
	}
}

func compactJSONValue(raw json.RawMessage) string {
	if text := jsonString(raw); text != "" {
		return strconv.Quote(text)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err == nil {
		return compact.String()
	}
	return strconv.Quote(string(raw))
}

// Writer adapts line-oriented non-JSON output to the hub.
func (hub *LogHub) Writer(scope string, level LogLevel) io.Writer {
	return &plainLogWriter{hub: hub, scope: scope, level: level}
}

type plainLogWriter struct {
	mu      sync.Mutex
	hub     *LogHub
	scope   string
	level   LogLevel
	pending string
}

func (writer *plainLogWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	writer.pending += string(data)
	for {
		newline := strings.IndexByte(writer.pending, '\n')
		if newline < 0 {
			break
		}
		line := strings.TrimSpace(writer.pending[:newline])
		writer.pending = writer.pending[newline+1:]
		if line != "" {
			writer.hub.Log(writer.level, writer.scope, "%s", line)
		}
	}
	writer.mu.Unlock()
	return len(data), nil
}
