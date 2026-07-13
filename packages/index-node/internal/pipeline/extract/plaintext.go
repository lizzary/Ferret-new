package extract

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/lizzary/index-node/internal/store"
)

const PlaintextExtractorVersion = "plaintext-v1"

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

// PlaintextExtractor handles plain text, Markdown, and source-code formats.
// The registry also uses it as a content-sniffed fallback for unknown textual
// extensions.
type PlaintextExtractor struct {
	maxBytes int64
}

func NewPlaintextExtractor(maxBytes ...int64) *PlaintextExtractor {
	limit := DefaultMaxExtractBytes
	if len(maxBytes) != 0 && maxBytes[0] > 0 {
		limit = maxBytes[0]
	}
	return &PlaintextExtractor{maxBytes: limit}
}

func (p *PlaintextExtractor) Match(path string, sniff []byte) bool {
	if IsBinary(sniff) {
		return false
	}
	base := strings.ToLower(filepath.Base(path))
	switch base {
	case "dockerfile", "makefile", "rakefile", "gemfile", "procfile":
		return true
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".txt", ".text", ".log", ".md", ".markdown", ".mdown", ".mkd", ".rst",
		".csv", ".tsv", ".go", ".py", ".pyw", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx",
		".java", ".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".rs", ".rb", ".php",
		".swift", ".kt", ".kts", ".scala", ".sh", ".bash", ".zsh", ".fish", ".ps1", ".bat", ".cmd",
		".sql", ".html", ".htm", ".css", ".scss", ".sass", ".less", ".xml", ".json", ".jsonl",
		".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".properties", ".proto", ".gradle":
		return true
	default:
		return false
	}
}

func (p *PlaintextExtractor) Extract(ctx context.Context, reader io.Reader, _ FileMeta) (Doc, error) {
	if err := ctx.Err(); err != nil {
		return Doc{}, fmt.Errorf("read plaintext before start: %w", err)
	}
	maxBytes := p.maxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxExtractBytes
	}
	// GBK expands by at most roughly 3/2 when represented as UTF-8. Reading at
	// most 2x the output cap plus one complete GB18030 sequence keeps memory
	// bounded while providing enough decoded bytes to make truncation exact.
	inputLimit := maxBytes*2 + 4
	raw, err := io.ReadAll(io.LimitReader(reader, inputLimit+1))
	if err != nil {
		return Doc{}, fmt.Errorf("read plaintext: %w", err)
	}
	inputTruncated := int64(len(raw)) > inputLimit
	if inputTruncated {
		raw = raw[:inputLimit]
	}
	if err := ctx.Err(); err != nil {
		return Doc{}, fmt.Errorf("read plaintext context: %w", err)
	}

	decoded, err := decodeText(raw)
	if err != nil && inputTruncated {
		// The bounded read may end in the middle of one UTF-8/GB18030 code
		// sequence. Drop only that incomplete suffix; malformed data earlier in
		// the document remains an error.
		for drop := 1; drop <= 3 && drop < len(raw); drop++ {
			if prefix, prefixErr := decodeText(raw[:len(raw)-drop]); prefixErr == nil {
				decoded, err = prefix, nil
				break
			}
		}
	}
	if err != nil {
		return Doc{}, err
	}
	content, truncated := truncateUTF8(string(decoded), maxBytes, inputTruncated)
	return Doc{
		Kind:             store.FileKindText,
		Content:          content,
		Truncated:        truncated,
		ExtractorVersion: PlaintextExtractorVersion,
	}, nil
}

func decodeText(raw []byte) ([]byte, error) {
	if bytes.HasPrefix(raw, utf8BOM) {
		raw = raw[len(utf8BOM):]
		if !utf8.Valid(raw) {
			return nil, fmt.Errorf("decode UTF-8 BOM text: invalid UTF-8")
		}
		return raw, nil
	}
	if utf8.Valid(raw) {
		return raw, nil
	}
	decoded, err := decodeGB18030(raw)
	if err != nil {
		return nil, fmt.Errorf("decode GBK/GB18030 text: %w", err)
	}
	return decoded, nil
}

// IsBinary is intentionally conservative for legacy East-Asian encodings:
// high bytes are not counted as controls. NUL or a high density of ASCII
// control bytes identifies a binary fallback.
func IsBinary(sniff []byte) bool {
	if len(sniff) == 0 {
		return false
	}
	if bytes.HasPrefix(sniff, utf8BOM) {
		sniff = sniff[len(utf8BOM):]
	}
	controls := 0
	for _, b := range sniff {
		if b == 0 {
			return true
		}
		if b < 0x20 {
			switch b {
			case '\t', '\n', '\r', '\f', '\b':
			default:
				controls++
			}
		}
	}
	return controls*10 > len(sniff)
}
