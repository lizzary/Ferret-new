package extract

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPlaintextUTF8BOM(t *testing.T) {
	raw := append([]byte{0xef, 0xbb, 0xbf}, []byte("你好, UTF-8")...)
	doc, err := NewPlaintextExtractor().Extract(context.Background(), bytes.NewReader(raw), FileMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.Content != "你好, UTF-8" {
		t.Fatalf("content = %q", doc.Content)
	}
}

func TestPlaintextDecodesGBKAndGB18030(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "GBK", raw: []byte{0xc4, 0xe3, 0xba, 0xc3}, want: "你好"},
		{name: "GB18030 four byte", raw: []byte{0x94, 0x39, 0xfc, 0x36}, want: "😀"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doc, err := NewPlaintextExtractor().Extract(context.Background(), bytes.NewReader(test.raw), FileMeta{})
			if err != nil {
				t.Fatal(err)
			}
			if doc.Content != test.want {
				t.Fatalf("content = %q, want %q", doc.Content, test.want)
			}
		})
	}
}

func TestPlaintextTruncationIsConfigurableAndUTF8Safe(t *testing.T) {
	doc, err := NewPlaintextExtractor(7).Extract(context.Background(), strings.NewReader("ab你好cd"), FileMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.Content != "ab你" || !doc.Truncated || !utf8.ValidString(doc.Content) {
		t.Fatalf("doc = %#v", doc)
	}
}

func TestBinarySniffAllowsLegacyHighBytesButRejectsControls(t *testing.T) {
	if IsBinary([]byte{0xc4, 0xe3, 0xba, 0xc3}) {
		t.Fatal("GBK bytes classified as binary")
	}
	if !IsBinary([]byte{1, 2, 3, 'a', 'b'}) {
		t.Fatal("dense controls not classified as binary")
	}
}
