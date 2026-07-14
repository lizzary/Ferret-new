package cli

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogHubSerializesConcurrentMirrorWritesInSnapshotOrder(t *testing.T) {
	const workers = 128
	var mirror bytes.Buffer
	hub := NewLogHub(workers, &mirror)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(workers)
	for index := 0; index < workers; index++ {
		index := index
		go func() {
			defer group.Done()
			<-start
			hub.Info("parallel", "entry-%03d", index)
		}()
	}
	close(start)
	group.Wait()

	entries := hub.Snapshot()
	lines := strings.Split(strings.TrimSuffix(mirror.String(), "\n"), "\n")
	if len(entries) != workers || len(lines) != workers {
		t.Fatalf("snapshot/mirror counts = %d/%d, want %d", len(entries), len(lines), workers)
	}
	for index, entry := range entries {
		if want := FormatLogEntry(entry); lines[index] != want {
			t.Fatalf("mirror line %d = %q, want snapshot line %q", index, lines[index], want)
		}
	}
}

func TestLogHubParsesChunkedJSONAndFiltersLevels(t *testing.T) {
	hub := NewLogHub(10)
	jsonPrefix := `{"time":"2026-07-14T01:02:03.000000004Z","level":"warning","component":"watch","msg":"indexed","z":2,`
	jsonSuffix := `"a":"x"}`
	if written, err := hub.Write([]byte(jsonPrefix)); err != nil || written != len(jsonPrefix) {
		t.Fatalf("first Write = (%d, %v), want (%d, nil)", written, err, len(jsonPrefix))
	}
	if got := len(hub.Snapshot()); got != 0 {
		t.Fatalf("incomplete JSON produced %d entries, want 0", got)
	}
	if written, err := hub.Write([]byte(jsonSuffix)); err != nil || written != len(jsonSuffix) {
		t.Fatalf("second Write = (%d, %v), want (%d, nil)", written, err, len(jsonSuffix))
	}
	if _, err := hub.Write([]byte("plain startup line\n")); err != nil {
		t.Fatalf("plain Write: %v", err)
	}
	hub.Error("store", "open failed: %s", "locked")

	entries := hub.Snapshot()
	if len(entries) != 3 {
		t.Fatalf("entries = %#v, want three", entries)
	}
	parsed := entries[0]
	if parsed.Sequence != 1 || parsed.Level != LogLevelWarn || parsed.Scope != "watch" {
		t.Fatalf("parsed JSON metadata = %#v", parsed)
	}
	wantTime := time.Date(2026, 7, 14, 1, 2, 3, 4, time.UTC)
	if !parsed.Time.Equal(wantTime) {
		t.Fatalf("parsed JSON time = %s, want %s", parsed.Time, wantTime)
	}
	if parsed.Message != `indexed  a="x" z=2` {
		t.Fatalf("parsed JSON message = %q", parsed.Message)
	}
	if entries[1].Level != LogLevelInfo || entries[1].Scope != "node" || entries[1].Message != "plain startup line" {
		t.Fatalf("plain entry = %#v", entries[1])
	}
	if entries[2].Sequence != 3 || entries[2].Level != LogLevelError || entries[2].Scope != "store" {
		t.Fatalf("error entry = %#v", entries[2])
	}

	m := &model{cfg: Config{Log: hub}}
	tests := []struct {
		filter int
		want   []string
	}{
		{filter: 0, want: []string{parsed.Message, "plain startup line", "open failed: locked"}},
		{filter: 1, want: []string{"plain startup line"}},
		{filter: 2, want: []string{parsed.Message}},
		{filter: 3, want: []string{"open failed: locked"}},
	}
	for _, test := range tests {
		m.levelFilter = test.filter
		filtered := m.filteredEntries()
		got := make([]string, len(filtered))
		for index := range filtered {
			got[index] = filtered[index].Message
		}
		if strings.Join(got, "\x00") != strings.Join(test.want, "\x00") {
			t.Fatalf("filter %d messages = %#v, want %#v", test.filter, got, test.want)
		}
	}
}

func TestLogHubRetainsOnlyNewestEntries(t *testing.T) {
	hub := NewLogHub(2)
	hub.Info("test", "one")
	hub.Warn("test", "two")
	hub.Error("test", "three")

	entries := hub.Snapshot()
	if len(entries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(entries))
	}
	if entries[0].Sequence != 2 || entries[0].Message != "two" || entries[1].Sequence != 3 || entries[1].Message != "three" {
		t.Fatalf("bounded entries = %#v", entries)
	}

	entries[0].Message = "mutated"
	if got := hub.Snapshot()[0].Message; got != "two" {
		t.Fatalf("Snapshot exposed mutable storage: %q", got)
	}
}
