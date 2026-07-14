package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/maintenance"
	"github.com/lizzary/index-node/internal/store"
)

func TestExecuteMaintenanceEnqueueAndListEmptyDeadLetters(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "document with spaces.txt")
	cfg := &config.Config{DataDir: dataDir}

	lines, err := executeMaintenance(context.Background(), cfg, "/ENQUEUE", []string{path})
	if err != nil {
		t.Fatalf("executeMaintenance(enqueue) error = %v", err)
	}
	want := []string{
		"Enqueued 1 path(s).",
		fmt.Sprintf("task=1 generation=1 inserted %s", filepath.Clean(path)),
	}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("executeMaintenance(enqueue) = %#v, want %#v", lines, want)
	}

	lines, err = executeMaintenance(context.Background(), cfg, "deadletters", []string{"list"})
	if err != nil {
		t.Fatalf("executeMaintenance(deadletters list) error = %v", err)
	}
	if want := []string{"Found 0 dead letter(s)."}; !reflect.DeepEqual(lines, want) {
		t.Fatalf("executeMaintenance(deadletters list) = %#v, want %#v", lines, want)
	}
}

func TestExecuteMaintenanceHelpDoesNotRequireConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		command   string
		arguments []string
		want      []string
	}{
		{"enqueue", []string{"-help"}, []string{"/enqueue <path>... - enqueue paths while stopped"}},
		{"search", []string{"--help"}, []string{"/search [-limit N] <query> - keyword search"}},
		{"deadletters", []string{"-h"}, maintenanceHelp("deadletters")},
		{"deadletters", []string{"list", "--help"}, []string{"/deadletters list [-class C] [-limit N]"}},
		{"deadletters", []string{"redrive", "-help"}, maintenanceHelp("deadletters-redrive")},
	}
	for _, test := range tests {
		test := test
		t.Run(test.command+strings.Join(test.arguments, "_"), func(t *testing.T) {
			t.Parallel()
			got, err := executeMaintenance(context.Background(), nil, test.command, test.arguments)
			if err != nil {
				t.Fatalf("executeMaintenance() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("executeMaintenance() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestMaintenanceArgumentErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		command   string
		arguments []string
		wantError string
	}{
		{"unsupported", "compact", nil, `unsupported maintenance command "compact"`},
		{"enqueue paths", "enqueue", nil, "at least one path is required"},
		{"enqueue option", "enqueue", []string{"-recursive"}, `unknown option "-recursive"`},
		{"search query", "search", nil, "query is required"},
		{"search missing limit", "search", []string{"-limit"}, "-limit requires a value"},
		{"search invalid limit", "search", []string{"-limit=nope", "query"}, `invalid limit "nope"`},
		{"search lower limit", "search", []string{"-limit", "0", "query"}, "limit must be between 1 and 1000"},
		{"search upper limit", "search", []string{"--limit=1001", "query"}, "limit must be between 1 and 1000"},
		{"search duplicate limit", "search", []string{"-limit", "1", "--limit=2", "query"}, "provided only once"},
		{"search unknown option", "search", []string{"-kind", "text", "query"}, `unknown option "-kind"`},
		{"deadletters subcommand", "deadletters", nil, "expected list or redrive"},
		{"deadletters unknown", "deadletters", []string{"purge"}, `unknown subcommand "purge"`},
		{"list positional", "deadletters", []string{"list", "extra"}, `unexpected argument "extra"`},
		{"list class", "deadletters", []string{"list", "-class", "mystery"}, `invalid class "mystery"`},
		{"list missing class", "deadletters", []string{"list", "-class"}, "-class requires a value"},
		{"list limit", "deadletters", []string{"list", "-limit", "-1"}, "limit must be between 1 and 1000"},
		{"list unknown option", "deadletters", []string{"list", "-all"}, `unknown option "-all"`},
		{"redrive selector", "deadletters", []string{"redrive"}, "provide exactly one"},
		{"redrive selectors", "deadletters", []string{"redrive", "-file-ids", "1", "-class", "poison"}, "provide exactly one"},
		{"redrive missing ids", "deadletters", []string{"redrive", "-file-ids"}, "-file-ids requires a value"},
		{"redrive invalid zero", "deadletters", []string{"redrive", "-file-ids", "0"}, `invalid file ID "0"`},
		{"redrive invalid empty", "deadletters", []string{"redrive", "-file-ids", "1,,2"}, `invalid file ID ""`},
		{"redrive invalid class", "deadletters", []string{"redrive", "-class", "retryable"}, `invalid class "retryable"`},
		{"redrive positional", "deadletters", []string{"redrive", "1"}, `unexpected argument "1"`},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := executeMaintenance(context.Background(), nil, test.command, test.arguments)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("executeMaintenance() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestParseSearchArguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		arguments []string
		want      searchMaintenanceRequest
	}{
		{"defaults", []string{"two", "words"}, searchMaintenanceRequest{query: "two words", limit: 20}},
		{"separate option", []string{"-limit", "7", "two", "words"}, searchMaintenanceRequest{query: "two words", limit: 7}},
		{"inline long option", []string{"--limit=8", "needle"}, searchMaintenanceRequest{query: "needle", limit: 8}},
		{"option after word", []string{"two", "--limit", "9", "words"}, searchMaintenanceRequest{query: "two words", limit: 9}},
		{"option terminator", []string{"--", "-literal", "query"}, searchMaintenanceRequest{query: "-literal query", limit: 20}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseSearchArguments(test.arguments)
			if err != nil {
				t.Fatalf("parseSearchArguments() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("parseSearchArguments() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestParseDeadLetterArguments(t *testing.T) {
	t.Parallel()

	list, err := parseDeadLetterListArguments([]string{"--class=permanent", "-limit", "12"})
	if err != nil {
		t.Fatalf("parseDeadLetterListArguments() error = %v", err)
	}
	if want := (deadLetterListMaintenanceRequest{errorClass: "permanent", limit: 12}); !reflect.DeepEqual(list, want) {
		t.Fatalf("parseDeadLetterListArguments() = %#v, want %#v", list, want)
	}

	redrive, err := parseDeadLetterRedriveArguments([]string{"--file-ids=3, 1,3,2"})
	if err != nil {
		t.Fatalf("parseDeadLetterRedriveArguments() error = %v", err)
	}
	want := deadLetterRedriveMaintenanceRequest{fileIDs: []int64{3, 1, 2}}
	if !reflect.DeepEqual(redrive, want) {
		t.Fatalf("parseDeadLetterRedriveArguments() = %#v, want %#v", redrive, want)
	}
}

func TestMaintenanceResultFormatting(t *testing.T) {
	t.Parallel()

	enqueued := formatEnqueueResults([]maintenance.EnqueueResult{
		{TaskID: 4, Path: "/inserted", Generation: 2, Inserted: true},
		{TaskID: 5, Path: "/coalesced", Generation: 3},
	})
	if want := []string{
		"Enqueued 2 path(s).",
		"task=4 generation=2 inserted /inserted",
		"task=5 generation=3 coalesced /coalesced",
	}; !reflect.DeepEqual(enqueued, want) {
		t.Fatalf("formatEnqueueResults() = %#v, want %#v", enqueued, want)
	}

	hits := formatSearchResults([]index.KeywordHit{
		{FileID: 7, Score: 1.25, Status: "indexed", Kind: "text", Path: "/document"},
	})
	if want := []string{
		"Keyword search returned 1 hit(s).",
		"file=7 score=1.2500 status=indexed kind=text /document",
	}; !reflect.DeepEqual(hits, want) {
		t.Fatalf("formatSearchResults() = %#v, want %#v", hits, want)
	}

	dead := store.DeadLetter{FileID: 9, Generation: 4, ErrorClass: "poison", Stage: "extract", Path: "/broken"}
	listed := formatDeadLetterList([]store.DeadLetter{dead})
	if want := []string{
		"Found 1 dead letter(s).",
		"file=9 generation=4 class=poison stage=extract /broken",
	}; !reflect.DeepEqual(listed, want) {
		t.Fatalf("formatDeadLetterList() = %#v, want %#v", listed, want)
	}

	redriven := formatDeadLetterRedrive([]store.DeadLetterRedriveResult{{
		DeadLetter: dead,
		EnqueueResult: store.EnqueueResult{Task: store.Task{
			ID: 11, Generation: 5,
		}},
	}})
	if want := []string{
		"Redrove 1 dead letter(s).",
		"file=9 task=11 generation=5 /broken",
	}; !reflect.DeepEqual(redriven, want) {
		t.Fatalf("formatDeadLetterRedrive() = %#v, want %#v", redriven, want)
	}
}
