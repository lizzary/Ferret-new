package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lizzary/index-node/internal/config"
)

func TestRuntimeRouting(t *testing.T) {
	configPath, dataDir := installRuntimeConfig(t)
	tests := []struct {
		name        string
		arguments   []string
		interactive bool
		wantUI      bool
	}{
		{name: "TTY defaults to Bubble Tea", interactive: true, wantUI: true},
		{name: "no-ui selects plain lifecycle", arguments: []string{"-no-ui"}, interactive: true},
		{name: "non-TTY degrades to plain lifecycle"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			terminalCalls := 0
			plainCalls := 0
			checkConfig := func(cfg *config.Config) {
				t.Helper()
				if cfg == nil || cfg.NodeID != "test-node" || cfg.DataDir != dataDir {
					t.Fatalf("loaded config = %#v, want node_id test-node and data_dir %q", cfg, dataDir)
				}
			}
			runners := runtimeRunners{
				terminal: func(_ context.Context, source string, cfg *config.Config) error {
					terminalCalls++
					if source != configPath {
						t.Fatalf("terminal config source = %q, want %q", source, configPath)
					}
					checkConfig(cfg)
					return nil
				},
				plain: func(_ context.Context, cfg *config.Config) error {
					plainCalls++
					checkConfig(cfg)
					return nil
				},
			}

			if err := runWithContext(context.Background(), test.arguments, io.Discard, test.interactive, runners); err != nil {
				t.Fatalf("runWithContext() error = %v", err)
			}
			wantTerminal, wantPlain := 0, 1
			if test.wantUI {
				wantTerminal, wantPlain = 1, 0
			}
			if terminalCalls != wantTerminal || plainCalls != wantPlain {
				t.Fatalf("runner calls = terminal %d, plain %d; want %d, %d", terminalCalls, plainCalls, wantTerminal, wantPlain)
			}
		})
	}
}

func TestLegacyArgumentsAreRejectedBeforeConfigurationOrRuntime(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.yaml")
	t.Setenv(config.ConfigEnvironmentVariable, missingConfig)
	runnerCalls := 0
	runners := runtimeRunners{
		terminal: func(context.Context, string, *config.Config) error { runnerCalls++; return nil },
		plain:    func(context.Context, *config.Config) error { runnerCalls++; return nil },
	}
	tests := []struct {
		name      string
		arguments []string
		want      string
	}{
		{name: "run", arguments: []string{"run"}, want: "unexpected positional arguments"},
		{name: "enqueue", arguments: []string{"enqueue"}, want: "unexpected positional arguments"},
		{name: "search", arguments: []string{"search"}, want: "unexpected positional arguments"},
		{name: "deadletters", arguments: []string{"deadletters"}, want: "unexpected positional arguments"},
		{name: "config flag", arguments: []string{"-config", "legacy.yaml"}, want: "flag provided but not defined: -config"},
		{name: "theme flag", arguments: []string{"-cli-theme", "dark"}, want: "flag provided but not defined: -cli-theme"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), test.arguments, io.Discard, true, runners)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runWithContext() error = %v, want containing %q", err, test.want)
			}
			if strings.Contains(err.Error(), "load configuration") {
				t.Fatalf("legacy arguments reached configuration loading: %v", err)
			}
		})
	}
	if runnerCalls != 0 {
		t.Fatalf("legacy arguments reached runtime runners %d times", runnerCalls)
	}
}

func TestHelpSucceedsBeforeConfigurationOrRuntime(t *testing.T) {
	t.Setenv(config.ConfigEnvironmentVariable, filepath.Join(t.TempDir(), "missing.yaml"))
	runnerCalls := 0
	runners := runtimeRunners{
		terminal: func(context.Context, string, *config.Config) error { runnerCalls++; return nil },
		plain:    func(context.Context, *config.Config) error { runnerCalls++; return nil },
	}
	var help bytes.Buffer
	if err := runWithContext(context.Background(), []string{"-h"}, &help, true, runners); err != nil {
		t.Fatalf("runWithContext(-h) error = %v", err)
	}
	if runnerCalls != 0 {
		t.Fatalf("help reached runtime runners %d times", runnerCalls)
	}
	if !strings.Contains(help.String(), "-no-ui") {
		t.Fatalf("help output = %q, want -no-ui", help.String())
	}
}

func TestRuntimeErrorsAreWrapped(t *testing.T) {
	installRuntimeConfig(t)
	sentinel := errors.New("runner failed")
	tests := []struct {
		name        string
		interactive bool
		runners     runtimeRunners
		want        string
	}{
		{
			name:        "Bubble Tea",
			interactive: true,
			runners: runtimeRunners{
				terminal: func(context.Context, string, *config.Config) error { return sentinel },
			},
			want: "run terminal UI: runner failed",
		},
		{
			name: "plain lifecycle",
			runners: runtimeRunners{
				plain: func(context.Context, *config.Config) error { return sentinel },
			},
			want: "run lifecycle: runner failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), nil, io.Discard, test.interactive, test.runners)
			if !errors.Is(err, sentinel) || err.Error() != test.want {
				t.Fatalf("runWithContext() error = %v, want %q wrapping sentinel", err, test.want)
			}
		})
	}
}

func TestMissingRuntimeRunnerIsReported(t *testing.T) {
	installRuntimeConfig(t)
	tests := []struct {
		name        string
		interactive bool
		want        string
	}{
		{name: "Bubble Tea", interactive: true, want: "run terminal UI: terminal runner is not configured"},
		{name: "plain lifecycle", want: "run lifecycle: plain runner is not configured"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), nil, io.Discard, test.interactive, runtimeRunners{})
			if err == nil || err.Error() != test.want {
				t.Fatalf("runWithContext() error = %v, want %q", err, test.want)
			}
		})
	}
}

func installRuntimeConfig(t *testing.T) (string, string) {
	t.Helper()
	dataDir := filepath.ToSlash(filepath.Join(t.TempDir(), "data"))
	configPath := filepath.Join(t.TempDir(), "indexnode.yaml")
	contents := fmt.Sprintf("node_id: test-node\ndata_dir: %q\n", dataDir)
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatalf("write test configuration: %v", err)
	}
	// PathFromEnvironment trims launcher input before both loading and passing
	// the source to Bubble Tea for display/reload.
	t.Setenv(config.ConfigEnvironmentVariable, " \t"+configPath+" \r\n")
	return configPath, dataDir
}
