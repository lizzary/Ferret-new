package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lizzary/index-node/internal/cli"
	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/lifecycle"
	"github.com/mattn/go-isatty"
)

var version = "dev"

func runTerminalUI(ctx context.Context, configPath string, cfg *config.Config) error {
	// Initialize the native runtime before Bubble Tea takes ownership of the
	// terminal. The index adapter contains tantivy-go's unsolicited stdout.
	if err := index.InitializeTantivy(); err != nil {
		return fmt.Errorf("initialize terminal runtime: %w", err)
	}

	statePath := filepath.Join(cfg.DataDir, "cli.json")
	logHub := cli.NewLogHub(3000)
	return cli.Run(cli.Config{
		Context:    ctx,
		Version:    version,
		Theme:      cli.LoadTheme(statePath),
		StatePath:  statePath,
		ConfigPath: configPath,
		NodeConfig: cfg,
		Log:        logHub,
		RunLifecycle: func(runCtx context.Context, current *config.Config, logWriter io.Writer) error {
			return lifecycle.RunWithOptions(runCtx, current, lifecycle.RunOptions{LogWriter: logWriter})
		},
		LoadConfig: config.Load,
		Health:     cli.FetchHealth,
		FatalLifecycleError: func(err error) bool {
			return errors.Is(err, lifecycle.ErrComponentsLive)
		},
	})
}

func isTerminal(file *os.File) bool {
	fd := file.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}
