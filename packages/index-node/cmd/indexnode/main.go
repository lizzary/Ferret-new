package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/lifecycle"
	"github.com/lizzary/index-node/internal/obs"
)

type runtimeRunners struct {
	terminal func(context.Context, string, *config.Config) error
	plain    func(context.Context, *config.Config) error
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		logger := obs.NewJSONLogger(os.Stderr, obs.LoggerOptions{Level: slog.LevelError})
		logger.Error("index-node stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

// run keeps the process boundary deliberately small: signals, the sole
// command-line option, terminal capability detection, configuration loading,
// and selection of one runtime. All operator commands live inside Bubble Tea.
func run(arguments []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithContext(
		ctx,
		arguments,
		os.Stderr,
		isTerminal(os.Stdin) && isTerminal(os.Stdout),
		runtimeRunners{terminal: runTerminalUI, plain: lifecycle.Run},
	)
}

func runWithContext(
	ctx context.Context,
	arguments []string,
	stderr io.Writer,
	interactiveTerminal bool,
	runners runtimeRunners,
) error {
	flags := flag.NewFlagSet("indexnode", flag.ContinueOnError)
	flags.SetOutput(stderr)
	noUI := flags.Bool("no-ui", false, "disable Bubble Tea and run the plain lifecycle")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse command line: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("parse command line: unexpected positional arguments: %v", flags.Args())
	}

	configPath := config.PathFromEnvironment()
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	if interactiveTerminal && !*noUI {
		if runners.terminal == nil {
			return errors.New("run terminal UI: terminal runner is not configured")
		}
		if err := runners.terminal(ctx, configPath, cfg); err != nil {
			return fmt.Errorf("run terminal UI: %w", err)
		}
		return nil
	}
	if runners.plain == nil {
		return errors.New("run lifecycle: plain runner is not configured")
	}
	if err := runners.plain(ctx, cfg); err != nil {
		return fmt.Errorf("run lifecycle: %w", err)
	}
	return nil
}
