package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/lifecycle"
	"github.com/lizzary/index-node/internal/obs"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		logger := obs.NewJSONLogger(os.Stderr, obs.LoggerOptions{Level: slog.LevelError})
		logger.Error("index-node stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

// run preserves the original no-subcommand behavior while making signal
// cancellation available to one-shot commands as well.
func run(arguments []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runContext(ctx, arguments, os.Stdout, os.Stderr)
}

func runContext(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("indexnode", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the index-node YAML configuration")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse command line: %w", err)
	}

	command := "run"
	commandArguments := flags.Args()
	if len(commandArguments) > 0 {
		command = commandArguments[0]
		commandArguments = commandArguments[1:]
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	switch command {
	case "run":
		if len(commandArguments) != 0 {
			return fmt.Errorf("run command: unexpected arguments: %v", commandArguments)
		}
		if err := lifecycle.Run(ctx, cfg); err != nil {
			return fmt.Errorf("run lifecycle: %w", err)
		}
		return nil
	case "enqueue":
		return runEnqueueCommand(ctx, cfg, commandArguments, stdout, stderr)
	case "search":
		return runSearchCommand(ctx, cfg, commandArguments, stdout, stderr)
	case "deadletters":
		return runDeadLettersCommand(ctx, cfg, commandArguments, stdout, stderr)
	default:
		return fmt.Errorf("parse command line: unknown command %q", command)
	}
}

func runEnqueueCommand(ctx context.Context, cfg *config.Config, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("indexnode enqueue", flag.ContinueOnError)
	flags.SetOutput(stderr)
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse enqueue command: %w", err)
	}
	if flags.NArg() == 0 {
		return errors.New("enqueue command: at least one path is required")
	}
	results, err := lifecycle.EnqueuePaths(ctx, cfg.DataDir, flags.Args())
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(results); err != nil {
		return fmt.Errorf("encode enqueue result: %w", err)
	}
	return nil
}

func runSearchCommand(ctx context.Context, cfg *config.Config, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("indexnode search", flag.ContinueOnError)
	flags.SetOutput(stderr)
	limit := flags.Int("limit", 20, "maximum number of keyword hits (1-1000)")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse search command: %w", err)
	}
	if *limit < 1 || *limit > 1000 {
		return errors.New("search command: limit must be between 1 and 1000")
	}
	query := strings.TrimSpace(strings.Join(flags.Args(), " "))
	if query == "" {
		return errors.New("search command: query is required")
	}
	hits, err := lifecycle.SearchKeyword(ctx, cfg.DataDir, query, *limit)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(hits); err != nil {
		return fmt.Errorf("encode search result: %w", err)
	}
	return nil
}

func runDeadLettersCommand(ctx context.Context, cfg *config.Config, arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		return errors.New("deadletters command: expected list or redrive")
	}
	switch arguments[0] {
	case "list":
		flags := flag.NewFlagSet("indexnode deadletters list", flag.ContinueOnError)
		flags.SetOutput(stderr)
		errorClass := flags.String("class", "", "optional error class filter")
		limit := flags.Int("limit", 100, "maximum records (1-1000)")
		if err := flags.Parse(arguments[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return fmt.Errorf("parse deadletters list command: %w", err)
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("deadletters list command: unexpected arguments: %v", flags.Args())
		}
		if *limit < 1 || *limit > 1000 {
			return errors.New("deadletters list command: limit must be between 1 and 1000")
		}
		if err := validateDeadLetterClass(*errorClass, true); err != nil {
			return err
		}
		dead, err := lifecycle.ListDeadLetters(ctx, cfg.DataDir, *errorClass, *limit)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(stdout).Encode(dead); err != nil {
			return fmt.Errorf("encode dead-letter list: %w", err)
		}
		return nil
	case "redrive":
		flags := flag.NewFlagSet("indexnode deadletters redrive", flag.ContinueOnError)
		flags.SetOutput(stderr)
		fileIDsValue := flags.String("file-ids", "", "comma-separated dead-letter file IDs")
		errorClass := flags.String("class", "", "redrive every dead letter of this class")
		if err := flags.Parse(arguments[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return fmt.Errorf("parse deadletters redrive command: %w", err)
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("deadletters redrive command: unexpected arguments: %v", flags.Args())
		}
		fileIDs, err := parseFileIDs(*fileIDsValue)
		if err != nil {
			return err
		}
		class := strings.TrimSpace(*errorClass)
		if (len(fileIDs) == 0) == (class == "") {
			return errors.New("deadletters redrive command: provide exactly one of -file-ids or -class")
		}
		if class != "" {
			if err := validateDeadLetterClass(class, false); err != nil {
				return err
			}
		}
		results, err := lifecycle.RedriveDeadLetters(ctx, cfg.DataDir, fileIDs, class, "cli")
		if err != nil {
			return err
		}
		if err := json.NewEncoder(stdout).Encode(results); err != nil {
			return fmt.Errorf("encode dead-letter redrive result: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("deadletters command: unknown subcommand %q", arguments[0])
	}
}

func parseFileIDs(value string) ([]int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	seen := make(map[int64]struct{})
	ids := make([]int64, 0)
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("deadletters redrive command: invalid file ID %q", part)
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func validateDeadLetterClass(value string, allowEmpty bool) error {
	value = strings.TrimSpace(value)
	if value == "" && allowEmpty {
		return nil
	}
	if _, err := errclass.Parse(value); err != nil {
		return fmt.Errorf("deadletters command: %w", err)
	}
	return nil
}
