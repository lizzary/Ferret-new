package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/maintenance"
	"github.com/lizzary/index-node/internal/store"
)

const (
	defaultSearchLimit     = 20
	defaultDeadLetterLimit = 100
)

// executeMaintenance is the stopped-node application boundary used by the
// terminal. It parses slash-command arguments and calls the typed lifecycle
// operations directly; no command-line flag set or JSON serialization is
// involved.
func executeMaintenance(ctx context.Context, cfg *config.Config, command string, arguments []string) ([]string, error) {
	command = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(command)), "/")
	switch command {
	case "enqueue":
		paths, help, err := parseEnqueueArguments(arguments)
		if err != nil {
			return nil, err
		}
		if help {
			return maintenanceHelp("enqueue"), nil
		}
		dataDir, err := maintenanceDataDir(cfg)
		if err != nil {
			return nil, err
		}
		results, err := maintenance.EnqueuePaths(ctx, dataDir, paths)
		if err != nil {
			if len(results) == 0 {
				return nil, err
			}
			return formatEnqueueResults(results), err
		}
		return formatEnqueueResults(results), nil

	case "search":
		request, err := parseSearchArguments(arguments)
		if err != nil {
			return nil, err
		}
		if request.help {
			return maintenanceHelp("search"), nil
		}
		dataDir, err := maintenanceDataDir(cfg)
		if err != nil {
			return nil, err
		}
		hits, err := maintenance.SearchKeyword(ctx, dataDir, request.query, request.limit)
		if err != nil {
			if len(hits) == 0 {
				return nil, err
			}
			return formatSearchResults(hits), err
		}
		return formatSearchResults(hits), nil

	case "deadletters":
		return executeDeadLetterMaintenance(ctx, cfg, arguments)

	default:
		return nil, fmt.Errorf("unsupported maintenance command %q", command)
	}
}

func executeDeadLetterMaintenance(ctx context.Context, cfg *config.Config, arguments []string) ([]string, error) {
	if len(arguments) == 0 {
		return nil, fmt.Errorf("deadletters command: expected list or redrive")
	}
	if len(arguments) == 1 && isMaintenanceHelp(arguments[0]) {
		return maintenanceHelp("deadletters"), nil
	}

	switch arguments[0] {
	case "list":
		request, err := parseDeadLetterListArguments(arguments[1:])
		if err != nil {
			return nil, err
		}
		if request.help {
			return maintenanceHelp("deadletters-list"), nil
		}
		dataDir, err := maintenanceDataDir(cfg)
		if err != nil {
			return nil, err
		}
		dead, err := maintenance.ListDeadLetters(ctx, dataDir, request.errorClass, request.limit)
		if err != nil {
			if len(dead) == 0 {
				return nil, err
			}
			return formatDeadLetterList(dead), err
		}
		return formatDeadLetterList(dead), nil

	case "redrive":
		request, err := parseDeadLetterRedriveArguments(arguments[1:])
		if err != nil {
			return nil, err
		}
		if request.help {
			return maintenanceHelp("deadletters-redrive"), nil
		}
		dataDir, err := maintenanceDataDir(cfg)
		if err != nil {
			return nil, err
		}
		results, err := maintenance.RedriveDeadLetters(ctx, dataDir, request.fileIDs, request.errorClass, "bubble-tea")
		if err != nil {
			if len(results) == 0 {
				return nil, err
			}
			return formatDeadLetterRedrive(results), err
		}
		return formatDeadLetterRedrive(results), nil

	default:
		return nil, fmt.Errorf("deadletters command: unknown subcommand %q", arguments[0])
	}
}

type searchMaintenanceRequest struct {
	query string
	limit int
	help  bool
}

func parseSearchArguments(arguments []string) (searchMaintenanceRequest, error) {
	request := searchMaintenanceRequest{limit: defaultSearchLimit}
	positionals := make([]string, 0, len(arguments))
	limitSet := false
	optionsEnded := false

	for index := 0; index < len(arguments); index++ {
		token := arguments[index]
		if !optionsEnded && isMaintenanceHelp(token) {
			request.help = true
			return request, nil
		}
		if !optionsEnded && token == "--" {
			optionsEnded = true
			continue
		}
		name, value, inline, option := splitMaintenanceOption(token)
		if !optionsEnded && option {
			if name != "limit" {
				return request, fmt.Errorf("search command: unknown option %q", token)
			}
			if limitSet {
				return request, fmt.Errorf("search command: -limit may be provided only once")
			}
			if !inline {
				index++
				if index >= len(arguments) {
					return request, fmt.Errorf("search command: -limit requires a value")
				}
				value = arguments[index]
			}
			limit, err := parseMaintenanceLimit("search command", value)
			if err != nil {
				return request, err
			}
			request.limit = limit
			limitSet = true
			continue
		}
		positionals = append(positionals, token)
	}

	request.query = strings.TrimSpace(strings.Join(positionals, " "))
	if request.query == "" {
		return request, fmt.Errorf("search command: query is required")
	}
	return request, nil
}

type deadLetterListMaintenanceRequest struct {
	errorClass string
	limit      int
	help       bool
}

func parseDeadLetterListArguments(arguments []string) (deadLetterListMaintenanceRequest, error) {
	request := deadLetterListMaintenanceRequest{limit: defaultDeadLetterLimit}
	classSet := false
	limitSet := false

	for index := 0; index < len(arguments); index++ {
		token := arguments[index]
		if isMaintenanceHelp(token) {
			request.help = true
			return request, nil
		}
		name, value, inline, option := splitMaintenanceOption(token)
		if !option || token == "--" {
			return request, fmt.Errorf("deadletters list command: unexpected argument %q", token)
		}
		switch name {
		case "class":
			if classSet {
				return request, fmt.Errorf("deadletters list command: -class may be provided only once")
			}
			if !inline {
				index++
				if index >= len(arguments) {
					return request, fmt.Errorf("deadletters list command: -class requires a value")
				}
				value = arguments[index]
			}
			class, err := validateMaintenanceDeadLetterClass(value, true)
			if err != nil {
				return request, err
			}
			request.errorClass = class
			classSet = true

		case "limit":
			if limitSet {
				return request, fmt.Errorf("deadletters list command: -limit may be provided only once")
			}
			if !inline {
				index++
				if index >= len(arguments) {
					return request, fmt.Errorf("deadletters list command: -limit requires a value")
				}
				value = arguments[index]
			}
			limit, err := parseMaintenanceLimit("deadletters list command", value)
			if err != nil {
				return request, err
			}
			request.limit = limit
			limitSet = true

		default:
			return request, fmt.Errorf("deadletters list command: unknown option %q", token)
		}
	}
	return request, nil
}

type deadLetterRedriveMaintenanceRequest struct {
	fileIDs    []int64
	errorClass string
	help       bool
}

func parseDeadLetterRedriveArguments(arguments []string) (deadLetterRedriveMaintenanceRequest, error) {
	var request deadLetterRedriveMaintenanceRequest
	fileIDsSet := false
	classSet := false

	for index := 0; index < len(arguments); index++ {
		token := arguments[index]
		if isMaintenanceHelp(token) {
			request.help = true
			return request, nil
		}
		name, value, inline, option := splitMaintenanceOption(token)
		if !option || token == "--" {
			return request, fmt.Errorf("deadletters redrive command: unexpected argument %q", token)
		}
		switch name {
		case "file-ids":
			if fileIDsSet {
				return request, fmt.Errorf("deadletters redrive command: -file-ids may be provided only once")
			}
			if !inline {
				index++
				if index >= len(arguments) {
					return request, fmt.Errorf("deadletters redrive command: -file-ids requires a value")
				}
				value = arguments[index]
			}
			fileIDs, err := parseMaintenanceFileIDs(value)
			if err != nil {
				return request, err
			}
			request.fileIDs = fileIDs
			fileIDsSet = true

		case "class":
			if classSet {
				return request, fmt.Errorf("deadletters redrive command: -class may be provided only once")
			}
			if !inline {
				index++
				if index >= len(arguments) {
					return request, fmt.Errorf("deadletters redrive command: -class requires a value")
				}
				value = arguments[index]
			}
			class := strings.TrimSpace(value)
			if class != "" {
				validated, err := validateMaintenanceDeadLetterClass(class, false)
				if err != nil {
					return request, err
				}
				class = validated
			}
			request.errorClass = class
			classSet = true

		default:
			return request, fmt.Errorf("deadletters redrive command: unknown option %q", token)
		}
	}

	if (len(request.fileIDs) == 0) == (request.errorClass == "") {
		return request, fmt.Errorf("deadletters redrive command: provide exactly one of -file-ids or -class")
	}
	return request, nil
}

func parseEnqueueArguments(arguments []string) ([]string, bool, error) {
	paths := make([]string, 0, len(arguments))
	optionsEnded := false
	for _, token := range arguments {
		if !optionsEnded && isMaintenanceHelp(token) {
			return nil, true, nil
		}
		if !optionsEnded && token == "--" {
			optionsEnded = true
			continue
		}
		if !optionsEnded {
			_, _, _, option := splitMaintenanceOption(token)
			if option {
				return nil, false, fmt.Errorf("enqueue command: unknown option %q", token)
			}
		}
		paths = append(paths, token)
	}
	if len(paths) == 0 {
		return nil, false, fmt.Errorf("enqueue command: at least one path is required")
	}
	return paths, false, nil
}

func splitMaintenanceOption(token string) (name, value string, inline, option bool) {
	if token == "-" || !strings.HasPrefix(token, "-") {
		return "", "", false, false
	}
	trimmed := strings.TrimPrefix(token, "-")
	trimmed = strings.TrimPrefix(trimmed, "-")
	if trimmed == "" {
		return "", "", false, false
	}
	if separator := strings.IndexByte(trimmed, '='); separator >= 0 {
		return trimmed[:separator], trimmed[separator+1:], true, true
	}
	return trimmed, "", false, true
}

func parseMaintenanceLimit(scope, value string) (int, error) {
	limit, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s: invalid limit %q", scope, value)
	}
	if limit < 1 || limit > maintenance.MaxResultLimit {
		return 0, fmt.Errorf("%s: limit must be between 1 and %d", scope, maintenance.MaxResultLimit)
	}
	return limit, nil
}

func parseMaintenanceFileIDs(value string) ([]int64, error) {
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

func validateMaintenanceDeadLetterClass(value string, allowEmpty bool) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" && allowEmpty {
		return "", nil
	}
	if _, err := errclass.Parse(value); err != nil {
		return "", fmt.Errorf("deadletters command: %w", err)
	}
	return value, nil
}

func maintenanceDataDir(cfg *config.Config) (string, error) {
	if cfg == nil || strings.TrimSpace(cfg.DataDir) == "" {
		return "", fmt.Errorf("stopped-node maintenance requires a resolved data_dir")
	}
	return cfg.DataDir, nil
}

func isMaintenanceHelp(token string) bool {
	switch token {
	case "-h", "--help", "-help":
		return true
	default:
		return false
	}
}

func maintenanceHelp(command string) []string {
	switch command {
	case "enqueue":
		return []string{"/enqueue <path>... - enqueue paths while stopped"}
	case "search":
		return []string{"/search [-limit N] <query> - keyword search"}
	case "deadletters":
		return []string{
			"/deadletters list [-class C] [-limit N]",
			"/deadletters redrive -file-ids 1,2",
			"/deadletters redrive -class poison",
		}
	case "deadletters-list":
		return []string{"/deadletters list [-class C] [-limit N]"}
	case "deadletters-redrive":
		return []string{
			"/deadletters redrive -file-ids 1,2",
			"/deadletters redrive -class poison",
		}
	default:
		panic("unknown maintenance help command")
	}
}

func formatEnqueueResults(results []maintenance.EnqueueResult) []string {
	lines := []string{fmt.Sprintf("Enqueued %d path(s).", len(results))}
	for _, result := range results {
		state := "coalesced"
		if result.Inserted {
			state = "inserted"
		}
		lines = append(lines, fmt.Sprintf(
			"task=%d generation=%d %s %s",
			result.TaskID,
			result.Generation,
			state,
			result.Path,
		))
	}
	return lines
}

func formatSearchResults(hits []index.KeywordHit) []string {
	lines := []string{fmt.Sprintf("Keyword search returned %d hit(s).", len(hits))}
	for _, hit := range hits {
		lines = append(lines, fmt.Sprintf(
			"file=%d score=%.4f status=%s kind=%s %s",
			hit.FileID,
			hit.Score,
			hit.Status,
			hit.Kind,
			hit.Path,
		))
	}
	return lines
}

func formatDeadLetterList(dead []store.DeadLetter) []string {
	lines := []string{fmt.Sprintf("Found %d dead letter(s).", len(dead))}
	for _, item := range dead {
		lines = append(lines, fmt.Sprintf(
			"file=%d generation=%d class=%s stage=%s %s",
			item.FileID,
			item.Generation,
			item.ErrorClass,
			item.Stage,
			item.Path,
		))
	}
	return lines
}

func formatDeadLetterRedrive(results []store.DeadLetterRedriveResult) []string {
	lines := []string{fmt.Sprintf("Redrove %d dead letter(s).", len(results))}
	for _, result := range results {
		lines = append(lines, fmt.Sprintf(
			"file=%d task=%d generation=%d %s",
			result.DeadLetter.FileID,
			result.EnqueueResult.Task.ID,
			result.EnqueueResult.Task.Generation,
			result.DeadLetter.Path,
		))
	}
	return lines
}
