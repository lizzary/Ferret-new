package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/lizzary/index-node/internal/config"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

// Config supplies the process-boundary lifecycle, configuration, and health
// dependencies. Stopped-node maintenance is parsed and formatted here, then
// sent directly to the typed maintenance service rather than being routed
// back through the executable command layer.
type Config struct {
	Context    context.Context
	Version    string
	Theme      ThemeMode
	StatePath  string
	ConfigPath string
	NodeConfig *config.Config
	Log        *LogHub

	RunLifecycle func(context.Context, *config.Config, io.Writer) error
	LoadConfig   func(path string) (*config.Config, error)
	Health       func(context.Context, *config.Config) (NodeStatus, error)
	// FatalLifecycleError identifies shutdown failures that deliberately leave
	// process-owned resources live and therefore require the process boundary
	// to exit instead of offering restart or stopped-node maintenance.
	FatalLifecycleError func(error) bool
}

type screen uint8

const (
	homeScreen screen = iota
	logScreen
)

type lifecyclePhase uint8

const (
	phaseRunning lifecyclePhase = iota
	phaseStopping
	phaseStopped
	phaseFailed
)

type tickMsg time.Time
type healthTickMsg time.Time
type contextCanceledMsg struct{}

type lifecycleExitedMsg struct {
	run *lifecycleRun
	err error
}

type healthResultMsg struct {
	run    *lifecycleRun
	status NodeStatus
	err    error
}

type maintenanceResultMsg struct {
	command string
	lines   []string
	err     error
}

type configReloadedMsg struct {
	cfg        *config.Config
	sourcePath string
	kind       configLoadKind
	err        error
}

type configLoadKind uint8

const (
	configReload configLoadKind = iota
	configLoad
)

func (kind configLoadKind) operation() string {
	if kind == configLoad {
		return "configuration load"
	}
	return "configuration reload"
}

func (kind configLoadKind) title() string {
	if kind == configLoad {
		return "Configuration load"
	}
	return "Configuration reload"
}

func (kind configLoadKind) presentParticiple() string {
	if kind == configLoad {
		return "Loading"
	}
	return "Reloading"
}

func (kind configLoadKind) pastParticiple() string {
	if kind == configLoad {
		return "loaded"
	}
	return "reloaded"
}

type model struct {
	cfg          Config
	ctx          context.Context
	controller   *lifecycleController
	run          *lifecycleRun
	nodeCfg      *config.Config
	palette      palette
	detectedDark bool
	mode         ThemeMode
	screen       screen
	phase        lifecyclePhase
	width        int
	height       int
	input        string
	notice       string
	spinner      int
	logOffset    int
	followLogs   bool
	levelFilter  int
	lastLogCount int
	health       NodeStatus
	healthErr    error
	healthBusy   bool
	busy         bool
	busyName     string
	quitWhenIdle bool
}

const (
	spinnerInterval         = 120 * time.Millisecond
	healthInterval          = time.Second
	lifecycleShutdownWindow = 35 * time.Second
)

// Run starts the node lifecycle, owns the Bubble Tea program, then cancels and
// waits for the lifecycle before returning. This final synchronous wait is the
// process boundary that preserves lifecycle clean-shutdown semantics.
func Run(cfg Config) error {
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	if cfg.NodeConfig == nil {
		return errors.New("cli: node configuration is required")
	}
	if cfg.RunLifecycle == nil {
		return errors.New("cli: RunLifecycle callback is required")
	}
	mode, err := ParseTheme(string(cfg.Theme))
	if err != nil {
		return fmt.Errorf("cli: %w", err)
	}
	cfg.Theme = mode
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	if cfg.Log == nil {
		cfg.Log = NewLogHub(defaultLogCapacity)
	}
	if cfg.Health == nil {
		cfg.Health = FetchHealth
	}
	if cfg.StatePath == "" && cfg.NodeConfig.DataDir != "" {
		cfg.StatePath = filepath.Join(cfg.NodeConfig.DataDir, "cli.json")
	}

	appCtx, cancelApp := context.WithCancel(cfg.Context)
	controller := newLifecycleController(appCtx, cfg.RunLifecycle, cfg.Log)
	run, started := controller.Start(cfg.NodeConfig)
	if !started {
		cancelApp()
		return errors.New("cli: lifecycle did not start")
	}
	m := newModel(appCtx, cfg, controller, run)
	// The process boundary already translates SIGINT/SIGTERM into cfg.Context.
	// Disabling Bubble Tea's parallel handler keeps every exit request on the
	// same model path, so maintenance and lifecycle shutdown are both joined.
	_, programErr := tea.NewProgram(m, tea.WithoutSignalHandler()).Run()

	// /quit and Ctrl+C cancel here even if the terminal backend returned an
	// error before Update could process the key. Context cancellation also
	// reaches any in-flight health request.
	cancelApp()
	controller.Stop()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), lifecycleShutdownWindow)
	// The user may have stopped and restarted the node while the dashboard was
	// open. Always join the controller's latest generation, not the initial one.
	lifecycleErr := controller.Current().wait(waitCtx)
	cancelWait()
	if errors.Is(lifecycleErr, context.DeadlineExceeded) {
		lifecycleErr = fmt.Errorf("cli: lifecycle did not stop within %s: %w", lifecycleShutdownWindow, lifecycleErr)
	} else if lifecycleErr != nil {
		lifecycleErr = fmt.Errorf("cli: lifecycle: %w", lifecycleErr)
	}
	return errors.Join(programErr, lifecycleErr)
}

func newModel(ctx context.Context, cfg Config, controller *lifecycleController, run *lifecycleRun) *model {
	dark := DetectDark(cfg.Theme)
	return &model{
		cfg: cfg, ctx: ctx, controller: controller, run: run,
		nodeCfg: cfg.NodeConfig, palette: newPalette(dark), detectedDark: dark,
		mode: cfg.Theme, phase: phaseRunning, width: 80, height: 24,
		followLogs: true,
	}
}

func (m *model) Init() tea.Cmd {
	commands := []tea.Cmd{m.tick(), m.healthTick(200 * time.Millisecond), waitLifecycle(m.run)}
	if m.ctx.Done() != nil {
		commands = append(commands, func() tea.Msg {
			<-m.ctx.Done()
			return contextCanceledMsg{}
		})
	}
	return tea.Batch(commands...)
}

func (m *model) tick() tea.Cmd {
	return tea.Tick(spinnerInterval, func(now time.Time) tea.Msg { return tickMsg(now) })
}

func (m *model) healthTick(after time.Duration) tea.Cmd {
	return tea.Tick(after, func(now time.Time) tea.Msg { return healthTickMsg(now) })
}

func waitLifecycle(run *lifecycleRun) tea.Cmd {
	return func() tea.Msg {
		return lifecycleExitedMsg{run: run, err: run.wait(context.Background())}
	}
}

func (m *model) fetchHealth() tea.Cmd {
	run := m.run
	cfg := m.nodeCfg
	return func() tea.Msg {
		status, err := m.cfg.Health(m.ctx, cfg)
		return healthResultMsg{run: run, status: status, err: err}
	}
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = max(36, msg.Width)
		m.height = max(12, msg.Height)
		return m, nil
	case tickMsg:
		m.spinner = (m.spinner + 1) % 4
		count := len(m.cfg.Log.Snapshot())
		if !m.followLogs && count > m.lastLogCount {
			m.logOffset += count - m.lastLogCount
		}
		m.lastLogCount = count
		return m, m.tick()
	case healthTickMsg:
		commands := []tea.Cmd{m.healthTick(healthInterval)}
		if m.phase == phaseRunning && !m.healthBusy {
			m.healthBusy = true
			commands = append(commands, m.fetchHealth())
		}
		return m, tea.Batch(commands...)
	case healthResultMsg:
		if msg.run != m.run {
			return m, nil
		}
		m.healthBusy = false
		m.healthErr = msg.err
		if msg.err == nil {
			m.health = msg.status
		}
		return m, nil
	case lifecycleExitedMsg:
		if msg.run != m.run {
			return m, nil
		}
		m.healthBusy = false
		m.health = NodeStatus{}
		m.healthErr = nil
		if msg.err != nil {
			m.phase = phaseFailed
			if m.cfg.FatalLifecycleError != nil && m.cfg.FatalLifecycleError(msg.err) {
				m.notice = "The node could not stop every component safely; the process must exit: " + safeInline(msg.err.Error())
				m.cfg.Log.Error("lifecycle", "fatal shutdown failure; process exit required: %v", msg.err)
				return m, tea.Quit
			}
			m.notice = "Node lifecycle stopped with an error: " + safeInline(msg.err.Error())
			m.cfg.Log.Error("lifecycle", "stopped: %v", msg.err)
		} else {
			m.phase = phaseStopped
			m.notice = "Node lifecycle stopped cleanly."
			m.cfg.Log.Info("lifecycle", "stopped cleanly")
		}
		if m.quitWhenIdle && !m.busy {
			return m, tea.Quit
		}
		return m, nil
	case maintenanceResultMsg:
		m.busy = false
		m.busyName = ""
		if msg.err != nil {
			if len(msg.lines) != 0 {
				for _, line := range msg.lines {
					m.cfg.Log.Warn("maintenance", "/%s result before error: %s", msg.command, line)
				}
				m.notice = fmt.Sprintf(
					"/%s reported an error after producing results: %s\n%s\n%d result lines were written to /log; verify them before retrying.",
					msg.command, safeInline(msg.err.Error()), safeInline(msg.lines[0]), len(msg.lines),
				)
			} else {
				m.notice = fmt.Sprintf("/%s failed: %s", msg.command, safeInline(msg.err.Error()))
			}
			m.cfg.Log.Error("maintenance", "/%s failed: %v", msg.command, msg.err)
		} else if len(msg.lines) == 0 {
			m.notice = fmt.Sprintf("/%s completed.", msg.command)
			m.cfg.Log.Info("maintenance", "/%s completed", msg.command)
		} else {
			for _, line := range msg.lines {
				m.cfg.Log.Info("maintenance", "/%s: %s", msg.command, line)
			}
			m.notice = safeInline(msg.lines[0])
			if len(msg.lines) > 1 {
				m.notice += fmt.Sprintf("\n%d result lines were written to /log.", len(msg.lines))
			}
		}
		if m.quitWhenIdle {
			return m, tea.Quit
		}
		return m, nil
	case configReloadedMsg:
		m.busy = false
		m.busyName = ""
		operation := msg.kind.operation()
		source := displayConfigPath(msg.sourcePath)
		if msg.err != nil {
			m.notice = fmt.Sprintf("%s failed for %s: %s", msg.kind.title(), source, safeInline(msg.err.Error()))
			m.cfg.Log.Error("config", "%s failed for %s: %v", operation, source, msg.err)
		} else if msg.cfg == nil {
			m.notice = fmt.Sprintf("%s failed for %s: loader returned no configuration.", msg.kind.title(), source)
			m.cfg.Log.Error("config", "%s for %s returned nil configuration", operation, source)
		} else {
			previousCfg := m.nodeCfg
			previousStatePath := m.cfg.StatePath
			statePathFollowsDataDir := previousCfg != nil && previousCfg.DataDir != "" &&
				filepath.Clean(previousStatePath) == filepath.Clean(filepath.Join(previousCfg.DataDir, "cli.json"))
			m.nodeCfg = msg.cfg
			m.cfg.NodeConfig = msg.cfg
			m.cfg.ConfigPath = msg.sourcePath
			stateSaveFailed := false
			if statePathFollowsDataDir && msg.cfg.DataDir != "" {
				nextStatePath := filepath.Join(msg.cfg.DataDir, "cli.json")
				m.cfg.StatePath = nextStatePath
				if filepath.Clean(previousStatePath) != filepath.Clean(nextStatePath) {
					if saveErr := SaveTheme(nextStatePath, m.mode); saveErr != nil {
						stateSaveFailed = true
						m.cfg.Log.Warn("config", "terminal state moved to %s but current theme could not be saved: %v", safeInline(nextStatePath), saveErr)
					} else {
						m.cfg.Log.Info("config", "terminal state moved to %s", safeInline(nextStatePath))
					}
				}
			}
			configurationNotice := m.showConfiguration()
			m.notice = fmt.Sprintf("Configuration %s from %s. It will be used by the next /start.\n%s", msg.kind.pastParticiple(), source, configurationNotice)
			if stateSaveFailed {
				m.notice = fmt.Sprintf("Configuration %s from %s; terminal appearance could not be saved in the new data_dir (see /log).\n%s", msg.kind.pastParticiple(), source, configurationNotice)
			}
			m.cfg.Log.Info("config", "%s %s", msg.kind.pastParticiple(), source)
		}
		if m.quitWhenIdle {
			return m, tea.Quit
		}
		return m, nil
	case contextCanceledMsg:
		m.quitWhenIdle = true
		active := m.controller.Stop()
		var clear tea.Cmd
		if m.screen == logScreen {
			m.screen = homeScreen
			clear = tea.ClearScreen
		}
		if m.busy {
			m.notice = "Shutdown requested; waiting for " + m.busyName + " to finish."
			return m, clear
		}
		if active {
			m.phase = phaseStopping
			m.notice = "Shutdown requested; stopping the node lifecycle cleanly..."
			return m, clear
		}
		return m, tea.Quit
	case tea.PasteMsg:
		// Bubble Tea v2 reports bracketed paste separately from key presses.
		// Keep pasted commands and Windows paths in the same editable buffer;
		// terminal controls are escaped when the prompt is rendered.
		m.input += msg.Content
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	default:
		return m, nil
	}
}

func (m *model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	key := msg.Key()
	if msg.String() == "ctrl+c" {
		return m.requestQuit()
	}

	if m.screen == logScreen && m.input == "" {
		switch key.Code {
		case tea.KeyEscape:
			m.screen = homeScreen
			return m, tea.ClearScreen
		case tea.KeyUp:
			m.followLogs = false
			m.logOffset++
			return m, nil
		case tea.KeyDown:
			m.logOffset = max(0, m.logOffset-1)
			m.followLogs = m.logOffset == 0
			return m, nil
		case tea.KeyPgUp:
			m.followLogs = false
			m.logOffset += max(3, m.logBodyHeight()-2)
			return m, nil
		case tea.KeyPgDown:
			m.logOffset = max(0, m.logOffset-max(3, m.logBodyHeight()-2))
			m.followLogs = m.logOffset == 0
			return m, nil
		case tea.KeyEnd:
			m.logOffset = 0
			m.followLogs = true
			return m, nil
		}

		switch key.Text {
		case "f":
			m.followLogs = !m.followLogs
			if m.followLogs {
				m.logOffset = 0
			}
			return m, nil
		case "1", "2", "3", "4":
			m.levelFilter = int(key.Text[0] - '1')
			m.logOffset = 0
			m.followLogs = true
			return m, nil
		}
	}

	switch key.Code {
	case tea.KeyEnter:
		command := strings.TrimSpace(m.input)
		m.input = ""
		return m.execute(command)
	case tea.KeyBackspace, tea.KeyDelete:
		if m.input != "" {
			_, size := utf8.DecodeLastRuneInString(m.input)
			m.input = m.input[:len(m.input)-size]
		}
		return m, nil
	case tea.KeyEscape:
		if m.input != "" {
			m.input = ""
		} else if m.screen == logScreen {
			m.screen = homeScreen
			return m, tea.ClearScreen
		}
		return m, nil
	default:
		// Bubble Tea only populates Text for printable input. Accept it regardless
		// of modifier flags so AltGr layouts can enter characters such as @ and
		// backslash; Ctrl+C is handled explicitly above.
		if key.Text != "" {
			m.input += key.Text
		}
		return m, nil
	}
}

func (m *model) requestQuit() (tea.Model, tea.Cmd) {
	if m.busy {
		m.quitWhenIdle = true
		m.controller.Stop()
		m.notice = "Exit requested; waiting for " + m.busyName + " to finish."
		if m.screen == logScreen {
			m.screen = homeScreen
			return m, tea.ClearScreen
		}
		return m, nil
	}
	m.quitWhenIdle = true
	if m.controller.Stop() {
		m.phase = phaseStopping
		m.notice = "Stopping the node lifecycle cleanly before exit..."
		if m.screen == logScreen {
			m.screen = homeScreen
			return m, tea.ClearScreen
		}
		return m, nil
	}
	return m, tea.Quit
}

func (m *model) execute(command string) (tea.Model, tea.Cmd) {
	if command == "" {
		return m, nil
	}
	parts, err := splitCommandLine(command)
	if err != nil {
		m.notice = "Could not parse command: " + safeInline(err.Error())
		return m, nil
	}
	if len(parts) == 0 {
		return m, nil
	}
	name := strings.ToLower(parts[0])
	switch name {
	case "/log":
		m.screen = logScreen
		m.followLogs = true
		m.logOffset = 0
		return m, nil
	case "/home":
		m.screen = homeScreen
		return m, tea.ClearScreen
	case "/help":
		m.notice = m.showHelp()
	case "/status":
		m.notice = m.statusText()
		if m.phase == phaseRunning && !m.healthBusy {
			m.healthBusy = true
			return m, m.fetchHealth()
		}
	case "/config":
		if len(parts) == 1 {
			m.notice = m.showConfiguration()
			return m, nil
		}
		switch strings.ToLower(parts[1]) {
		case "reload":
			if len(parts) != 2 {
				m.notice = "Use /config reload without arguments to reload the current configuration source."
				return m, nil
			}
			return m.reloadConfiguration()
		case "load":
			if len(parts) != 3 || strings.TrimSpace(parts[2]) == "" {
				m.notice = "Use /config load <path> while stopped; quote paths that contain spaces."
				return m, nil
			}
			return m.loadConfiguration(parts[2])
		default:
			m.notice = "Use /config to inspect settings, /config reload for the current source, or /config load <path> for a YAML file."
			return m, nil
		}
	case "/start":
		if len(parts) != 1 {
			m.notice = "Use /start without arguments."
			return m, nil
		}
		if m.busy {
			m.notice = "Please wait for " + m.busyName + " to finish before starting."
			return m, nil
		}
		if m.controller.Active() {
			m.notice = "The node lifecycle is already active."
			return m, nil
		}
		// The callback can return just before its lifecycleExitedMsg reaches the
		// update loop. Do not replace m.run in that settling window: doing so
		// would make the old exit and health messages stale and could leave
		// health polling permanently marked busy.
		if m.phase != phaseStopped && m.phase != phaseFailed {
			m.notice = "The node lifecycle has returned and is still settling; wait for the stopped status before starting again."
			return m, nil
		}
		run, ok := m.controller.Start(m.nodeCfg)
		if !ok {
			m.notice = "The node lifecycle is still stopping; wait for a clean return before starting again."
			return m, nil
		}
		m.run = run
		m.phase = phaseRunning
		m.health = NodeStatus{}
		m.healthErr = nil
		m.healthBusy = false
		m.notice = "Starting the node lifecycle..."
		m.cfg.Log.Info("lifecycle", "start requested")
		return m, waitLifecycle(run)
	case "/stop":
		if len(parts) != 1 {
			m.notice = "Use /stop without arguments."
			return m, nil
		}
		if !m.controller.Active() {
			m.notice = "The node lifecycle is already stopped."
			return m, nil
		}
		m.controller.Stop()
		m.phase = phaseStopping
		m.notice = "Stopping the node lifecycle cleanly..."
		m.cfg.Log.Info("lifecycle", "stop requested")
	case "/enqueue", "/search", "/deadletters":
		return m.runMaintenance(strings.TrimPrefix(name, "/"), parts[1:])
	case "/theme":
		if len(parts) != 2 {
			m.notice = "Choose /theme auto, /theme dark, or /theme light."
			return m, nil
		}
		mode, parseErr := ParseTheme(parts[1])
		if parseErr != nil {
			m.notice = safeInline(parseErr.Error())
			return m, nil
		}
		m.mode = mode
		dark := m.detectedDark
		if mode == ThemeDark {
			dark = true
		} else if mode == ThemeLight {
			dark = false
		}
		m.palette = newPalette(dark)
		if saveErr := SaveTheme(m.cfg.StatePath, mode); saveErr != nil {
			m.notice = "Theme changed for this session; saving failed: " + safeInline(saveErr.Error())
		} else {
			m.notice = "Terminal appearance set to " + string(mode) + "."
		}
	case "/quit", "/exit":
		return m.requestQuit()
	default:
		m.notice = "Unknown command. Type /help to see the available commands."
	}
	return m, nil
}

func (m *model) reloadConfiguration() (tea.Model, tea.Cmd) {
	return m.requestConfiguration(configReload, m.cfg.ConfigPath)
}

func (m *model) loadConfiguration(path string) (tea.Model, tea.Cmd) {
	return m.requestConfiguration(configLoad, path)
}

func (m *model) requestConfiguration(kind configLoadKind, sourcePath string) (tea.Model, tea.Cmd) {
	operation := kind.operation()
	if m.stoppedOperationBlocked(operation) {
		return m, nil
	}
	if m.cfg.LoadConfig == nil {
		m.notice = kind.title() + " is not available in this build."
		return m, nil
	}
	m.busy = true
	m.busyName = operation
	m.notice = fmt.Sprintf("%s stopped-node configuration from %s...", kind.presentParticiple(), displayConfigPath(sourcePath))
	return m, func() tea.Msg {
		loaded, err := m.cfg.LoadConfig(sourcePath)
		return configReloadedMsg{cfg: loaded, sourcePath: sourcePath, kind: kind, err: err}
	}
}

func (m *model) runMaintenance(command string, arguments []string) (tea.Model, tea.Cmd) {
	operation := "/" + command
	if m.stoppedOperationBlocked(operation) {
		return m, nil
	}
	m.busy = true
	m.busyName = operation
	m.notice = "Running stopped-node " + operation + "..."
	copiedArguments := append([]string(nil), arguments...)
	cfg := m.nodeCfg
	return m, func() tea.Msg {
		lines, err := executeMaintenance(m.ctx, cfg, command, copiedArguments)
		return maintenanceResultMsg{command: command, lines: lines, err: err}
	}
}

func (m *model) stoppedOperationBlocked(operation string) bool {
	if m.controller != nil && m.controller.Active() {
		m.notice = activeMaintenanceMessage(operation)
		return true
	}
	if m.busy {
		m.notice = "Please wait for " + m.busyName + " to finish."
		return true
	}
	return false
}

func activeMaintenanceMessage(operation string) string {
	return operation + " cannot run while the lifecycle is active: stopped-node maintenance requires exclusive data-directory ownership. Use /stop first; online administration arrives with the M8 control plane."
}

// splitCommandLine preserves backslashes in Windows paths while grouping
// whitespace inside single or double quotes. Quotes can start midway through a
// token (for example --class="permanent").
func splitCommandLine(input string) ([]string, error) {
	var arguments []string
	var current strings.Builder
	var quote rune
	started := false
	flush := func() {
		if !started {
			return
		}
		arguments = append(arguments, current.String())
		current.Reset()
		started = false
	}
	for _, character := range input {
		if quote != 0 {
			if character == quote {
				quote = 0
				started = true
			} else {
				current.WriteRune(character)
				started = true
			}
			continue
		}
		switch character {
		case '\'', '"':
			quote = character
			started = true
		case ' ', '\t', '\r', '\n':
			flush()
		default:
			current.WriteRune(character)
			started = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %c quote", quote)
	}
	flush()
	return arguments, nil
}

func (m *model) View() tea.View {
	content := m.renderHome()
	altScreen := false
	promptFromBottom := 2
	if m.screen == logScreen {
		content = m.renderLogs()
		altScreen = true
		promptFromBottom = 1
	}
	view := tea.NewView(content)
	view.AltScreen = altScreen
	view.WindowTitle = "Index Node"
	view.Cursor = m.promptCursor(content, promptFromBottom, altScreen)
	return view
}

func (m *model) promptCursor(content string, promptFromBottom int, altScreen bool) *tea.Cursor {
	lineCount := strings.Count(content, "\n") + 1
	y := lineCount - max(1, promptFromBottom)
	// The inline renderer drops rows from the top when content is taller than
	// the terminal. Cursor coordinates are relative to that visible frame.
	if !altScreen && lineCount > m.height {
		y -= lineCount - m.height
	}
	y = min(max(0, y), max(0, m.height-1))
	x := min(lipgloss.Width(m.renderPrompt()), max(0, m.width-1))
	cursor := tea.NewCursor(x, y)
	cursor.Shape = tea.CursorBar
	return cursor
}

func (m *model) contentWidth() int { return max(1, m.width-1) }

func (m *model) renderHome() string {
	p := m.palette
	// Keep one untouched terminal column beyond the panel. Drawing into the
	// final column can put Windows Terminal into pending-wrap state and break
	// Bubble Tea's physical cursor accounting.
	panelWidth := min(118, max(34, m.width-2))
	wide := panelWidth >= 88
	leftWidth := panelWidth - 2
	if wide {
		leftWidth = 38
	}

	art := strings.Split(renderFrameCrab(p, !wide), "\n")
	artWidth := 0
	for _, line := range art {
		artWidth = max(artWidth, lipgloss.Width(line))
	}
	leftLines := []string{
		centerCell(p.textStyle().Bold(true).Render("Welcome to Index Node"), leftWidth),
		"",
	}
	for _, line := range art {
		leftLines = append(leftLines, centerCell(fitCell(line, artWidth), leftWidth))
	}
	leftLines = append(leftLines,
		"",
		centerCell(p.secondaryStyle().Render("Frame Crab keeps your index local"), leftWidth),
		centerCell(p.mutedStyle().Render("terminal theme · "+string(m.mode)), leftWidth),
	)

	statusLines := m.homeStatusLines()
	actionLines := m.homeActionLines()
	panel := renderStackedPanel(p, panelWidth, m.cfg.Version, leftLines, statusLines, actionLines)
	if wide {
		panel = renderWidePanel(p, panelWidth, m.cfg.Version, leftLines, statusLines, actionLines)
	}

	parts := []string{panel}
	if m.notice != "" {
		parts = append(parts, "", p.secondaryStyle().Render(boundedText(m.notice, m.contentWidth(), 3)))
	}
	if suggestions := m.commandSuggestions(); suggestions != "" {
		parts = append(parts, "", suggestions)
	}
	footer := p.mutedStyle().Render("/log logs   /status health   /config settings   /help commands   Ctrl+C quit")
	parts = append(parts, "", m.renderPrompt(), ansi.Truncate(footer, m.contentWidth(), "…"))
	return strings.Join(parts, "\n")
}

func (m *model) homeStatusLines() []string {
	p := m.palette
	lines := []string{p.accentStyle().Bold(true).Render("STATUS")}
	switch m.phase {
	case phaseStopping:
		spinners := []string{"◒", "◐", "◓", "◑"}
		return append(lines, p.warningStyle().Render(spinners[m.spinner])+" Stopping lifecycle cleanly")
	case phaseStopped:
		return append(lines, p.mutedStyle().Render("■")+" Node lifecycle stopped")
	case phaseFailed:
		return append(lines, p.dangerStyle().Render("■")+" Node lifecycle failed", p.mutedStyle().Render("Use /log for details."))
	}

	if m.health.Status == "" {
		spinners := []string{"◒", "◐", "◓", "◑"}
		lines = append(lines, p.accentStyle().Render(spinners[m.spinner])+" Node lifecycle active")
		if m.healthErr != nil {
			lines = append(lines, p.mutedStyle().Render("Health endpoint is not reachable yet."))
		} else {
			lines = append(lines, p.mutedStyle().Render("Checking /healthz..."))
		}
		return lines
	}
	marker := p.successStyle().Render("■")
	if m.health.Status == "warming" {
		marker = p.warningStyle().Render("■")
	} else if m.health.Status != "ready" {
		marker = p.dangerStyle().Render("■")
	}
	lines = append(lines,
		marker+" Health "+safeInline(m.health.Status),
		p.secondaryStyle().Render(fmt.Sprintf("Roots %d · active %d · pending %d", m.health.Roots, m.health.ActiveRoots, m.health.PendingRoots)),
	)
	if m.health.DegradedRoots != 0 || m.health.DirtyRoots != 0 {
		lines = append(lines, p.warningStyle().Render(fmt.Sprintf("Degraded %d · dirty %d", m.health.DegradedRoots, m.health.DirtyRoots)))
	}
	return lines
}

func (m *model) homeActionLines() []string {
	p := m.palette
	lines := []string{p.accentStyle().Bold(true).Render("CONTROL")}
	if m.phase == phaseRunning || m.phase == phaseStopping {
		return append(lines,
			p.secondaryStyle().Render("/status refresh aggregate health"),
			p.secondaryStyle().Render("/stop shut down the lifecycle cleanly"),
			"",
			p.mutedStyle().Render("Stopped-node maintenance stays offline until M8."),
		)
	}
	return append(lines,
		p.secondaryStyle().Render("/start run the M0-M5 node lifecycle"),
		p.secondaryStyle().Render("/enqueue  /search  /deadletters"),
		p.secondaryStyle().Render("/config reload"),
		p.secondaryStyle().Render("/config load <path>"),
		"",
		p.mutedStyle().Render("Maintenance is safe here because the node is stopped."),
	)
}

func renderWidePanel(p palette, width int, version string, left, status, actions []string) string {
	const ruleMarker = "\x00rule"
	leftWidth := 38
	rightWidth := width - leftWidth - 3
	right := append(append(append([]string{}, status...), ruleMarker), actions...)
	rowCount := max(len(left), len(right))
	border := p.accentStyle()
	rows := []string{panelHeader(p, width, version)}

	for index := 0; index < rowCount; index++ {
		leftCell := ""
		if index < len(left) {
			leftCell = left[index]
		}
		if index < len(right) && right[index] == ruleMarker {
			rows = append(rows,
				border.Render("│")+fitCell(leftCell, leftWidth)+
					border.Render("├"+strings.Repeat("─", rightWidth)+"┤"),
			)
			continue
		}
		rightCell := ""
		if index < len(right) {
			rightCell = right[index]
		}
		rows = append(rows,
			border.Render("│")+fitCell(leftCell, leftWidth)+
				border.Render("│")+fitCell(rightCell, rightWidth)+border.Render("│"),
		)
	}
	rows = append(rows, border.Render("╰"+strings.Repeat("─", leftWidth)+"┴"+strings.Repeat("─", rightWidth)+"╯"))
	return strings.Join(rows, "\n")
}

func renderStackedPanel(p palette, width int, version string, sections ...[]string) string {
	innerWidth := width - 2
	border := p.accentStyle()
	rows := []string{panelHeader(p, width, version)}
	for sectionIndex, section := range sections {
		if sectionIndex > 0 {
			rows = append(rows, border.Render("├"+strings.Repeat("─", innerWidth)+"┤"))
		}
		for _, line := range section {
			rows = append(rows, border.Render("│")+fitCell(line, innerWidth)+border.Render("│"))
		}
	}
	rows = append(rows, border.Render("╰"+strings.Repeat("─", innerWidth)+"╯"))
	return strings.Join(rows, "\n")
}

func panelHeader(p palette, width int, version string) string {
	border := p.accentStyle()
	label := p.accentStyle().Bold(true).Render("INDEX NODE") + p.mutedStyle().Render(" "+safeInline(version))
	ruleWidth := max(1, width-lipgloss.Width(label)-5)
	return border.Render("╭─ ") + label + " " + border.Render(strings.Repeat("─", ruleWidth)+"╮")
}

func fitCell(value string, width int) string {
	value = ansi.Truncate(value, max(1, width), "…")
	return value + strings.Repeat(" ", max(0, width-lipgloss.Width(value)))
}

func centerCell(value string, width int) string {
	value = ansi.Truncate(value, max(1, width), "…")
	padding := max(0, width-lipgloss.Width(value))
	left := padding / 2
	return strings.Repeat(" ", left) + value + strings.Repeat(" ", padding-left)
}

func (m *model) renderLogs() string {
	p := m.palette
	filterNames := []string{"ALL", "INFO", "WARN", "ERROR"}
	follow := "paused"
	if m.followLogs {
		follow = "following"
	}
	header := p.accentStyle().Bold(true).Render("INDEX NODE / LOGS") +
		p.mutedStyle().Render(fmt.Sprintf("   %s · %s", filterNames[m.levelFilter], follow))
	help := p.mutedStyle().Render("↑↓ scroll  PgUp/PgDn page  End latest  f follow  1–4 filter  Esc home")
	header = ansi.Truncate(header, m.contentWidth(), "…")
	help = ansi.Truncate(help, m.contentWidth(), "…")
	divider := p.mutedStyle().Render(strings.Repeat("─", m.contentWidth()))

	entries := m.filteredEntries()
	bodyHeight := m.logBodyHeight()
	maxOffset := max(0, len(entries)-bodyHeight)
	m.logOffset = min(m.logOffset, maxOffset)
	start := max(0, len(entries)-bodyHeight-m.logOffset)
	end := min(len(entries), start+bodyHeight)

	body := make([]string, 0, bodyHeight)
	for _, entry := range entries[start:end] {
		body = append(body, m.renderLogEntry(entry))
	}
	for len(body) < bodyHeight {
		body = append(body, "")
	}

	return strings.Join([]string{
		header,
		help,
		divider,
		strings.Join(body, "\n"),
		m.renderPrompt(),
	}, "\n")
}

func (m *model) renderLogEntry(entry LogEntry) string {
	p := m.palette
	levelStyle := p.successStyle()
	if entry.Level == LogLevelWarn {
		levelStyle = p.warningStyle()
	} else if entry.Level == LogLevelError {
		levelStyle = p.dangerStyle()
	}
	line := p.mutedStyle().Render(entry.Time.Format("15:04:05")) + " " +
		levelStyle.Bold(true).Render(fmt.Sprintf("%-5s", entry.Level.String())) + " " +
		p.secondaryStyle().Render(fmt.Sprintf("%-9s", safeInline(entry.Scope))) + " " +
		p.textStyle().Render(safeInline(entry.Message))
	return ansi.Truncate(line, m.contentWidth(), "…")
}

func (m *model) filteredEntries() []LogEntry {
	entries := m.cfg.Log.Snapshot()
	if m.levelFilter == 0 {
		return entries
	}
	level := LogLevelInfo
	if m.levelFilter == 2 {
		level = LogLevelWarn
	} else if m.levelFilter == 3 {
		level = LogLevelError
	}
	filtered := make([]LogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Level == level {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (m *model) logBodyHeight() int {
	return max(3, m.height-5)
}

func (m *model) renderPrompt() string {
	return m.palette.accentStyle().Bold(true).Render("›") + " " +
		m.palette.textStyle().Render(m.visibleInput())
}

func (m *model) visibleInput() string {
	value := safeInline(m.input)
	available := max(0, m.contentWidth()-lipgloss.Width("› "))
	width := lipgloss.Width(value)
	if width <= available {
		return value
	}
	if available <= 1 {
		return ansi.Truncate(value, available, "")
	}
	// Preserve the text nearest the cursor. TruncateLeft's n is the number of
	// cells removed, so include the ellipsis cell in that calculation.
	return ansi.TruncateLeft(value, width-available+1, "…")
}

func (m *model) commandSuggestions() string {
	if !strings.HasPrefix(m.input, "/") || strings.Contains(m.input, " ") {
		return ""
	}
	commands := []string{"/config", "/deadletters", "/enqueue", "/exit", "/help", "/home", "/log", "/quit", "/search", "/start", "/status", "/stop", "/theme"}
	matches := make([]string, 0, len(commands))
	for _, command := range commands {
		if strings.HasPrefix(command, strings.ToLower(m.input)) {
			matches = append(matches, command)
		}
	}
	sort.Strings(matches)
	if len(matches) == 0 {
		return ""
	}
	return ansi.Truncate(
		m.palette.mutedStyle().Render(strings.Join(matches, "   ")),
		m.contentWidth(),
		"…",
	)
}

func (m *model) statusText() string {
	switch m.phase {
	case phaseStopping:
		return "Node lifecycle is stopping cleanly."
	case phaseStopped:
		return "Node lifecycle is stopped. Stopped-node maintenance is available."
	case phaseFailed:
		return "Node lifecycle is stopped after an error. Use /log for details."
	}
	if m.health.Status == "" {
		if m.healthErr != nil {
			return "Node lifecycle is active; /healthz is not reachable yet: " + safeInline(m.healthErr.Error())
		}
		return "Node lifecycle is active; waiting for the first /healthz response."
	}
	return fmt.Sprintf(
		"Health %s · roots %d · active %d · pending %d · degraded %d · dirty %d",
		safeInline(m.health.Status), m.health.Roots, m.health.ActiveRoots, m.health.PendingRoots,
		m.health.DegradedRoots, m.health.DirtyRoots,
	)
}

func (m *model) configSummary() string {
	cfg := m.nodeCfg
	if cfg == nil {
		return "No resolved node configuration is loaded."
	}
	lines := []string{
		"RESOLVED CONFIGURATION · ACTIVE IN M0-M5",
		"source: " + displayConfigPath(m.cfg.ConfigPath),
		"node_id: " + safeInline(cfg.NodeID),
		"data_dir: " + safeInline(cfg.DataDir),
		"metrics_listen: " + safeInline(cfg.MetricsListen),
		fmt.Sprintf("watch.roots: %d", len(cfg.Watch.Roots)),
	}
	for index, root := range cfg.Watch.Roots {
		lines = append(lines, fmt.Sprintf("  [%d] %s (recursive=%t)", index, safeInline(root.Path), root.Recursive))
	}
	lines = append(lines,
		fmt.Sprintf("watch.buffer_size: %d", cfg.Watch.BufferSize),
		"watch.settle_window: "+cfg.Watch.SettleWindow.String(),
		fmt.Sprintf("pipeline.io_concurrency: %d", cfg.Pipeline.IOConcurrency),
		fmt.Sprintf("pipeline.io_bytes_inflight: %d", cfg.Pipeline.IOBytesInflight),
		fmt.Sprintf("pipeline.cpu_percent_cap: %d", cfg.Pipeline.CPUPercentCap),
		fmt.Sprintf("pipeline.max_file_size: %d", cfg.Pipeline.MaxFileSize),
		fmt.Sprintf("pipeline.max_extract_bytes: %d", cfg.Pipeline.MaxExtractBytes),
		fmt.Sprintf("pipeline.image_size: %d", cfg.Pipeline.ImageSize),
		fmt.Sprintf("pipeline.image_jpeg_quality: %d", cfg.Pipeline.ImageJPEGQuality),
		fmt.Sprintf("pipeline.image_max_pixels: %d", cfg.Pipeline.ImageMaxPixels),
		fmt.Sprintf("pipeline.image_bytes_inflight: %d", cfg.Pipeline.ImageBytesInflight),
		"compute.endpoint: "+safeInline(cfg.Compute.Endpoint),
		"compute.request_timeout: "+cfg.Compute.RequestTimeout.String(),
		fmt.Sprintf("compute.batch_size: %d", cfg.Compute.BatchSize),
		"compute.batch_linger: "+cfg.Compute.BatchLinger.String(),
		fmt.Sprintf("compute.inflight_batches: %d", cfg.Compute.InflightBatches),
		fmt.Sprintf("compute.breaker.failures: %d", cfg.Compute.Breaker.Failures),
		"compute.breaker.open_for: "+cfg.Compute.Breaker.OpenFor.String(),
		fmt.Sprintf("index.commit_max_ops: %d", cfg.Index.CommitMaxOps),
		"index.commit_interval: "+cfg.Index.CommitInterval.String(),
		fmt.Sprintf("index.vector.m: %d", cfg.Index.Vector.M),
		fmt.Sprintf("index.vector.ef_construction: %d", cfg.Index.Vector.EFConstruction),
		fmt.Sprintf("index.vector.ef_search: %d", cfg.Index.Vector.EFSearch),
		"index.vector.snapshot_interval: "+cfg.Index.Vector.SnapshotInterval.String(),
		fmt.Sprintf("index.vector.snapshot_changes: %d", cfg.Index.Vector.SnapshotChanges),
		"retry.base: "+cfg.Retry.Base.String(),
		"retry.cap: "+cfg.Retry.Cap.String(),
		fmt.Sprintf("retry.max_attempts_transient: %d", cfg.Retry.MaxAttemptsTransient),
		fmt.Sprintf("retry.retry_budget_ratio: %g", cfg.Retry.RetryBudgetRatio),
		fmt.Sprintf("dead_letter.retention_days: %d", cfg.DeadLetter.RetentionDays),
		"reconcile.periodic: "+cfg.Reconcile.Periodic.String(),
		"log.level: "+safeInline(cfg.Log.Level),
		fmt.Sprintf("log.redact_paths: %t", cfg.Log.RedactPaths),
		fmt.Sprintf("log.retain_days: %d", cfg.Log.RetainDays),
		"Later milestones (not active here): video extraction, notes, grpc control plane, and complete resource admission.",
	)
	return strings.Join(lines, "\n")
}

func (m *model) showConfiguration() string {
	summary := m.configSummary()
	lines := strings.Split(summary, "\n")
	for _, line := range lines {
		m.cfg.Log.Info("config", "%s", line)
	}
	if m.nodeCfg == nil {
		return "No resolved node configuration is loaded."
	}
	return fmt.Sprintf(
		"Resolved M0-M5 config · node %s · data_dir %s · roots %d\n%d configuration lines were written to /log.",
		safeInline(m.nodeCfg.NodeID), safeInline(m.nodeCfg.DataDir), len(m.nodeCfg.Watch.Roots), len(lines),
	)
}

func (m *model) showHelp() string {
	lines := []string{
		"/start - start the stopped node lifecycle",
		"/stop - stop the lifecycle cleanly",
		"/status - refresh aggregate node health",
		"/config - show resolved M0-M5 configuration",
		"/config reload - reload the current source while stopped",
		"/config load <path> - load a YAML file while stopped",
		"/log - open logs; Esc returns home",
		"/home - return to the dashboard",
		"/theme auto|dark|light - set terminal appearance",
		"/enqueue <path>... - enqueue while stopped",
		searchMaintenanceUsage + " - stopped-node hybrid search",
		"/deadletters list [-class C] [-limit N]",
		"/deadletters redrive -file-ids 1,2",
		"/deadletters redrive -class poison",
		"/quit or /exit - stop cleanly and quit",
		"Ctrl+C - stop cleanly and quit",
		"Live administration arrives with the M8 control plane.",
	}
	for _, line := range lines {
		m.cfg.Log.Info("help", "%s", line)
	}
	return fmt.Sprintf(
		"Command reference (%d lines) written to /log.\nControl: /start /stop /status /config /log /home /quit\nStopped node: /enqueue /search /deadletters · appearance: /theme",
		len(lines),
	)
}

func displayConfigPath(path string) string {
	if strings.TrimSpace(path) == "" {
		return "defaults + environment"
	}
	return safeInline(path)
}

// safeInline strips ANSI/OSC sequences and makes every remaining terminal
// control visible. Printable Unicode is preserved verbatim.
func safeInline(value string) string {
	value = ansi.Strip(value)
	var output strings.Builder
	for _, character := range value {
		switch character {
		case '\n':
			output.WriteString("\\n")
		case '\r':
			output.WriteString("\\r")
		case '\t':
			output.WriteString("\\t")
		default:
			if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
				fmt.Fprintf(&output, "\\u%04X", character)
			} else {
				output.WriteRune(character)
			}
		}
	}
	return output.String()
}

func safeMultiline(value string) string {
	lines := strings.Split(value, "\n")
	for index := range lines {
		lines[index] = safeInline(lines[index])
	}
	return strings.Join(lines, "\n")
}

func boundedText(value string, width, maxLines int) string {
	lines := strings.Split(safeMultiline(value), "\n")
	if maxLines < 1 {
		maxLines = 1
	}
	if len(lines) > maxLines {
		lines = append(lines[:maxLines-1], "…")
	}
	for index := range lines {
		lines[index] = ansi.Truncate(lines[index], max(1, width), "…")
	}
	return strings.Join(lines, "\n")
}

type lifecycleRun struct {
	done chan struct{}
	mu   sync.RWMutex
	err  error
}

func (run *lifecycleRun) finish(err error) {
	run.mu.Lock()
	run.err = err
	run.mu.Unlock()
	close(run.done)
}

func (run *lifecycleRun) wait(ctx context.Context) error {
	if run == nil {
		return nil
	}
	select {
	case <-run.done:
		run.mu.RLock()
		defer run.mu.RUnlock()
		return run.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type lifecycleController struct {
	mu      sync.Mutex
	parent  context.Context
	run     func(context.Context, *config.Config, io.Writer) error
	log     *LogHub
	active  bool
	cancel  context.CancelFunc
	current *lifecycleRun
}

func newLifecycleController(parent context.Context, run func(context.Context, *config.Config, io.Writer) error, log *LogHub) *lifecycleController {
	return &lifecycleController{parent: parent, run: run, log: log}
}

func (controller *lifecycleController) Start(cfg *config.Config) (*lifecycleRun, bool) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.active || cfg == nil || controller.run == nil {
		return controller.current, false
	}
	ctx, cancel := context.WithCancel(controller.parent)
	handle := &lifecycleRun{done: make(chan struct{})}
	controller.active = true
	controller.cancel = cancel
	controller.current = handle
	go func() {
		var runErr error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					runErr = fmt.Errorf("lifecycle callback panicked: %v", recovered)
				}
			}()
			runErr = controller.run(ctx, cfg, controller.log)
		}()
		controller.mu.Lock()
		if controller.current == handle {
			controller.active = false
			controller.cancel = nil
		}
		controller.mu.Unlock()
		handle.finish(runErr)
	}()
	return handle, true
}

func (controller *lifecycleController) Stop() bool {
	controller.mu.Lock()
	cancel := controller.cancel
	active := controller.active
	controller.mu.Unlock()
	if active && cancel != nil {
		cancel()
	}
	return active
}

func (controller *lifecycleController) Active() bool {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.active
}

func (controller *lifecycleController) Current() *lifecycleRun {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.current
}
