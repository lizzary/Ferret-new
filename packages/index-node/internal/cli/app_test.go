package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/lizzary/index-node/internal/config"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

type lifecycleTestInvocation struct {
	generation int
	ctx        context.Context
	cfg        *config.Config
}

func TestLifecycleControllerStopsAndJoinsCurrentGeneration(t *testing.T) {
	releases := []chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}
	calls := make(chan lifecycleTestInvocation, len(releases))
	var generation atomic.Int32
	secondErr := errors.New("second generation failed")
	runErrors := []error{nil, secondErr}

	controller := newLifecycleController(
		context.Background(),
		func(ctx context.Context, cfg *config.Config, _ io.Writer) error {
			index := int(generation.Add(1)) - 1
			calls <- lifecycleTestInvocation{generation: index, ctx: ctx, cfg: cfg}
			<-ctx.Done()
			<-releases[index]
			return runErrors[index]
		},
		NewLogHub(8),
	)
	t.Cleanup(func() {
		controller.Stop()
		for _, release := range releases {
			select {
			case release <- struct{}{}:
			default:
			}
		}
	})

	firstCfg := &config.Config{NodeID: "first"}
	first, started := controller.Start(firstCfg)
	if !started || first == nil {
		t.Fatal("first lifecycle generation did not start")
	}
	firstCall := receiveInvocation(t, calls)
	if firstCall.generation != 0 || firstCall.cfg != firstCfg {
		t.Fatalf("first invocation = %#v, want generation 0 and the first config", firstCall)
	}
	if controller.Current() != first || !controller.Active() {
		t.Fatal("controller did not expose the active first generation")
	}
	if !controller.Stop() {
		t.Fatal("Stop reported an inactive first generation")
	}
	select {
	case <-firstCall.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the first lifecycle context")
	}

	secondCfg := &config.Config{NodeID: "second"}
	if current, ok := controller.Start(secondCfg); ok || current != first {
		t.Fatalf("Start while first generation is returning = (%p, %t), want (%p, false)", current, ok, first)
	}
	releases[0] <- struct{}{}
	if err := waitRun(t, first); err != nil {
		t.Fatalf("first generation wait: %v", err)
	}
	if controller.Active() {
		t.Fatal("controller remained active after joining the first generation")
	}

	second, started := controller.Start(secondCfg)
	if !started || second == nil || second == first {
		t.Fatalf("second lifecycle generation = (%p, %t), first was %p", second, started, first)
	}
	secondCall := receiveInvocation(t, calls)
	if secondCall.generation != 1 || secondCall.cfg != secondCfg {
		t.Fatalf("second invocation = %#v, want generation 1 and the second config", secondCall)
	}
	if controller.Current() != second {
		t.Fatal("Current did not advance to the second lifecycle generation")
	}
	if !controller.Stop() {
		t.Fatal("Stop reported an inactive second generation")
	}
	releases[1] <- struct{}{}
	if err := waitRun(t, controller.Current()); !errors.Is(err, secondErr) {
		t.Fatalf("current generation wait = %v, want %v", err, secondErr)
	}
	if err := waitRun(t, first); err != nil {
		t.Fatalf("waiting the prior generation changed its result: %v", err)
	}
}

func TestModelRequestQuitWaitsForActiveLifecycle(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{}, 1)
	controller := newLifecycleController(
		context.Background(),
		func(ctx context.Context, _ *config.Config, _ io.Writer) error {
			started <- ctx
			<-ctx.Done()
			<-release
			return nil
		},
		NewLogHub(8),
	)
	run, ok := controller.Start(&config.Config{NodeID: "active"})
	if !ok {
		t.Fatal("lifecycle did not start")
	}
	ctx := receiveContext(t, started)
	t.Cleanup(func() {
		controller.Stop()
		select {
		case release <- struct{}{}:
		default:
		}
	})

	m := &model{
		cfg:        Config{Log: NewLogHub(8)},
		controller: controller,
		run:        run,
		phase:      phaseRunning,
	}
	_, command := m.requestQuit()
	if command != nil {
		if _, quit := command().(tea.QuitMsg); quit {
			t.Fatal("requestQuit emitted tea.Quit before the active lifecycle returned")
		}
		t.Fatalf("requestQuit returned unexpected command result %T", command())
	}
	if !m.quitWhenIdle || m.phase != phaseStopping {
		t.Fatalf("quit state = (whenIdle=%t, phase=%d), want (true, stopping)", m.quitWhenIdle, m.phase)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("requestQuit did not cancel the active lifecycle")
	}

	release <- struct{}{}
	err := waitRun(t, run)
	_, command = m.Update(lifecycleExitedMsg{run: run, err: err})
	assertQuitCommand(t, command)
	if m.phase != phaseStopped {
		t.Fatalf("phase after lifecycle exit = %d, want stopped", m.phase)
	}
}

func TestModelRequestQuitImmediatelyWhenLifecycleIsInactive(t *testing.T) {
	controller := newLifecycleController(context.Background(), nil, NewLogHub(2))
	m := &model{cfg: Config{Log: NewLogHub(2)}, controller: controller, phase: phaseStopped}

	_, command := m.requestQuit()
	assertQuitCommand(t, command)
	if !m.quitWhenIdle {
		t.Fatal("requestQuit did not remember the exit request")
	}
}

func TestFatalLifecycleFailureForcesProcessBoundaryExit(t *testing.T) {
	fatal := errors.New("components remain live")
	run := &lifecycleRun{done: make(chan struct{})}
	m := &model{
		cfg: Config{
			Log: NewLogHub(8),
			FatalLifecycleError: func(err error) bool {
				return errors.Is(err, fatal)
			},
		},
		run:   run,
		phase: phaseStopping,
	}

	_, command := m.Update(lifecycleExitedMsg{run: run, err: fmt.Errorf("shutdown deadline: %w", fatal)})
	assertQuitCommand(t, command)
	if m.phase != phaseFailed {
		t.Fatalf("fatal phase = %d, want failed", m.phase)
	}
	if !strings.Contains(m.notice, "process must exit") {
		t.Fatalf("fatal notice = %q", m.notice)
	}
	if strings.Contains(strings.ToLower(m.notice), "maintenance") || strings.Contains(strings.ToLower(m.notice), "stopped-node") {
		t.Fatalf("fatal notice incorrectly offered stopped-node work: %q", m.notice)
	}
}

func TestModelAcceptsBracketedPasteAndShiftedText(t *testing.T) {
	m := &model{}
	_, command := m.Update(tea.PasteMsg{Content: `/enqueue "C:\Photo Library\a.jpg"`})
	if command != nil {
		t.Fatalf("paste command = %v, want nil", command)
	}
	if got, want := m.input, `/enqueue "C:\Photo Library\a.jpg"`; got != want {
		t.Fatalf("input after paste = %q, want %q", got, want)
	}

	_, command = m.handleKey(tea.KeyPressMsg(tea.Key{Text: "!", Code: '1', Mod: tea.ModShift}))
	if command != nil || !strings.HasSuffix(m.input, "!") {
		t.Fatalf("shifted text = (input=%q, command=%v), want appended text", m.input, command != nil)
	}
	_, _ = m.handleKey(tea.KeyPressMsg(tea.Key{Text: "@", Code: 'q', Mod: tea.ModCtrl | tea.ModAlt}))
	if !strings.HasSuffix(m.input, "!@") {
		t.Fatalf("AltGr text was not appended: input=%q", m.input)
	}
}

func TestModelRequestQuitWhileBusyWaitsThenQuits(t *testing.T) {
	controller := newLifecycleController(context.Background(), nil, NewLogHub(2))
	m := &model{
		cfg:        Config{Log: NewLogHub(2)},
		controller: controller,
		phase:      phaseStopped,
		busy:       true,
		busyName:   "/search",
	}

	_, command := m.requestQuit()
	if command != nil {
		t.Fatalf("busy quit command = %v, want deferred exit", command)
	}
	if !m.quitWhenIdle || !strings.Contains(m.notice, "waiting for /search") {
		t.Fatalf("busy quit state = (whenIdle=%t, notice=%q)", m.quitWhenIdle, m.notice)
	}

	_, command = m.Update(maintenanceResultMsg{command: "search", lines: []string{"done"}})
	assertQuitCommand(t, command)
}

func TestStartWaitsForExitMessageAndResetsHealthPolling(t *testing.T) {
	started := make(chan struct{}, 1)
	controller := newLifecycleController(
		context.Background(),
		func(ctx context.Context, _ *config.Config, _ io.Writer) error {
			started <- struct{}{}
			<-ctx.Done()
			return nil
		},
		NewLogHub(8),
	)
	oldRun := &lifecycleRun{done: make(chan struct{})}
	m := &model{
		cfg:        Config{Log: NewLogHub(8)},
		controller: controller,
		run:        oldRun,
		nodeCfg:    &config.Config{NodeID: "next"},
		phase:      phaseRunning,
		healthBusy: true,
	}

	_, command := m.execute("/start")
	if command != nil || m.run != oldRun || controller.Active() {
		t.Fatalf("start during settling = (cmd=%v, runChanged=%t, active=%t), want guarded", command != nil, m.run != oldRun, controller.Active())
	}
	if !strings.Contains(m.notice, "still settling") {
		t.Fatalf("settling notice = %q", m.notice)
	}

	m.phase = phaseStopped
	_, command = m.execute("/start")
	if command == nil || m.run == oldRun || !controller.Active() {
		t.Fatalf("start after stopped = (cmd=%v, runChanged=%t, active=%t), want started", command != nil, m.run != oldRun, controller.Active())
	}
	if m.healthBusy {
		t.Fatal("start did not reset stale healthBusy state")
	}
	receiveSignal(t, started)
	controller.Stop()
	if err := waitRun(t, m.run); err != nil {
		t.Fatalf("stop restarted lifecycle: %v", err)
	}
}

func TestHelpWritesCompleteCommandReferenceToLog(t *testing.T) {
	hub := NewLogHub(64)
	m := &model{cfg: Config{Log: hub}}
	_, command := m.execute("/help")
	if command != nil {
		t.Fatalf("help command = %v, want nil", command)
	}
	if !strings.Contains(m.notice, "written to /log") {
		t.Fatalf("help notice = %q", m.notice)
	}
	var messages []string
	for _, entry := range hub.Snapshot() {
		messages = append(messages, entry.Message)
	}
	all := strings.Join(messages, "\n")
	for _, want := range []string{
		"/start", "/config reload", "/config load <path>", "/theme auto|dark|light",
		"/enqueue <path>...", "/search [-mode hybrid|keyword|semantic]",
		"-path-prefix P", "-kind K[,K]", "-mtime-from-ns N", "-mtime-to-ns N",
		"/deadletters list [-class C] [-limit N]",
		"/deadletters redrive -file-ids 1,2", "/quit or /exit",
	} {
		if !strings.Contains(all, want) {
			t.Fatalf("help log is missing %q:\n%s", want, all)
		}
	}
}

func TestActiveLifecycleGuardsOfflineMaintenanceAndConfigReload(t *testing.T) {
	started := make(chan struct{}, 1)
	controller := newLifecycleController(
		context.Background(),
		func(ctx context.Context, _ *config.Config, _ io.Writer) error {
			started <- struct{}{}
			<-ctx.Done()
			return nil
		},
		NewLogHub(8),
	)
	run, ok := controller.Start(&config.Config{NodeID: "active"})
	if !ok {
		t.Fatal("lifecycle did not start")
	}
	receiveSignal(t, started)
	t.Cleanup(func() {
		controller.Stop()
		_ = waitRun(t, run)
	})

	var loadCalls atomic.Int32
	m := &model{
		cfg: Config{
			Log: NewLogHub(8),
			LoadConfig: func(string) (*config.Config, error) {
				loadCalls.Add(1)
				return &config.Config{}, nil
			},
		},
		controller: controller,
		run:        run,
		nodeCfg:    &config.Config{},
		phase:      phaseRunning,
	}

	_, command := m.runMaintenance("search", []string{"--help"})
	if command != nil || m.busy {
		t.Fatalf("active maintenance = (cmd=%v, busy=%t), want guarded", command != nil, m.busy)
	}
	if !strings.Contains(m.notice, "cannot run while the lifecycle is active") || !strings.Contains(m.notice, "Use /stop first") {
		t.Fatalf("maintenance guard notice = %q", m.notice)
	}

	_, command = m.reloadConfiguration()
	if command != nil || m.busy || loadCalls.Load() != 0 {
		t.Fatalf("active config reload = (cmd=%v, busy=%t, calls=%d), want guarded", command != nil, m.busy, loadCalls.Load())
	}
	if !strings.Contains(m.notice, "configuration reload cannot run while the lifecycle is active") {
		t.Fatalf("config guard notice = %q", m.notice)
	}

	_, command = m.loadConfiguration(`C:\Config Files\next.yaml`)
	if command != nil || m.busy || loadCalls.Load() != 0 {
		t.Fatalf("active config load = (cmd=%v, busy=%t, calls=%d), want guarded", command != nil, m.busy, loadCalls.Load())
	}
	if !strings.Contains(m.notice, "configuration load cannot run while the lifecycle is active") {
		t.Fatalf("config load guard notice = %q", m.notice)
	}
}

func TestMaintenanceUsesTypedInPackageExecutorWithoutCallback(t *testing.T) {
	m := &model{
		cfg:        Config{Log: NewLogHub(8)},
		ctx:        context.Background(),
		controller: newLifecycleController(context.Background(), nil, NewLogHub(2)),
		phase:      phaseStopped,
	}

	_, command := m.execute("/search --help")
	if command == nil || !m.busy || m.busyName != "/search" {
		t.Fatalf("typed maintenance start = (cmd=%v, busy=%t, name=%q)", command != nil, m.busy, m.busyName)
	}
	message, ok := command().(maintenanceResultMsg)
	if !ok {
		t.Fatalf("maintenance command result = %T, want maintenanceResultMsg", command())
	}
	if message.err != nil || len(message.lines) != 2 || !strings.Contains(message.lines[0], "/search [-mode hybrid|keyword|semantic]") {
		t.Fatalf("maintenance help result = (lines=%#v, err=%v)", message.lines, message.err)
	}
	_, followUp := m.Update(message)
	if followUp != nil || m.busy {
		t.Fatalf("maintenance completion = (cmd=%v, busy=%t), want idle", followUp != nil, m.busy)
	}
}

func TestMaintenanceErrorPreservesProducedResultsForOperatorReview(t *testing.T) {
	hub := NewLogHub(8)
	m := &model{cfg: Config{Log: hub}, busy: true, busyName: "/enqueue"}
	resultErr := errors.New("finalization failed")

	_, command := m.Update(maintenanceResultMsg{
		command: "enqueue",
		lines:   []string{"Enqueued 1 path(s).", "task=7 generation=1 inserted C:\\document.txt"},
		err:     resultErr,
	})
	if command != nil || m.busy {
		t.Fatalf("partial result update = (cmd=%v, busy=%t), want idle", command != nil, m.busy)
	}
	if !strings.Contains(m.notice, "after producing results") ||
		!strings.Contains(m.notice, "verify them before retrying") ||
		!strings.Contains(m.notice, "Enqueued 1 path(s).") {
		t.Fatalf("partial result notice = %q", m.notice)
	}
	entries := hub.Snapshot()
	joined := make([]string, 0, len(entries))
	for _, entry := range entries {
		joined = append(joined, entry.Message)
	}
	logText := strings.Join(joined, "\n")
	if !strings.Contains(logText, "task=7 generation=1") || !strings.Contains(logText, resultErr.Error()) {
		t.Fatalf("partial result log = %q", logText)
	}
}

func TestConfigLoadUpdatesSourceOnlyAfterSuccess(t *testing.T) {
	oldCfg := config.Default()
	oldCfg.NodeID = "old-node"
	newCfg := oldCfg
	newCfg.NodeID = "new-node"
	oldPath := `C:\Config\current.yaml`
	newPath := `C:\Config Files\next.yaml`
	var loadedPath string
	m := &model{
		cfg: Config{
			ConfigPath: oldPath,
			NodeConfig: &oldCfg,
			Log:        NewLogHub(64),
			LoadConfig: func(path string) (*config.Config, error) {
				loadedPath = path
				return &newCfg, nil
			},
		},
		ctx:        context.Background(),
		controller: newLifecycleController(context.Background(), nil, NewLogHub(2)),
		nodeCfg:    &oldCfg,
		phase:      phaseStopped,
	}

	_, command := m.execute(`/config load "C:\Config Files\next.yaml"`)
	if command == nil || !m.busy {
		t.Fatalf("config load start = (cmd=%v, busy=%t), want async load", command != nil, m.busy)
	}
	if m.cfg.ConfigPath != oldPath || m.nodeCfg != &oldCfg {
		t.Fatalf("configuration changed before loader completed: path=%q cfg=%p", m.cfg.ConfigPath, m.nodeCfg)
	}
	message, ok := command().(configReloadedMsg)
	if !ok {
		t.Fatalf("config load result = %T, want configReloadedMsg", command())
	}
	if loadedPath != newPath || message.sourcePath != newPath || message.kind != configLoad {
		t.Fatalf("config load request = (loader=%q, message=%q, kind=%d)", loadedPath, message.sourcePath, message.kind)
	}
	_, followUp := m.Update(message)
	if followUp != nil || m.busy {
		t.Fatalf("config load completion = (cmd=%v, busy=%t), want idle", followUp != nil, m.busy)
	}
	if m.cfg.ConfigPath != newPath || m.nodeCfg != &newCfg || m.cfg.NodeConfig != &newCfg {
		t.Fatalf("loaded configuration = (path=%q, nodeCfg=%p, cfg.NodeConfig=%p)", m.cfg.ConfigPath, m.nodeCfg, m.cfg.NodeConfig)
	}
	if !strings.Contains(m.notice, "Configuration loaded from "+newPath) {
		t.Fatalf("config load notice = %q", m.notice)
	}
	if summary := m.configSummary(); !strings.Contains(summary, "source: "+newPath) {
		t.Fatalf("configuration summary does not use new source:\n%s", summary)
	}
	var loggedSource bool
	for _, entry := range m.cfg.Log.Snapshot() {
		if strings.Contains(entry.Message, "source: "+newPath) {
			loggedSource = true
			break
		}
	}
	if !loggedSource {
		t.Fatalf("configuration log does not contain new source %q", newPath)
	}
}

func TestConfigLoadFailurePreservesConfigurationAndSource(t *testing.T) {
	oldCfg := config.Default()
	oldPath := `C:\Config\current.yaml`
	newPath := `C:\Config\broken.yaml`
	loadErr := errors.New("invalid YAML")
	m := &model{
		cfg: Config{
			ConfigPath: oldPath,
			NodeConfig: &oldCfg,
			Log:        NewLogHub(16),
			LoadConfig: func(path string) (*config.Config, error) {
				if path != newPath {
					t.Fatalf("LoadConfig path = %q, want %q", path, newPath)
				}
				return nil, loadErr
			},
		},
		ctx:        context.Background(),
		controller: newLifecycleController(context.Background(), nil, NewLogHub(2)),
		nodeCfg:    &oldCfg,
		phase:      phaseStopped,
	}

	_, command := m.execute(`/config load C:\Config\broken.yaml`)
	if command == nil {
		t.Fatal("config load did not return an asynchronous command")
	}
	message := command().(configReloadedMsg)
	_, _ = m.Update(message)
	if m.cfg.ConfigPath != oldPath || m.nodeCfg != &oldCfg || m.cfg.NodeConfig != &oldCfg {
		t.Fatalf("failed load changed configuration: path=%q nodeCfg=%p cfg.NodeConfig=%p", m.cfg.ConfigPath, m.nodeCfg, m.cfg.NodeConfig)
	}
	if !strings.Contains(m.notice, newPath) || !strings.Contains(m.notice, loadErr.Error()) {
		t.Fatalf("failed load notice = %q", m.notice)
	}
}

func TestConfigReloadPassesCurrentSourcePath(t *testing.T) {
	nodeCfg := config.Default()
	currentPath := `D:\index-node\node.yaml`
	var loadedPath string
	m := &model{
		cfg: Config{
			ConfigPath: currentPath,
			NodeConfig: &nodeCfg,
			Log:        NewLogHub(16),
			LoadConfig: func(path string) (*config.Config, error) {
				loadedPath = path
				return &nodeCfg, nil
			},
		},
		controller: newLifecycleController(context.Background(), nil, NewLogHub(2)),
		nodeCfg:    &nodeCfg,
		phase:      phaseStopped,
	}

	_, command := m.execute("/config reload")
	if command == nil {
		t.Fatal("config reload did not return an asynchronous command")
	}
	message := command().(configReloadedMsg)
	if loadedPath != currentPath || message.sourcePath != currentPath || message.kind != configReload {
		t.Fatalf("config reload source = (loader=%q, message=%q, kind=%d)", loadedPath, message.sourcePath, message.kind)
	}
}

func TestConfigCommandsValidateArgumentsAvailabilityAndBusyState(t *testing.T) {
	controller := newLifecycleController(context.Background(), nil, NewLogHub(2))
	m := &model{cfg: Config{Log: NewLogHub(8)}, controller: controller, phase: phaseStopped}

	for _, commandLine := range []string{"/config reload extra", "/config load", `/config load ""`, "/config unknown"} {
		_, command := m.execute(commandLine)
		if command != nil || m.busy || !strings.Contains(m.notice, "Use /config") {
			t.Fatalf("validation for %q = (cmd=%v, busy=%t, notice=%q)", commandLine, command != nil, m.busy, m.notice)
		}
	}

	_, command := m.execute("/config reload")
	if command != nil || !strings.Contains(m.notice, "not available") {
		t.Fatalf("nil loader = (cmd=%v, notice=%q)", command != nil, m.notice)
	}

	m.cfg.LoadConfig = func(string) (*config.Config, error) { return &config.Config{}, nil }
	m.busy = true
	m.busyName = "/search"
	_, command = m.execute(`/config load "C:\Config Files\next.yaml"`)
	if command != nil || !strings.Contains(m.notice, "Please wait for /search") {
		t.Fatalf("busy config load = (cmd=%v, notice=%q)", command != nil, m.notice)
	}
}

func TestSplitCommandLinePreservesQuotedWindowsPaths(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "quoted path with spaces",
			input: `/enqueue "C:\Users\Jane Doe\Pictures\a photo.jpg" D:\Raw\shot.jpg`,
			want:  []string{"/enqueue", `C:\Users\Jane Doe\Pictures\a photo.jpg`, `D:\Raw\shot.jpg`},
		},
		{
			name:  "single quotes and empty argument",
			input: `/enqueue '' 'D:\Camera Roll\frame 01.png'`,
			want:  []string{"/enqueue", "", `D:\Camera Roll\frame 01.png`},
		},
		{
			name:  "quote begins inside token",
			input: `/deadletters list --class="permanent failure" -file-ids "one,two"`,
			want:  []string{"/deadletters", "list", "--class=permanent failure", "-file-ids", "one,two"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := splitCommandLine(test.input)
			if err != nil {
				t.Fatalf("splitCommandLine: %v", err)
			}
			if strings.Join(got, "\x00") != strings.Join(test.want, "\x00") {
				t.Fatalf("arguments = %#v, want %#v", got, test.want)
			}
		})
	}

	if _, err := splitCommandLine(`/enqueue "C:\unfinished path`); err == nil || !strings.Contains(err.Error(), "unterminated") {
		t.Fatalf("unterminated quote error = %v", err)
	}
}

func TestSafeInlineStripsTerminalSequencesAndMakesControlsVisible(t *testing.T) {
	input := "\x1b[31mred\x1b[0m" +
		"\x1b]8;;https://evil.example\x1b\\CLICK\x1b]8;;\x1b\\" +
		"\n\r\t\x00\x07\u200b"
	want := `redCLICK\n\r\t\u0000\u0007\u200B`
	if got := safeInline(input); got != want {
		t.Fatalf("safeInline = %q, want %q", got, want)
	}

	formatted := FormatLogEntry(LogEntry{
		Time:    time.Date(2026, 7, 14, 1, 2, 3, 0, time.UTC),
		Scope:   "ui\x1b[2J",
		Message: "line\n\x1b]0;owned\x07tail",
	})
	assertNoRawTerminalControl(t, formatted)
	if !strings.Contains(formatted, `line\ntail`) {
		t.Fatalf("formatted log did not preserve visible text safely: %q", formatted)
	}
}

func TestBoundedTextAndConfigurationNoticeStayCompact(t *testing.T) {
	bounded := boundedText("\x1b[31m1234567890ABCDE\x1b[0m\nsecond\r\nthird\nfourth", 8, 3)
	lines := strings.Split(bounded, "\n")
	if len(lines) != 3 || lines[2] != "…" {
		t.Fatalf("bounded lines = %#v, want two content lines and an ellipsis", lines)
	}
	for _, line := range lines {
		if width := lipgloss.Width(line); width > 8 {
			t.Fatalf("bounded line %q has width %d, want <= 8", line, width)
		}
		assertNoRawTerminalControl(t, line)
	}

	nodeCfg := config.Default()
	nodeCfg.NodeID = "node\x1b[31m-red\x1b[0m"
	nodeCfg.DataDir = `C:\Index Node\data`
	for index := 0; index < 12; index++ {
		nodeCfg.Watch.Roots = append(nodeCfg.Watch.Roots, config.WatchRoot{
			Path:      `C:\Photo Library\root ` + string(rune('A'+index)),
			Recursive: index%2 == 0,
		})
	}
	hub := NewLogHub(128)
	m := &model{
		cfg: Config{
			ConfigPath: "C:\\config\x1b]0;owned\x07\\index-node.yaml",
			Log:        hub,
		},
		nodeCfg: &nodeCfg,
	}
	summaryLines := strings.Split(m.configSummary(), "\n")
	notice := m.showConfiguration()
	if got := len(hub.Snapshot()); got != len(summaryLines) {
		t.Fatalf("configuration log entries = %d, want %d", got, len(summaryLines))
	}
	if strings.Count(notice, "\n") != 1 || !strings.Contains(notice, "configuration lines were written to /log") {
		t.Fatalf("configuration notice was not compact: %q", notice)
	}
	for _, line := range strings.Split(notice, "\n") {
		assertNoRawTerminalControl(t, line)
	}
	for _, entry := range hub.Snapshot() {
		assertNoRawTerminalControl(t, entry.Message)
	}
}

func TestConfigReloadMovesThemeStateWithDataDir(t *testing.T) {
	oldCfg := config.Default()
	oldCfg.NodeID = "reload-node"
	oldCfg.DataDir = filepath.Join(t.TempDir(), "old-data")
	newCfg := oldCfg
	newCfg.DataDir = filepath.Join(t.TempDir(), "new-data")
	oldStatePath := filepath.Join(oldCfg.DataDir, "cli.json")
	newStatePath := filepath.Join(newCfg.DataDir, "cli.json")

	m := &model{
		cfg: Config{
			ConfigPath: "old.yaml",
			NodeConfig: &oldCfg,
			StatePath:  oldStatePath,
			Log:        NewLogHub(128),
		},
		nodeCfg: &oldCfg,
		mode:    ThemeDark,
	}
	_, command := m.Update(configReloadedMsg{cfg: &newCfg, sourcePath: "new.yaml", kind: configLoad})
	if command != nil {
		t.Fatalf("reload update command = %v, want nil", command)
	}
	if got := m.cfg.StatePath; filepath.Clean(got) != filepath.Clean(newStatePath) {
		t.Fatalf("state path after data_dir reload = %q, want %q", got, newStatePath)
	}
	if got := LoadTheme(newStatePath); got != ThemeDark {
		t.Fatalf("migrated theme = %q, want %q", got, ThemeDark)
	}
	if m.nodeCfg != &newCfg || m.cfg.NodeConfig != &newCfg {
		t.Fatal("reloaded configuration was not adopted")
	}
	if m.cfg.ConfigPath != "new.yaml" {
		t.Fatalf("configuration source after load = %q, want new.yaml", m.cfg.ConfigPath)
	}
}

func TestFrameCrabNoColorAndResponsivePanels(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	p := newPalette(true)
	if !p.noColor {
		t.Fatal("NO_COLOR did not disable the palette")
	}
	crab := renderFrameCrab(p, false)
	if strings.Contains(crab, "\x1b") {
		t.Fatalf("NO_COLOR mascot contains ANSI: %q", crab)
	}
	if lines := strings.Count(crab, "\n") + 1; lines != 10 {
		t.Fatalf("Frame Crab has %d lines, want 10", lines)
	}
	for _, fragment := range []string{"▄██████████████▄", "▄████████▀█████▄", "▀███▀"} {
		if !strings.Contains(crab, fragment) {
			t.Fatalf("Frame Crab is missing %q", fragment)
		}
	}

	for _, width := range []int{36, 72, 88, 118} {
		header := panelHeader(p, width, "v0.4.0")
		if got := lipgloss.Width(header); got != width {
			t.Fatalf("header width at %d columns = %d", width, got)
		}
		if !strings.Contains(ansi.Strip(header), "INDEX NODE v0.4.0") {
			t.Fatalf("header at %d columns = %q", width, header)
		}
	}

	stacked := renderStackedPanel(p, 52, "test", []string{"one"}, []string{"two"})
	assertRenderedWidth(t, stacked, 52)
	wide := renderWidePanel(p, 96, "test", []string{"left", ""}, []string{"status"}, []string{"action"})
	assertRenderedWidth(t, wide, 96)
}

func TestViewUsesRealPromptCursorWithoutSoftWrappedLines(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	nodeCfg := config.Default()

	for _, width := range []int{36, 80, 120} {
		t.Run(fmt.Sprintf("home-%d", width), func(t *testing.T) {
			m := &model{
				cfg:     Config{Version: "test", Log: NewLogHub(16)},
				nodeCfg: &nodeCfg,
				palette: newPalette(true),
				mode:    ThemeAuto,
				phase:   phaseRunning,
				width:   width,
				height:  40,
				input:   "123",
			}
			view := m.View()
			assertPromptCursor(t, view, width, "123")
			if view.AltScreen {
				t.Fatal("home unexpectedly enabled the alternate screen")
			}
			if strings.Contains(ansi.Strip(view.Content), "▌") {
				t.Fatal("home still contains the simulated block cursor")
			}

			footer := strings.Split(view.Content, "\n")[strings.Count(view.Content, "\n")]
			y := view.Cursor.Y
			x := view.Cursor.X
			m.input = ""
			empty := m.View()
			emptyFooter := strings.Split(empty.Content, "\n")[strings.Count(empty.Content, "\n")]
			if footer != emptyFooter {
				t.Fatalf("footer changed with input:\nwith input: %q\nempty: %q", footer, emptyFooter)
			}
			if empty.Cursor == nil || empty.Cursor.Y != y || empty.Cursor.X+3 != x {
				t.Fatalf("cursor movement empty->123 = (%v)->(%d,%d), want same row and +3 columns", empty.Cursor, x, y)
			}
		})
	}

	short := &model{
		cfg:     Config{Version: "test", Log: NewLogHub(16)},
		nodeCfg: &nodeCfg,
		palette: newPalette(true),
		mode:    ThemeAuto,
		phase:   phaseRunning,
		width:   80,
		height:  12,
		input:   "123",
	}
	shortView := short.View()
	shortLines := strings.Split(shortView.Content, "\n")
	visibleStart := max(0, len(shortLines)-short.height)
	visibleLines := shortLines[visibleStart:]
	if shortView.Cursor == nil || shortView.Cursor.Y >= len(visibleLines) {
		t.Fatalf("short terminal cursor = %v for %d visible lines", shortView.Cursor, len(visibleLines))
	}
	if got := ansi.Strip(visibleLines[shortView.Cursor.Y]); got != "› 123" {
		t.Fatalf("short terminal cursor row = %q, want prompt", got)
	}

	m := &model{
		cfg:        Config{Version: "test", Log: NewLogHub(16)},
		nodeCfg:    &nodeCfg,
		palette:    newPalette(true),
		mode:       ThemeAuto,
		phase:      phaseRunning,
		width:      36,
		height:     24,
		input:      strings.Repeat("界", 40),
		screen:     logScreen,
		followLogs: true,
	}
	logView := m.View()
	if !logView.AltScreen {
		t.Fatal("log screen did not enable the alternate screen")
	}
	assertPromptCursor(t, logView, m.width, m.visibleInput())
}

func assertPromptCursor(t *testing.T, view tea.View, terminalWidth int, visibleInput string) {
	t.Helper()
	if view.Cursor == nil {
		t.Fatal("view cursor is nil")
	}
	lines := strings.Split(view.Content, "\n")
	if view.Cursor.Y < 0 || view.Cursor.Y >= len(lines) {
		t.Fatalf("cursor row %d is outside %d content lines", view.Cursor.Y, len(lines))
	}
	prompt := ansi.Strip(lines[view.Cursor.Y])
	if want := "› " + visibleInput; prompt != want {
		t.Fatalf("cursor row content = %q, want %q", prompt, want)
	}
	if wantX := lipgloss.Width(prompt); view.Cursor.X != wantX {
		t.Fatalf("cursor X = %d, want display width %d", view.Cursor.X, wantX)
	}
	for lineNumber, line := range lines {
		if width := lipgloss.Width(line); width >= terminalWidth {
			t.Fatalf("line %d width = %d, must stay below terminal width %d: %q", lineNumber+1, width, terminalWidth, ansi.Strip(line))
		}
	}
}

func receiveInvocation(t *testing.T, calls <-chan lifecycleTestInvocation) lifecycleTestInvocation {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle callback")
		return lifecycleTestInvocation{}
	}
}

func receiveContext(t *testing.T, contexts <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-contexts:
		return ctx
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle context")
		return nil
	}
}

func receiveSignal(t *testing.T, signals <-chan struct{}) {
	t.Helper()
	select {
	case <-signals:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback")
	}
}

func waitRun(t *testing.T, run *lifecycleRun) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return run.wait(ctx)
}

func assertQuitCommand(t *testing.T, command tea.Cmd) {
	t.Helper()
	if command == nil {
		t.Fatal("command is nil, want tea.Quit")
	}
	if message := command(); func() bool { _, ok := message.(tea.QuitMsg); return ok }() == false {
		t.Fatalf("command result = %T, want tea.QuitMsg", message)
	}
}

func assertNoRawTerminalControl(t *testing.T, value string) {
	t.Helper()
	for _, character := range value {
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			t.Fatalf("value %q contains raw terminal control U+%04X", value, character)
		}
	}
}

func assertRenderedWidth(t *testing.T, rendered string, width int) {
	t.Helper()
	for lineNumber, line := range strings.Split(rendered, "\n") {
		if got := lipgloss.Width(line); got != width {
			t.Fatalf("line %d width = %d, want %d: %q", lineNumber+1, got, width, line)
		}
	}
}
