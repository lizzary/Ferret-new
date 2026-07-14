// Package cli provides the interactive Index Node terminal UI.
package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"charm.land/lipgloss/v2"
)

// ThemeMode controls whether the terminal palette follows automatic
// background detection or a fixed light/dark appearance.
type ThemeMode string

const (
	ThemeAuto  ThemeMode = "auto"
	ThemeDark  ThemeMode = "dark"
	ThemeLight ThemeMode = "light"
)

// ParseTheme validates a user-facing theme value.
func ParseTheme(value string) (ThemeMode, error) {
	switch ThemeMode(strings.ToLower(strings.TrimSpace(value))) {
	case ThemeAuto, "":
		return ThemeAuto, nil
	case ThemeDark:
		return ThemeDark, nil
	case ThemeLight:
		return ThemeLight, nil
	default:
		return "", errors.New("theme must be auto, dark, or light")
	}
}

type persistedState struct {
	Theme ThemeMode `json:"theme"`
}

// LoadTheme returns the persisted terminal theme. Missing, unreadable, or
// invalid state safely falls back to automatic detection.
func LoadTheme(path string) ThemeMode {
	if strings.TrimSpace(path) == "" {
		return ThemeAuto
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ThemeAuto
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return ThemeAuto
	}
	mode, err := ParseTheme(string(state.Theme))
	if err != nil {
		return ThemeAuto
	}
	return mode
}

// SaveTheme atomically persists terminal-only state separately from the node
// configuration. The state file intentionally contains no runtime settings.
func SaveTheme(path string, mode ThemeMode) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("theme state path is empty")
	}
	parsed, err := ParseTheme(string(mode))
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(persistedState{Theme: parsed}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode terminal state: %w", err)
	}
	data = append(data, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create terminal state directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".indexnode-cli-*.tmp")
	if err != nil {
		return fmt.Errorf("create terminal state file: %w", err)
	}
	temporaryPath := temporary.Name()
	complete := false
	defer func() {
		_ = temporary.Close()
		if !complete {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("secure terminal state file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write terminal state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync terminal state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close terminal state: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace terminal state: %w", err)
	}
	complete = true
	return nil
}

// DetectDark resolves automatic mode using the same terminal probing behavior
// as the canonical Artifex UI.
func DetectDark(mode ThemeMode) bool {
	switch mode {
	case ThemeDark:
		return true
	case ThemeLight:
		return false
	default:
		return lipgloss.HasDarkBackground(os.Stdin, os.Stdout)
	}
}

type palette struct {
	dark      bool
	noColor   bool
	accent    string
	text      string
	secondary string
	muted     string
	success   string
	warning   string
	danger    string
	crab      string
	picture   string
	eye       string
}

func newPalette(dark bool) palette {
	p := palette{dark: dark, noColor: os.Getenv("NO_COLOR") != ""}
	if dark {
		p.accent = "#9B87F5"
		p.text = "#E4E2DF"
		p.secondary = "#B8B4AE"
		p.muted = "#6B6762"
		p.success = "#4ADE80"
		p.warning = "#FBBF24"
		p.danger = "#F87171"
		p.crab = "#F0A26A"
		p.picture = "#CFC7FF"
		p.eye = "#18181B"
	} else {
		p.accent = "#7C5CE0"
		p.text = "#2D2B28"
		p.secondary = "#555350"
		p.muted = "#958F88"
		p.success = "#22C55E"
		p.warning = "#D97706"
		p.danger = "#EF4444"
		p.crab = "#C76F3E"
		p.picture = "#BDB2F2"
		p.eye = "#18181B"
	}
	return p
}

func (p palette) style(color string) lipgloss.Style {
	style := lipgloss.NewStyle()
	if !p.noColor {
		style = style.Foreground(lipgloss.Color(color))
	}
	return style
}

func (p palette) accentStyle() lipgloss.Style    { return p.style(p.accent) }
func (p palette) textStyle() lipgloss.Style      { return p.style(p.text) }
func (p palette) secondaryStyle() lipgloss.Style { return p.style(p.secondary) }
func (p palette) mutedStyle() lipgloss.Style     { return p.style(p.muted) }
func (p palette) successStyle() lipgloss.Style   { return p.style(p.success) }
func (p palette) warningStyle() lipgloss.Style   { return p.style(p.warning) }
func (p palette) dangerStyle() lipgloss.Style    { return p.style(p.danger) }
func (p palette) crabStyle() lipgloss.Style      { return p.style(p.crab) }
func (p palette) pictureStyle() lipgloss.Style   { return p.style(p.picture) }
func (p palette) eyeStyle() lipgloss.Style       { return p.style(p.eye) }
