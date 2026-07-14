package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveThemeCanReplaceStateRepeatedly(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "nested", "state")
	path := filepath.Join(directory, "cli.json")
	modes := []ThemeMode{ThemeDark, ThemeLight, ThemeAuto, ThemeDark, ThemeLight}
	for index, mode := range modes {
		if err := SaveTheme(path, mode); err != nil {
			t.Fatalf("SaveTheme pass %d (%s): %v", index+1, mode, err)
		}
		if got := LoadTheme(path); got != mode {
			t.Fatalf("LoadTheme after pass %d = %s, want %s", index+1, got, mode)
		}
	}

	if err := SaveTheme(path, ThemeMode("neon")); err == nil {
		t.Fatal("SaveTheme accepted an invalid mode")
	}
	if got := LoadTheme(path); got != ThemeLight {
		t.Fatalf("invalid save changed persisted theme to %s", got)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".indexnode-cli-") {
			t.Fatalf("temporary state file was left behind: %s", entry.Name())
		}
	}
}
